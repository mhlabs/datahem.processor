package org.datahem.processor.measurementprotocol.utils;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 Robert Sahlin and MatHem Sverige AB
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * =========================LICENSE_END==================================
 */

import org.datahem.processor.utils.FieldMapper;
import org.datahem.processor.measurementprotocol.utils.PageviewEntity;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.datahem.protobuf.collector.v1.CollectorPayloadEntityProto.CollectorPayloadEntity;
import com.google.protobuf.TextFormat;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.net.URL;
import java.net.MalformedURLException;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.Instant;
import org.joda.time.DateTimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeasurementProtocolBuilder{
	
	private static final Logger LOG = LoggerFactory.getLogger(MeasurementProtocolBuilder.class);
	
	private Map<String, String> paramMap;
	private List<MPEntity> events = new ArrayList<>();
	private PageviewEntity pageviewEntity = new PageviewEntity();
	private EventEntity eventEntity = new EventEntity();
	private ExceptionEntity exceptionEntity = new ExceptionEntity();
	private SocialEntity socialEntity = new SocialEntity();
	private TimingEntity timingEntity = new TimingEntity();
	private TransactionEntity transactionEntity = new TransactionEntity();
	private ProductEntity productEntity = new ProductEntity();
	private TrafficEntity trafficEntity = new TrafficEntity();
	private PromotionEntity promotionEntity = new PromotionEntity();
	private ExperimentEntity experimentEntity = new ExperimentEntity();
	
	private ProductImpressionEntity productImpressionEntity = new ProductImpressionEntity();
	
	private SiteSearchEntity siteSearchEntity = new SiteSearchEntity();
    private static String excludedBotsPattern;
    private static String includedHostnamesPattern;
    private static String timeZone;
    
    public MeasurementProtocolBuilder(){
	}
	
	
  	public String getSearchEnginesPattern(){
    	return this.trafficEntity.getSearchEnginesPattern();
  	}

	public void setSearchEnginesPattern(String pattern){
    	this.trafficEntity.setSearchEnginesPattern(pattern);
  	}
  	
  	public String getSocialNetworksPattern(){
    	return this.trafficEntity.getSocialNetworksPattern();
  	}

	public void setSocialNetworksPattern(String pattern){
    	this.trafficEntity.setSocialNetworksPattern(pattern);
  	}
  	
  	public String getIgnoredReferersPattern(){
    	return this.trafficEntity.getIgnoredReferersPattern();
  	}

	public void setIgnoredReferersPattern(String pattern){
    	this.trafficEntity.setIgnoredReferersPattern(pattern);
  	}
  	
  	
  	public String getExcludedBotsPattern(){
    	return this.excludedBotsPattern;
  	}

	public void setExcludedBotsPattern(String pattern){
    	this.excludedBotsPattern = pattern;
  	}
  	
  	public String getIncludedHostnamesPattern(){
    	return this.includedHostnamesPattern;
  	}

	public void setIncludedHostnamesPattern(String pattern){
    	this.includedHostnamesPattern = pattern;
  	}
  	
  	
  	public String getSiteSearchPattern(){
    	return this.siteSearchEntity.getSiteSearchPattern();
  	}

	public void setSiteSearchPattern(String pattern){
    	this.siteSearchEntity.setSiteSearchPattern(pattern);
  	}
  	
  	
  	public String getTimeZone(){
    	return this.timeZone;
  	}

	public void setTimeZone(String tz){
    	this.timeZone = tz;
  	}

	//public List<MPEntity> mpEntitiesFromCollectorPayload(CollectorPayloadEntity cp){
	public List<MPEntity> mpEntitiesFromPayload(String payload){
		try{
	        //Check if post body contains payload and add parameters in a map
	        //if (!"".equals(cp.getPayload())) {
	        if (!"".equals(payload)) {
	            //Add header parameters to paramMap
	            //paramMap = FieldMapper.fieldMapFromQuery(cp.getPayload());
	            paramMap = FieldMapper.fieldMapFromQuery(payload);
	            //paramMap.putAll(cp.getHeadersMap());
	            
	            //Exclude bots, spiders and crawlers
				if(paramMap.get("User-Agent") == null){
					paramMap.put("User-Agent", "");
					LOG.info("User-Agent = null");
				}

	        	if(!paramMap.get("User-Agent").matches(getExcludedBotsPattern()) && paramMap.get("dl").matches(getIncludedHostnamesPattern())){
	                //Add epochMillis and timestamp to paramMap       
		            
		            //Instant payloadTimeStamp = new Instant(Long.parseLong(cp.getEpochMillis()));
		            Instant payloadTimeStamp = new Instant(Long.parseLong(paramMap.get("timestamp")));
					DateTimeFormatter utc_timestamp = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZoneUTC();
		            paramMap.put("cpts", payloadTimeStamp.toString(utc_timestamp));
		            //paramMap.put("cpem", cp.getEpochMillis());
		            paramMap.put("cpem", paramMap.get("timestamp"));

					//Set local timezone for use as partition field
					DateTimeFormatter partition = DateTimeFormat.forPattern("YYYY-MM-dd").withZone(DateTimeZone.forID(getTimeZone()));
					paramMap.put("cpd", payloadTimeStamp.toString(partition));
					
					addAllIfNotNull(events, pageviewEntity.build(paramMap));
					addAllIfNotNull(events, eventEntity.build(paramMap));
					addAllIfNotNull(events, exceptionEntity.build(paramMap));
					addAllIfNotNull(events, siteSearchEntity.build(paramMap));
					addAllIfNotNull(events, socialEntity.build(paramMap));
					addAllIfNotNull(events, timingEntity.build(paramMap));
					addAllIfNotNull(events, transactionEntity.build(paramMap));
					addAllIfNotNull(events, productEntity.build(paramMap));
					addAllIfNotNull(events, trafficEntity.build(paramMap));
					addAllIfNotNull(events, promotionEntity.build(paramMap));
					addAllIfNotNull(events, productImpressionEntity.build(paramMap));
					addAllIfNotNull(events, experimentEntity.build(paramMap));
				}
	        }
        }
        catch (Exception e) {
				LOG.error(e.toString());
		}
    	return events;   
    }
        
    public static void addAllIfNotNull(List<MPEntity> list, List<MPEntity> c) {
    	if (c != null) {
        	list.addAll(c);
    	}
	}
	
	public static boolean regexMatcherFind(String expression,String input) {
    	if(!"".equals(input)) return false;
    	Pattern p = Pattern.compile(expression);
        Matcher m = p.matcher(input);
        return m.find();
	}

}
