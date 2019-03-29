package org.datahem.processor.measurementprotocol.v2.utils;

/*-
 * ========================LICENSE_START=================================
 * Datahem.processor.measurementprotocol
 * %%
 * Copyright (C) 2018 - 2019 Robert Sahlin
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */


import java.nio.charset.StandardCharsets;

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
import java.util.Optional;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import org.datahem.processor.measurementprotocol.v2.utils.*;
import org.datahem.processor.utils.FieldMapper;
import org.datahem.protobuf.measurementprotocol.v2.*;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.Instant;
import org.joda.time.DateTimeZone;

import java.net.URL;
import java.net.MalformedURLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeasurementProtocolBuilder{
	
	private static final Logger LOG = LoggerFactory.getLogger(MeasurementProtocolBuilder.class);
	
	private Map<String, String> pm;
	
	private PageEntity pageEntity = new PageEntity();
    private EventEntity eventEntity = new EventEntity();
	private ExceptionEntity exceptionEntity = new ExceptionEntity();
    private ExperimentEntity experimentEntity = new ExperimentEntity();
    private ProductEntity productEntity = new ProductEntity();
    private TrafficSourceEntity trafficSourceEntity = new TrafficSourceEntity();
    private TransactionEntity transactionEntity = new TransactionEntity();
    private DeviceEntity deviceEntity = new DeviceEntity();

    /*
	private SocialEntity socialEntity = new SocialEntity();
	private TimingEntity timingEntity = new TimingEntity();
	
	
	
	private PromotionEntity promotionEntity = new PromotionEntity();
	private ProductImpressionEntity productImpressionEntity = new ProductImpressionEntity();
	private SiteSearchEntity siteSearchEntity = new SiteSearchEntity();
    */
    
    private static String excludedBotsPattern;
    private static String includedHostnamesPattern;
    private static String timeZone;
    
    public MeasurementProtocolBuilder(){
	}
	
	
  	public String getSearchEnginesPattern(){
    	return this.trafficSourceEntity.getSearchEnginesPattern();
  	}

	public void setSearchEnginesPattern(String pattern){
    	this.trafficSourceEntity.setSearchEnginesPattern(pattern);
  	}
  	
  	public String getSocialNetworksPattern(){
    	return this.trafficSourceEntity.getSocialNetworksPattern();
  	}

	public void setSocialNetworksPattern(String pattern){
    	this.trafficSourceEntity.setSocialNetworksPattern(pattern);
  	}
  	
  	public String getIgnoredReferersPattern(){
    	return this.trafficSourceEntity.getIgnoredReferersPattern();
  	}

	public void setIgnoredReferersPattern(String pattern){
    	this.trafficSourceEntity.setIgnoredReferersPattern(pattern);
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
  	
  /*	
  	public String getSiteSearchPattern(){
    	return this.siteSearchEntity.getSiteSearchPattern();
  	}

	public void setSiteSearchPattern(String pattern){
    	this.siteSearchEntity.setSiteSearchPattern(pattern);
  	}
*/  	
  	
  	public String getTimeZone(){
    	return this.timeZone;
  	}

	public void setTimeZone(String tz){
    	this.timeZone = tz;
  	}


    public MeasurementProtocol measurementProtocolFromPayload(PubsubMessage message){
		try{
            LOG.info("start");
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
	        LOG.info(payload);
            //Check if post body contains payload and add parameters in a map
	        if (!"".equals(payload)) {
	            //Add header parameters to pm
	            pm = FieldMapper.fieldMapFromQuery(payload);
                pm.putAll(message.getAttributeMap());
	            LOG.info(pm.toString());
	            //Exclude bots, spiders and crawlers
				if(pm.get("User-Agent") == null){
					pm.put("User-Agent", "");
				}

	        	if(!pm.get("User-Agent").matches(getExcludedBotsPattern()) && pm.get("dl").matches(getIncludedHostnamesPattern()) && !pm.get("t").equals("adtiming")){
	                //Add epochMillis and timestamp to pm       
                    Instant payloadTimeStamp = new Instant(Long.parseLong(pm.get("MessageTimestamp")));
					//DateTimeFormatter utc_timestamp = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZoneUTC();
					DateTimeFormatter local_timestamp = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZone(DateTimeZone.forID(getTimeZone()));
		            pm.put("cpts", payloadTimeStamp.toString(local_timestamp));
                    pm.put("cpem", pm.get("MessageTimestamp"));

					//Set local timezone for use as partition field
					DateTimeFormatter partition = DateTimeFormat.forPattern("YYYY-MM-dd").withZone(DateTimeZone.forID(getTimeZone()));
					pm.put("cpd", payloadTimeStamp.toString(partition));

                    try{
                        //If document location parameter exist, extract host and path and add those as separate parameters
                        if (pm.get("dh") != null && pm.get("dp") != null){
                            pm.put("dlu",pm.get("dh") + pm.get("dp"));
                        } else if(pm.get("dl") != null){
                            URL url = new URL(pm.get("dl"));
                            if(pm.get("dh")==null) pm.put("dh", url.getHost());
                            if(pm.get("dp")==null) pm.put("dp", url.getPath());
                            pm.put("dlu", url.getHost()+url.getFile());
                        }
                    }catch (MalformedURLException e) {
                        LOG.error(e.toString() + " document location, pm:" + pm.toString());
                    }
                    
                    try{
                        //If document referer parameter exist, extract host and path and add those as separate parameters
                        if(pm.get("dr") != null){
                            URL referer = new URL(pm.get("dr"));
                            pm.put("drh", referer.getHost());
                            pm.put("drp", referer.getPath());
                        }
                    }catch (MalformedURLException e) {
                        LOG.error(e.toString() + " referer, pm:" + pm.toString());
                    }
					
                    MeasurementProtocol.Builder builder = MeasurementProtocol.newBuilder();
                    Optional.ofNullable(pm.get("t")).ifPresent(builder::setHitType);
                    Optional.ofNullable(pm.get("cid")).ifPresent(builder::setClientId);
                    Optional.ofNullable(pm.get("uid")).ifPresent(builder::setUserId);
                    Optional.ofNullable(pm.get("MessageUuid")).ifPresent(builder::setHitId);
                    Optional.ofNullable(pm.get("v")).ifPresent(builder::setVersion);
                    FieldMapper.intVal(pm.get("ni")).ifPresent(g -> builder.setNonInteraction(g.intValue()));

                    Optional.ofNullable(pageEntity.build(pm)).ifPresent(builder::setPage);
                    Optional.ofNullable(eventEntity.build(pm)).ifPresent(builder::setEvent);
                    Optional.ofNullable(exceptionEntity.build(pm)).ifPresent(builder::setException);
                    Optional.ofNullable(experimentEntity.build(pm)).ifPresent(builder::addAllExperiment);
                    Optional.ofNullable(productEntity.build(pm)).ifPresent(builder::addAllProducts);
                    Optional.ofNullable(trafficSourceEntity.build(pm)).ifPresent(builder::setTrafficSource);
                    Optional.ofNullable(transactionEntity.build(pm)).ifPresent(builder::setTransaction);
                    Optional.ofNullable(deviceEntity.build(pm)).ifPresent(builder::setDevice);
                    
                    MeasurementProtocol measurementProtocol = builder.build();
                    LOG.info(measurementProtocol.toString());
                    return measurementProtocol; 
                    /*
					addAllIfNotNull(events, pageviewEntity.build(pm));
                    addAllIfNotNull(events, eventEntity.build(pm));
                    addAllIfNotNull(events, exceptionEntity.build(pm));
					addAllIfNotNull(events, siteSearchEntity.build(pm));
					addAllIfNotNull(events, socialEntity.build(pm));
					addAllIfNotNull(events, timingEntity.build(pm));
					addAllIfNotNull(events, transactionEntity.build(pm));
					addAllIfNotNull(events, productEntity.build(pm));
					addAllIfNotNull(events, trafficEntity.build(pm));
					addAllIfNotNull(events, promotionEntity.build(pm));
					addAllIfNotNull(events, productImpressionEntity.build(pm));
					addAllIfNotNull(events, experimentEntity.build(pm));
                    */
				}
	        }else{
                LOG.info("not matching MeasurementProtocolBuilder conditions: User-Agent: " + pm.getOrDefault("User-Agent", "null") + ", document.location: " + pm.getOrDefault("dl", "null") + ", type:" + pm.getOrDefault("t", "null"));
            }
        }
        catch (NullPointerException e) {
				LOG.error(e.toString());// + " pm:" + pm.toString());
		}
    	return null;   
    }
        
}
