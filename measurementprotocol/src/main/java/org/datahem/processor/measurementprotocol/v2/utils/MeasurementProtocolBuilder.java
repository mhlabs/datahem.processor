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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.net.URI;
import java.net.URISyntaxException;
//import java.net.URL;
//import java.net.MalformedURLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeasurementProtocolBuilder{
	
	private static final Logger LOG = LoggerFactory.getLogger(MeasurementProtocolBuilder.class);
	
	private HashMap<String, String> pm;
	
	private PageEntity pageEntity = new PageEntity();
    private EventEntity eventEntity = new EventEntity();
	private ExceptionEntity exceptionEntity = new ExceptionEntity();
    private ExperimentEntity experimentEntity = new ExperimentEntity();
    private ProductEntity productEntity = new ProductEntity();
    private GeoEntity geoEntity = new GeoEntity();
    private TrafficSourceEntity trafficSourceEntity = new TrafficSourceEntity();
    private TransactionEntity transactionEntity = new TransactionEntity();
    private DeviceEntity deviceEntity = new DeviceEntity();
    private PromotionEntity promotionEntity = new PromotionEntity();
    private SocialEntity socialEntity = new SocialEntity();
    private LatencyEntity latencyEntity = new LatencyEntity();
    private PropertyEntity propertyEntity = new PropertyEntity();
    private AttributesEntity attributesEntity = new AttributesEntity();
    private TimeEntity timeEntity = new TimeEntity();

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
  	
  	
  	public String getSiteSearchPattern(){
    	return this.pageEntity.getSiteSearchPattern();
  	}

	public void setSiteSearchPattern(String pattern){
    	this.pageEntity.setSiteSearchPattern(pattern);
  	}  	
  	
  	public String getTimeZone(){
    	return this.timeZone;
  	}

	public void setTimeZone(String tz){
    	this.timeZone = tz;
        this.timeEntity.setTimeZone(tz);
  	}

    public MeasurementProtocol measurementProtocolFromPayload(PubsubMessage message){
		try{
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
	        //LOG.info("payload: " + payload);
            //Check if post body contains payload and add parameters in a map
	        if (!"".equals(payload)) {
	            //Add header parameters to pm
	            pm = FieldMapper.fieldMapFromQuery(payload);
                pm.putAll(message.getAttributeMap());
	            //Exclude bots, spiders and crawlers
				if(pm.get("User-Agent") == null){
					pm.put("User-Agent", "");
				}
                //LOG.info(pm.toString());
	        	if(!pm.get("User-Agent").matches(getExcludedBotsPattern()) && (pm.getOrDefault("dl","").matches(getIncludedHostnamesPattern()) || pm.getOrDefault("dh","").matches(getIncludedHostnamesPattern())) && !pm.get("t").equals("adtiming")){
                    //LOG.info("checkpoint");
                    try{
                        //If document location parameter exist, extract host and path and add those as separate parameters 
                        URI uri = new URI(pm.get("dl").replace(" ", "%20"));
                        pm.put("dlh", uri.getHost());
                        pm.put("dlp", (uri.getRawQuery() != null ? uri.getRawPath() + "?" + uri.getRawQuery() : uri.getRawPath()));
                    }
                    catch (URISyntaxException e) {
                        //LOG.error("pm: "+ pm.toString());
                        //LOG.error(pm.get("dl"));
                        LOG.error("URISyntaxException (IE11 user agent?): ", e);
                    }
                    catch (NullPointerException e) {
                        LOG.error("NullPointerException: ", e);
                        //LOG.error(e.toString());
		            }
                    
                    if(pm.get("dh") != null) pm.put("dlh", (pm.get("dh")));
                    if(pm.get("dp") != null) pm.put("dlp", (pm.get("dp").replace(" ", "%20")));
                    if(pm.get("dlh") != null && pm.get("dlp") != null) pm.put("dlu", pm.get("dlh") + pm.get("dlp"));

                        /*
                        if(pm.get("dh")!=null) pm.put("dlh", uri.getHost());
                        if (pm.get("dh") != null && pm.get("dp") != null){
                            //pm.put("dlu","https://" + pm.get("dh") + pm.get("dp"));
                            pm.put("dlu", pm.get("dh") + pm.get("dp"));
                        } else if(pm.get("dl") != null){
                            //URL url = new URL(pm.get("dl"));
                            //if(pm.get("dh")==null) pm.put("dh", url.getHost());
                            //if(pm.get("dp")==null) pm.put("dp", url.getFile());
                            URI uri = new URI(pm.get("dl"));
                            if(pm.get("dh")==null) pm.put("dh", uri.getHost());
                            if(pm.get("dp")==null){
                                String path = (uri.getQuery() != null ? uri.getPath() + "?" + uri.getQuery() : uri.getPath());
                                pm.put("dp", path);
                            } 
                            pm.put("dlu", pm.get("dh") + pm.get("dp"));
                        }
                        
                    }//catch (MalformedURLException e) {
                    catch (URISyntaxException e) {
                        LOG.error("URISyntaxException: ", e);
                    }*/
                    
                    try{
                        //If document referer parameter exist, extract host and path and add those as separate parameters
                        if(pm.get("dr") != null && !pm.get("dr").equals("(not set)")){
                            URI referer = new URI(pm.get("dr"));
                            pm.put("drh", referer.getHost());
                            pm.put("drp", referer.getRawPath());
                        }
                    }//catch (MalformedURLException e) {
                    catch (URISyntaxException e) {
                        LOG.error("MalformedURLException: ", e);
                    }
                    catch (NullPointerException e) {
                        LOG.error("NullPointerException: ", e);
		            }
                    MeasurementProtocol.Builder builder = MeasurementProtocol.newBuilder();
                    Optional.ofNullable(pm.get("t")).ifPresent(builder::setHitType);
                    Optional.ofNullable(pm.get("cid")).ifPresent(builder::setClientId);
                    Optional.ofNullable(pm.get("uid")).ifPresent(builder::setUserId);
                    Optional.ofNullable(pm.get("v")).ifPresent(builder::setVersion);
                    FieldMapper.intVal(pm.get("ni")).ifPresent(g -> builder.setNonInteraction(g.intValue()));
                    //Set local timezone for use as partition field
					DateTimeFormatter partition = DateTimeFormat.forPattern("YYYY-MM-dd").withZone(DateTimeZone.forID(getTimeZone()));
                    Optional.ofNullable(DateTime.parse(pm.get("timestamp")).toString(partition)).ifPresent(builder::setDate);
                    Optional.ofNullable(pageEntity.build((HashMap)pm.clone())).ifPresent(builder::setPage);
                    Optional.ofNullable(eventEntity.build((HashMap)pm.clone())).ifPresent(builder::setEvent);
                    Optional.ofNullable(exceptionEntity.build((HashMap)pm.clone())).ifPresent(builder::setException);
                    Optional.ofNullable(geoEntity.build((HashMap)pm.clone())).ifPresent(builder::setGeo);
                    Optional.ofNullable(experimentEntity.build((HashMap)pm.clone())).ifPresent(builder::addAllExperiments);
                    Optional.ofNullable(trafficSourceEntity.build((HashMap)pm.clone())).ifPresent(builder::setTrafficSource);
                    Optional.ofNullable(transactionEntity.build((HashMap)pm.clone())).ifPresent(builder::setTransaction);
                    Optional.ofNullable(deviceEntity.build((HashMap)pm.clone())).ifPresent(builder::setDevice);
                    Optional.ofNullable(promotionEntity.build((HashMap)pm.clone())).ifPresent(builder::addAllPromotions);
                    Optional.ofNullable(productEntity.build((HashMap)pm.clone())).ifPresent(builder::addAllProducts);
                    Optional.ofNullable(socialEntity.build((HashMap)pm.clone())).ifPresent(builder::setSocial);
                    Optional.ofNullable(latencyEntity.build((HashMap)pm.clone())).ifPresent(builder::setLatency);
                    Optional.ofNullable(propertyEntity.build((HashMap)pm.clone())).ifPresent(builder::setProperty);
                    Optional.ofNullable(FieldMapper.getCustomDimensions((HashMap)pm.clone(), "^(cd[0-9]{1,3})$", "^cd([0-9]{1,3})$")).ifPresent(builder::addAllCustomDimensions);
                    Optional.ofNullable(FieldMapper.getCustomMetrics((HashMap)pm.clone(),"^(cm[0-9]{1,3})$","^cm([0-9]{1,3})$")).ifPresent(builder::addAllCustomMetrics);
                    Optional.ofNullable(attributesEntity.build((HashMap)pm.clone())).ifPresent(builder::setATTRIBUTES);
                    Optional.ofNullable(timeEntity.build((HashMap)pm.clone())).ifPresent(builder::setTime);

                    MeasurementProtocol measurementProtocol = builder.build();
                    //LOG.info(measurementProtocol.toString());
                    return measurementProtocol; 
                }else{
                    //LOG.info("not matching MeasurementProtocolBuilder conditions: User-Agent: " + pm.getOrDefault("User-Agent", "null") + ", document.location: " + pm.getOrDefault("dl", "null") + ", type:" + pm.getOrDefault("t", "null"));
                    }
            }else{LOG.info("no message payload");}
        }
        catch (NullPointerException e) {
                LOG.error("NullPointerException: ", e);
                //LOG.error(e.toString());
		}
    	return null;   
    }       
}