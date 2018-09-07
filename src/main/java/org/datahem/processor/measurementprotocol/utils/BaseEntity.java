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

import org.datahem.processor.measurementprotocol.utils.Parameter;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;
import java.net.URL;
import java.net.MalformedURLException;

import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseEntity{
	//private Map<String, Parameter> parameters;
	private List<Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(BaseEntity.class);
	
	/*
	public BaseEntity(){
		parameters = new HashMap<String, Parameter>();
		parameters.put("HIT_TYPE", new Parameter("ht", "String", null, 50, "hitType", true));
		parameters.put("URL", new Parameter("dl", "String", null, 2048, "url", false));
		parameters.put("HOST", new Parameter("dh", "String", null, 100, "host", false));
		parameters.put("PATH", new Parameter("dp", "String", null, 2048, "path", false));
		parameters.put("TITLE", new Parameter("dt", "String", null, 1500, "title", false));
		parameters.put("LINK_ID", new Parameter("linkid", "String", null, 2048, "linkId", false));
		parameters.put("VERSION", new Parameter("v", "String", null, 100, "version", true));
		parameters.put("TRACKING_ID", new Parameter("tid", "String", null, 100, "trackingId", true));
		parameters.put("DATA_SOURCE", new Parameter("ds", "String", null, 100, "dataSource", false));
		parameters.put("QUEUE_TIME", new Parameter("qt", "String", null, 100, "queueTime", false));
		parameters.put("CLIENT_ID", new Parameter("cid", "String", null, 100, "clientId", false));
		parameters.put("USER_ID", new Parameter("uid", "String", null, 100, "userId", false));
		parameters.put("USER_AGENT", new Parameter("ua|user-agent|User-Agent", "String", null, 1500, "userAgent", false));
		parameters.put("NON_INTERACTION_HIT", new Parameter("ni", "Boolean", null, 10, "nonInteractionHit", false));
		parameters.put("CUSTOM_DIMENSION", new Parameter("(cd[0-9]{1,3})", "String", null, 10, "customDimension", false, "cd([0-9]{1,3})"));
		parameters.put("CUSTOM_METRIC", new Parameter("(cm[0-9]{1,3})", "Integer", null, 10, "customMetric", false, "cm([0-9]{1,3})"));
		parameters.put("CONTENT_EXPERIMENT_ID", new Parameter("xid", "String", null, 40, "contentExperimentId", false));
		parameters.put("CONTENT_EXPERIMENT_VARIANT", new Parameter("xvar", "String", null, 500, "contentExperimentVariant", false));
		parameters.put("COUNTRY", new Parameter("X-AppEngine-Country", "String", null, 100, "country", false));
		parameters.put("REGION", new Parameter("X-AppEngine-Region", "String", null, 100, "region", false));
		parameters.put("CITY", new Parameter("X-AppEngine-City", "String", null, 100, "city", false));
		parameters.put("CITY_LAT_LONG", new Parameter("X-AppEngine-CityLatLong", "String", null, 100, "cityLatLong", false));
		parameters.put("REFERER", new Parameter("dr", "String", null, 100, "referer", false));
		parameters.put("REFERER_HOST", new Parameter("drh", "String", null, 100, "refererHost", false));
		parameters.put("REFERER_PATH", new Parameter("drp", "String", null, 100, "refererPath", false));
		parameters.put("JOIN_ID", new Parameter("jid", "String", null, 100, "joinId", false));
		parameters.put("ADSENSE_ID", new Parameter("a", "String", null, 100, "adSenseId", false));
		parameters.put("GTM_CONTAINER_ID", new Parameter("gtm", "String", null, 100, "gtmContainerId", false));
	}*/
	
	public BaseEntity(){
		parameters = new ArrayList<>(Arrays.asList(
			new Parameter("a", "String", null, 100, "adSenseId", false, "1140262547"),
			new Parameter("cid", "String", null, 100, "clientId", false, "1062063169.1517835391"),
			new Parameter("X-AppEngine-City", "String", null, 100, "city", false, "stockholm"),
			new Parameter("X-AppEngine-CityLatLong", "String", null, 100, "cityLatLong", false, "59.422571,17.833131"),
			new Parameter("xid", "String", null, 40, "contentExperimentId", false),
			new Parameter("xvar", "String", null, 500, "contentExperimentVariant", false),
			new Parameter("X-AppEngine-Country", "String", null, 100, "country", false, "SE"),
			new Parameter("(cd[0-9]{1,3})", "String", null, 10, "customDimension", false, "cd([0-9]{1,3})", "gold"),
			new Parameter("(cm[0-9]{1,3})", "Integer", null, 10, "customMetric", false, "cm([0-9]{1,3})", 25),
			new Parameter("ds", "String", null, 100, "dataSource", false),
			new Parameter("gtm", "String", null, 100, "gtmContainerId", false, "G7rP2BRHCI"),
			new Parameter("ht", "String", null, 50, "hitType", true, "pageview"),
			new Parameter("dh", "String", null, 100, "host", false, "www.datahem.org"),
			new Parameter("jid", "String", null, 100, "joinId", false, "(not set)"),
			new Parameter("linkid", "String", null, 2048, "linkId", false),
			new Parameter("ni", "Boolean", null, 10, "nonInteractionHit", false),
			new Parameter("dp", "String", null, 2048, "path", false, "/varor/kott-o-chark"),
			new Parameter("qt", "String", null, 100, "queueTime", false),
			new Parameter("dr", "String", null, 100, "referer", false),
			new Parameter("drh", "String", null, 100, "refererHost", false),
			new Parameter("drp", "String", null, 100, "refererPath", false),
			new Parameter("X-AppEngine-Region", "String", null, 100, "region", false, "ab"),
			new Parameter("dt", "String", null, 1500, "title", false,"Fruit & Vegetables"),
			new Parameter("tid", "String", null, 100, "trackingId", true, "UA-1234567-89"),
			new Parameter("ua|user-agent|User-Agent", "String", null, 1500, "userAgent", false, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36"),
			new Parameter("dl", "String", null, 2048, "url", false, "https://www.datahem.org/varor/fruit"),
			new Parameter("uid", "String", null, 100, "userId", false, "947563"),
			new Parameter("v", "String", null, 100, "version", true, "1")
		));
	}
	
	public List<Parameter> getParameters(){return parameters;}
	
	private boolean trigger(Map<String, String> paramMap){
		return true;
	}
	
	private void parse(Map<String, String> paramMap){
		try{
			//If document location parameter exist, extract host and path and add those as separate parameters
			URL url = new URL(paramMap.get("dl"));
			if(paramMap.get("dh")==null) paramMap.put("dh", url.getHost());
			if(paramMap.get("dp")==null) paramMap.put("dp", url.getPath());
		}catch (MalformedURLException e) {}
		try{
			//If document referer parameter exist, extract host and path and add those as separate parameters
			URL referer = new URL(paramMap.get("dr"));
			paramMap.put("drh", referer.getHost());
			paramMap.put("drp", referer.getPath());
		}catch (MalformedURLException e) {}
	}
			
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> mpEntities = new ArrayList<>();
		if(trigger(paramMap)){
    		try{
				mpEntities.add(builder(paramMap).build());
				return mpEntities;
			}
			catch(IllegalArgumentException e){
				LOG.error(e.toString());
				return null;
			}
		}
		else{
			return null;
		}
	}
	
	public MPEntity.Builder builder(Map<String, String> paramMap) throws IllegalArgumentException{
		parse(paramMap);
		return builder(paramMap, MPEntity.newBuilder(), this.parameters);
	}
	
	//public MPEntity.Builder builder(Map<String, String> paramMap, MPEntity.Builder mpEntityBuilder, Map<String, Parameter> parameters) throws IllegalArgumentException {
	public MPEntity.Builder builder(Map<String, String> paramMap, MPEntity.Builder mpEntityBuilder, List<Parameter> parameters) throws IllegalArgumentException {
		
		mpEntityBuilder
			//.setVersion(paramMap.get("v"))
			.setType(paramMap.get("ht"))
			.setClientId(paramMap.get("cid"))
			.setUserId(paramMap.getOrDefault("uid", ""))
			.setEpochMillis(Long.parseLong(paramMap.get("cpem")))
			.setDate(paramMap.get("cpd"))
			.setUtcTimestamp(paramMap.get("cpts"));
		//for (Parameter p : parameters.values()){
		for (Parameter p : parameters) {
			Pattern pattern = Pattern.compile("^" + p.getParameter() + "$");
 			List<String> bu = paramMap
 				.keySet()
 				.stream()
 				.filter(pattern.asPredicate())
 				.collect(Collectors.toList());
 			if (bu.size() == 0){
 				if(p.getRequired()){
 					throw new IllegalArgumentException("Required parameter (" + p.getParameter() + ") not set.");
 				}
	    		else{
	    			if(p.getDefaultValue() != null){
	    				mpEntityBuilder.putParams(p.getParameterName(), getValues(p, p.getDefaultValue()));
					}
	    		}
 			}
 			else{
	           	for(String b : bu){
	           		if(paramMap.get(b) == null){
	    				if(p.getRequired()){
	    					throw new IllegalArgumentException("Required parameter (" + p.getParameter() + ") not set.");
	    				}
	    				else{
	    					if(p.getDefaultValue() != null){
	    						mpEntityBuilder.putParams(p.getParameterName(), getValues(p, p.getDefaultValue()));
							}
	    				}
	    			}
	    			else{
	    				//mpEntityBuilder.putParams((p.getParameterName()==null) ? b : p.getParameterName(), getValues(p, paramMap.get(b)));
	    				mpEntityBuilder.putParams((p.getParameterName()==null) ? b : p.getParameterNameWithSuffix(b), getValues(p, paramMap.get(b)));
	    			}	
	           	}
    	}

		}
		return mpEntityBuilder;
	}
	
	//Store parameters as the type specified for each parameter
	private ValEntity getValues(Parameter p, String val){
		ValEntity.Builder valBuilder = ValEntity.newBuilder();
		switch(p.getValueType()){
			case "Integer":	valBuilder.setIntValue(Long.parseLong(val));
						break;
			case "String":	valBuilder.setStringValue(val);
						break;
			case "Boolean":	valBuilder.setIntValue(Integer.parseInt(val));
						break;
			case "Double":	 valBuilder.setFloatValue(Double.parseDouble(val));
						break;
		}
		return valBuilder.build();
	}
}
