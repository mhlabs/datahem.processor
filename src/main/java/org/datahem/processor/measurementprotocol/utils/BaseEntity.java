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
import java.util.Comparator;
import java.net.URL;
import java.net.MalformedURLException;

import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseEntity{
	private List<Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(BaseEntity.class);
	
	public BaseEntity(){
		parameters = Arrays.asList(
			new Parameter("a", "String", null, 100, "adSenseId", false, "1140262547"),
			new Parameter("cid", "String", null, 100, "clientId", false, "35009a79-1a05-49d7-b876-2b884d0f825b"),
			new Parameter("X-AppEngine-City", "String", null, 100, "city", false, "stockholm"),
			new Parameter("X-AppEngine-CityLatLong", "String", null, 100, "cityLatLong", false, "59.422571,17.833131"),
			new Parameter("xid", "String", null, 40, "contentExperimentId", false, "Qp0gahJ3RAO3DJ18b0XoUQ"),
			new Parameter("xvar", "String", null, 500, "contentExperimentVariant", false,"1"),
			new Parameter("X-AppEngine-Country", "String", null, 100, "country", false, "SE"),
			new Parameter("(cd[0-9]{1,3})", "String", null, 10, "customDimension", false, "cd([0-9]{1,3})", "cd1", "Sports","customDimension1"),
			new Parameter("(cm[0-9]{1,3})", "Integer", null, 10, "customMetric", false, "cm([0-9]{1,3})", "cm1",47, "customMetric1"),
			new Parameter("ds", "String", null, 100, "dataSource", false, "web"),
			new Parameter("gtm", "String", null, 100, "gtmContainerId", false, "G7rP2BRHCI"),
			new Parameter("et", "String", null, 50, "entityType", true, "pageview"),
			new Parameter("t", "String", null, 50, "hitType", true, "pageview"),
			new Parameter("dh", "String", null, 100, "host", false, "foo.com"),
			new Parameter("jid", "String", null, 100, "joinId", false, "(not set)"),
			new Parameter("ni", "Boolean", null, 10, "nonInteractionHit", false, 1),
			new Parameter("dp", "String", null, 2048, "path", false, "/home"),
			new Parameter("qt", "Integer", null, 100, "queueTime", false, 560),
			new Parameter("dr", "String", null, 100, "referer", false,"http://example.com"),
			new Parameter("drh", "String", null, 100, "refererHost", false,"example.com"),
			new Parameter("drp", "String", null, 100, "refererPath", false,""),
			new Parameter("X-AppEngine-Region", "String", null, 100, "region", false, "ab"),
			new Parameter("dt", "String", null, 1500, "title", false,"Settings"),
			new Parameter("tid", "String", null, 100, "trackingId", true, "UA-XXXX-Y"),
			new Parameter("ua|user-agent|User-Agent", "String", null, 1500, "userAgent", false, null, "ua","Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14", "userAgent"),
			new Parameter("dl", "String", null, 2048, "url", false, "http://foo.com/home?a=b"),
			new Parameter("uid", "String", null, 100, "userId", false, "as8eknlll"),
			new Parameter("v", "String", null, 100, "version", true, "1")
		);
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
		}catch (MalformedURLException e) {
			LOG.error(e.toString());
		}
		try{
			//If document referer parameter exist, extract host and path and add those as separate parameters
			URL referer = new URL(paramMap.get("dr"));
			paramMap.put("drh", referer.getHost());
			paramMap.put("drp", referer.getPath());
		}catch (MalformedURLException e) {
			LOG.error(e.toString());
		}
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
	
	public MPEntity.Builder builder(Map<String, String> paramMap, MPEntity.Builder mpEntityBuilder, List<Parameter> parameters) throws IllegalArgumentException {
		
		parameters.sort(Comparator.comparing(p -> p.getParameterName()));
		
		mpEntityBuilder
			.setType(paramMap.get("et"))
			.setClientId(paramMap.get("cid"))
			.setUserId(paramMap.getOrDefault("uid", ""))
			.setEpochMillis(Long.parseLong(paramMap.get("cpem")))
			.setDate(paramMap.get("cpd"))
			.setUtcTimestamp(paramMap.get("cpts"));
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
	    				mpEntityBuilder.putParams((p.getParameterName()==null) ? b : p.getParameterNameWithSuffix(b), getValues(p, paramMap.get(b)));
	    			}	
	           	}
		}
		
    	
		}
		//Make a deep copy of params, then clear params from entity, then sort params copy and put back into entity
		Map<String, ValEntity> collect = mpEntityBuilder.getParamsMap().entrySet().stream().collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
		mpEntityBuilder.clearParams();
		collect
			.entrySet()
			.stream()
			.sorted(Map.Entry.<String, ValEntity>comparingByKey())
			.forEach(e -> mpEntityBuilder.putParams(e.getKey(), (ValEntity) e.getValue()));

		
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
