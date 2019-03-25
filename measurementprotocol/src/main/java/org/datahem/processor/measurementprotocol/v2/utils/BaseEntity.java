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



import org.datahem.processor.measurementprotocol.v1.utils.Parameter;
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
			new Parameter("a", "String", null, 100, "adsense_id", false, "1140262547"),
			new Parameter("cid", "String", null, 100, "client_id", false, "35009a79-1a05-49d7-b876-2b884d0f825b"),
			new Parameter("X-AppEngine-City", "String", null, 100, "city", false, "stockholm"),
			new Parameter("X-AppEngine-CityLatLong", "String", null, 100, "city_lat_long", false, "59.422571,17.833131"),
			new Parameter("X-AppEngine-Country", "String", null, 100, "country", false, "SE"),
			new Parameter("(cd[0-9]{1,3})", "String", null, 10, "custom_dimension_", false, "cd([0-9]{1,3})", "cd1", "Sports","custom_dimension_1"),
			new Parameter("(cm[0-9]{1,3})", "Integer", null, 10, "custom_metric_", false, "cm([0-9]{1,3})", "cm1",47, "custom_metric_1"),
			new Parameter("ds", "String", null, 100, "data_source", false, "web"),
			new Parameter("gtm", "String", null, 100, "gtm_container_id", false, "G7rP2BRHCI"),
			new Parameter("et", "String", null, 50, "entity_type", true, "pageview"),
			new Parameter("t", "String", null, 50, "hit_type", true, "pageview"),
			new Parameter("dh", "String", null, 100, "host", false, "foo.com"),
			new Parameter("jid", "String", null, 100, "join_id", false, "(not set)"),
			new Parameter("ni", "Boolean", null, 10, "non_interaction_hit", false, 1),
			new Parameter("dp", "String", null, 2048, "path", false, "/home"),
			new Parameter("qt", "Integer", null, 100, "queue_time", false, 560),
			new Parameter("dr", "String", null, 100, "referer", false,"http://foo.com"),
			new Parameter("drh", "String", null, 100, "referer_host", false,"foo.com"),
			new Parameter("drp", "String", null, 100, "referer_path", false,""),
			new Parameter("X-AppEngine-Region", "String", null, 100, "region", false, "ab"),
			new Parameter("dt", "String", null, 1500, "title", false,"Settings"),
			new Parameter("tid", "String", null, 100, "tracking_id", true, "UA-XXXX-Y"),
			new Parameter("ua|user-agent|User-Agent", "String", null, 1500, "user_agent", false, null, "ua","Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14", "userAgent"),
			new Parameter("dlu", "String", null, 2048, "url", false, "http://foo.com/home?a=b"),
			new Parameter("uid", "String", null, 100, "user_id", false, "as8eknlll"),
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
            if (paramMap.get("dh") != null && paramMap.get("dp") != null){
                paramMap.put("dlu",paramMap.get("dh") + paramMap.get("dp"));
            } else if(paramMap.get("dl") != null){
                URL url = new URL(paramMap.get("dl"));
				if(paramMap.get("dh")==null) paramMap.put("dh", url.getHost());
				if(paramMap.get("dp")==null) paramMap.put("dp", url.getPath());
                paramMap.put("dlu", url.getHost()+url.getFile());
            }
            
            /*
            if(paramMap.get("dl") != null){
				URL url = new URL(paramMap.get("dl"));
				if(paramMap.get("dh")==null) paramMap.put("dh", url.getHost());
				if(paramMap.get("dp")==null) paramMap.put("dp", url.getPath());
			}*/
		}catch (MalformedURLException e) {
			LOG.error(e.toString() + " document location, paramMap:" + paramMap.toString());
		}
		try{
			//If document referer parameter exist, extract host and path and add those as separate parameters
			if(paramMap.get("dr") != null){
				URL referer = new URL(paramMap.get("dr"));
				paramMap.put("drh", referer.getHost());
				paramMap.put("drp", referer.getPath());
			}
		}catch (MalformedURLException e) {
			//LOG.error(e.toString() + " referer, paramMap:" + paramMap.toString());
		}
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
        //LOG.info("baseentity build 1");
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
		//LOG.info("pageview builder 1");
        parse(paramMap);
		return builder(paramMap, MPEntity.newBuilder(), this.parameters);
	}
	
	public MPEntity.Builder builder(Map<String, String> paramMap, MPEntity.Builder mpEntityBuilder, List<Parameter> parameters) throws IllegalArgumentException {
		//LOG.info("pageview builder 2");
		parameters.sort(Comparator.comparing(p -> p.getParameterName()));
		
		mpEntityBuilder
			.setType(paramMap.get("et"))
            .setHitId(paramMap.get("MessageUuid"))
			.setClientId(paramMap.get("cid"))
			.setUserId(paramMap.getOrDefault("uid", ""))
			.setEpochMillis(Long.parseLong(paramMap.get("cpem")))
			.setDate(paramMap.get("cpd"))
			.setLocalDateTime(paramMap.get("cpts"));
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
		try{
			switch(p.getValueType()){
				case "Integer":	valBuilder.setIntValue(Long.parseLong(val));
							break;
				case "String":	valBuilder.setStringValue(val);
							break;
				case "Boolean":	valBuilder.setIntValue(Integer.parseInt(val));
							break;
                case "Double":	 valBuilder.setDoubleValue(Double.parseDouble(val));
							break;
				case "Float":	 valBuilder.setFloatValue(Float.parseFloat(val));
							break;
			}
		}catch(Exception e){
			//LOG.error(e.toString());
		}finally{
			return valBuilder.build();
		}
	}
}
