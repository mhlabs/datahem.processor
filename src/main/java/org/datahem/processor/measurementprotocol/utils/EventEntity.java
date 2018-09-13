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

import org.datahem.processor.measurementprotocol.utils.BaseEntity;
import org.datahem.processor.measurementprotocol.utils.Parameter;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventEntity extends BaseEntity{
	/*
	private Map<String, Parameter> parameters;
	
	private static final Logger LOG = LoggerFactory.getLogger(EventEntity.class);
	
	public EventEntity(){
		super();
		parameters = new HashMap<String, Parameter>();
		parameters.put("EVENT_CATEGORY", new Parameter("ec", "String", null, 150, "eventCategory", true));
		parameters.put("EVENT_ACTION", new Parameter("ea", "String", null, 500, "eventAction", true));
		parameters.put("EVENT_LABEL", new Parameter("el", "String", null, 500, "eventLabel", false));
		parameters.put("EVENT_VALUE", new Parameter("ev", "Integer", null, 100, "eventValue", false));
	}*/
	
	private List<Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(EventEntity.class);
	
	public EventEntity(){
		super();
		parameters = new ArrayList<>(Arrays.asList(
			new Parameter("ec", "String", null, 150, "eventCategory", true, "Category"),
			new Parameter("ea", "String", null, 500, "eventAction", true, "Action"),
			new Parameter("el", "String", null, 500, "eventLabel", false, "Label"),
			new Parameter("ev", "Integer", null, 100, "eventValue", false, 55)
		));
	}
	
	public List<Parameter> getParameters(){return parameters;}
	
	private boolean trigger(Map<String, String> paramMap){
		return "event".equals(paramMap.get("t"));
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> mpEntities = new ArrayList<>();
		if(trigger(paramMap)){
			paramMap.put("ht", "event");
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
		return builder(paramMap, super.builder(paramMap));
	}
	
	public MPEntity.Builder builder(Map<String, String> paramMap, MPEntity.Builder mpEntityBuilder) throws IllegalArgumentException{
		return super.builder(paramMap, mpEntityBuilder, this.parameters);
	}	
}
