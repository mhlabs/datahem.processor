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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;

import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageviewEntity extends BaseEntity{
	//private Map<String, Parameter> parameters;
	private List<Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(PageviewEntity.class);
	
	/*
	public PageviewEntity(){
		super();
		parameters = new HashMap<String, Parameter>();
		parameters.put("SCREEN_RESOLUTION", new Parameter("sr", "String", null, 20, "screenResolution", false));
		parameters.put("VIEWPORT_SIZE", new Parameter("vp", "String", null, 20, "viewportSize", false));
		parameters.put("DOCUMENT_ENCODING", new Parameter("de", "String", "UTF-8", 20, "encoding", false));
		parameters.put("SCREEN_COLORS", new Parameter("sd", "String", null, 20, "screenColors", false));
		parameters.put("USER_LANGUAGE", new Parameter("ul", "String", null, 20, "language", false));
		parameters.put("JAVA_ENABLED", new Parameter("je", "Integer", null, 20, "javaEnabled", false));
		parameters.put("FLASH_VERSION", new Parameter("fl", "String", null, 20, "flashVersion", false));
	}*/
	
		public PageviewEntity(){
		super();
		parameters = new ArrayList<>(Arrays.asList(
			new Parameter("sr", "String", null, 20, "screenResolution", false),
			new Parameter("vp", "String", null, 20, "viewportSize", false),
			new Parameter("de", "String", "UTF-8", 20, "encoding", false),
			new Parameter("sd", "String", null, 20, "screenColors", false),
			new Parameter("ul", "String", null, 20, "language", false),
			new Parameter("je", "Integer", null, 20, "javaEnabled", false),
			new Parameter("fl", "String", null, 20, "flashVersion", false)
		));
	}
	
	private boolean trigger(Map<String, String> paramMap){
		return "pageview".equals(paramMap.get("t"));
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
			paramMap.put("ht", "pageview");
    		try{
				eventList.add(builder(paramMap).build());
				return eventList;
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
	
	public MPEntity.Builder builder(Map<String, String> paramMap, MPEntity.Builder eventBuilder) throws IllegalArgumentException{
		return super.builder(paramMap, eventBuilder, this.parameters);
	}
}
