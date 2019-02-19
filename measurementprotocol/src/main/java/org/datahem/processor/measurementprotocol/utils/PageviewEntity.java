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
	private List<Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(PageviewEntity.class);
	
	public PageviewEntity(){
		super();
		parameters = new ArrayList<>(Arrays.asList(
			new Parameter("de", "String", "UTF-8", 20, "encoding", false, "UTF-8"),
			new Parameter("fl", "String", null, 20, "flash_version", false, "10 1 r103"),
			new Parameter("sr", "String", null, 20, "screen_resolution", false, "800x600"),
			new Parameter("vp", "String", null, 20, "viewport_size", false, "123x456"),
			new Parameter("sd", "String", null, 20, "screen_colors", false, "24-bits"),
			new Parameter("ul", "String", null, 20, "language", false, "en-us"),
			new Parameter("je", "Integer", null, 20, "java_enabled", false, 1),
			new Parameter("linkid", "String", null, 2048, "link_id", false,"nav_bar")
		));
	}
	
	public List<Parameter> getParameters(){return parameters;}
	
	private boolean trigger(Map<String, String> paramMap){
		return "pageview".equals(paramMap.get("t"));
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
        //LOG.info("pageview build 1");
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
			paramMap.put("et", "pageview");
    		try{
				eventList.add(builder(paramMap).build());
                //LOG.info("pageview build 2");
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
