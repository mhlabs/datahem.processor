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
import org.datahem.processor.measurementprotocol.utils.BaseEntity;
import org.datahem.processor.measurementprotocol.utils.Parameter;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.net.URL;
import java.net.MalformedURLException;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SiteSearchEntity extends BaseEntity{
	private Map<String, Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(TrafficEntity.class);
	private static String siteSearchPattern = ".*q=(([^&#]*)|&|#|$)";
	
	
	public String getSiteSearchPattern(){
    	return this.siteSearchPattern;
  	}

	public void setSiteSearchPattern(String pattern){
    	this.siteSearchPattern = pattern;
  	}
	
	public SiteSearchEntity(){
		super();
		parameters = new HashMap<String, Parameter>();
		parameters.put("SITE_SEARCH_TERM", new Parameter("sst", "String", null, 100, "siteSearchTerm", false));
		parameters.put("SITE_SEARCH_URL", new Parameter("dl", "String", null, 100, "siteSearchURL", false));
		parameters.put("SITE_SEARCH_PATH", new Parameter("dp", "String", null, 100, "siteSearchPath", false));
	}
	
	private boolean trigger(Map<String, String> paramMap){
		Pattern pattern = Pattern.compile(siteSearchPattern);
        Matcher matcher = pattern.matcher(paramMap.get("dl"));
		return (matcher.find());
	}
	

	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
			paramMap.put("ht", "siteSearch");
			Pattern pattern = Pattern.compile(siteSearchPattern);
			Matcher matcher = pattern.matcher(paramMap.get("dl"));
			if(matcher.find()){
				paramMap.put("sst", FieldMapper.decode(matcher.group(1)));
			}
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
