package org.datahem.processor.measurementprotocol.v1.utils;

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


import org.datahem.processor.utils.FieldMapper;
import org.datahem.processor.measurementprotocol.v1.utils.BaseEntity;
import org.datahem.processor.measurementprotocol.v1.utils.Parameter;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.net.URL;
import java.net.MalformedURLException;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SiteSearchEntity extends BaseEntity{
	private List<Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(SiteSearchEntity.class);
	private static String siteSearchPattern = ".*q=(([^&#]*)|&|#|$)";
	
	
	public String getSiteSearchPattern(){
    	return this.siteSearchPattern;
  	}

	public void setSiteSearchPattern(String pattern){
    	this.siteSearchPattern = pattern;
  	}
	
	public SiteSearchEntity(){
		super();
		parameters = Arrays.asList(
			new Parameter("sst", "String", null, 100, "search_term", false, "creme fraiche"),
			new Parameter("dl", "String", null, 100, "search_url", false, "http://foo.com/home?q=creme%20fraiche"),
			new Parameter("dp", "String", null, 100, "search_path", false, "/home")
		);
	}
	
	public List<Parameter> getParameters(){return parameters;}
	
	
	private boolean trigger(Map<String, String> paramMap){
		Pattern pattern = Pattern.compile(siteSearchPattern);
		Matcher matcher = pattern.matcher(paramMap.get("dl"));
		return (matcher.find());
	}

	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
			paramMap.put("et", "search");
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
