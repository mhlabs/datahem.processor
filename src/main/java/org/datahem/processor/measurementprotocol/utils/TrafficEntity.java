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
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.net.URL;
import java.net.MalformedURLException;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TrafficEntity extends BaseEntity{
	private Map<String, Parameter> parameters;
	private Map<String, String> campaignParameters = new HashMap<String, String>();
	private Pattern pattern;
    private Matcher matcher;
	private static final Logger LOG = LoggerFactory.getLogger(TrafficEntity.class);
	private static String searchEnginesPattern = ".*www\\.google\\..*|.*www\\.bing\\..*|.*search\\.yahoo\\..*";  
	private static String ignoredReferersPattern = ".*mathem\\.se.*";
	private static String socialNetworksPattern = ".*facebook\\..*|.*instagram\\..*|.*pinterest\\..*|.*youtube\\..*|.*linkedin\\..*|.*twitter\\..*";
	
	
	public String getSearchEnginesPattern(){
    	return this.searchEnginesPattern;
  	}

	public void setSearchEnginesPattern(String pattern){
    	this.searchEnginesPattern = pattern;
  	}
  	
  	public String getIgnoredReferersPattern(){
    	return this.ignoredReferersPattern;
  	}

	public void setIgnoredReferersPattern(String pattern){
    	this.ignoredReferersPattern = pattern;
  	}
  	
  	public String getSocialNetworksPattern(){
    	return this.socialNetworksPattern;
  	}

	public void setSocialNetworksPattern(String pattern){
    	this.socialNetworksPattern = pattern;
  	}
	
	/*
	public TrafficEntity(String searchEnginesPattern){
		this();
		this.searchEnginesPattern = searchEnginesPattern;
	}*/
	
	public TrafficEntity(){
		super();
		parameters = new HashMap<String, Parameter>();
		parameters.put("CAMPAIGN_NAME", new Parameter("cn", "String", null, 100, "campaignName", false));
		parameters.put("CAMPAIGN_SOURCE", new Parameter("cs", "String", null, 100, "campaignSource", false));
		parameters.put("CAMPAIGN_MEDIUM", new Parameter("cm", "String", null, 50, "campaignMedium", false));
		parameters.put("CAMPAIGN_KEYWORD", new Parameter("ck", "String", null, 500, "campaignKeyword", false));
		parameters.put("CAMPAIGN_CONTENT", new Parameter("cc", "String", null, 500, "campaignContent", false));
		parameters.put("CAMPAIGN_ID", new Parameter("ci", "String", null, 100, "campaignId", false));
		parameters.put("GOOGLE_ADWORDS_ID", new Parameter("gclid", "String", null, 1500, "googleAdwordsId", false));
		parameters.put("GOOGLE_DISPLAY_ADS_ID", new Parameter("dclid", "String", null, 1500, "googleDisplayId", false));
	}
	
	private boolean trigger(Map<String, String> paramMap){
		parse(paramMap);
		return (null != campaignParameters.getOrDefault("cm", null));
	}
	
	private void parse(Map<String, String> paramMap){
		try{
				URL url = new URL(paramMap.get("dl"));

				if(null != url.getQuery()){
					Map<String, String> campaignMap = FieldMapper.fieldMapFromURL(url);
					//Adwords traffic
					if(campaignMap.get("gclid") != null){
						campaignParameters.put("cn", "adwords");
						campaignParameters.put("cs", "google");
						campaignParameters.put("cm", "cpc");
						campaignParameters.put("ck", "adwords");
						campaignParameters.put("cc", "adwords");
						campaignParameters.put("gclid", campaignMap.get("gclid"));
						return;
					}
					//double click traffic
					if(campaignMap.get("gclsrc") != null){
						campaignParameters.put("cn", "DoubleClick");
						campaignParameters.put("cs", "google");
						campaignParameters.put("cm", "cpc");
						campaignParameters.put("ck", "DoubleClick");
						campaignParameters.put("cc", "DoubleClick");
						campaignParameters.put("gclsrc", campaignMap.get("gclsrc"));
						return;
					}
					//campaign traffic
					if(campaignMap.get("utm_source") != null){
						campaignParameters.put("cn", (campaignMap.get("utm_campaign") == null) ? "(not set)" : campaignMap.get("utm_campaign"));
						campaignParameters.put("cs", campaignMap.get("utm_source"));
						campaignParameters.put("cm", (campaignMap.get("utm_medium") == null) ? "(not set)" : campaignMap.get("utm_medium"));
						campaignParameters.put("ck", (campaignMap.get("utm_term") == null) ? "(not set)" : campaignMap.get("utm_term"));
						campaignParameters.put("cc", (campaignMap.get("utm_content") == null) ? "(not set)" : campaignMap.get("utm_content"));
						return;
					}
				}
				//Search Engine or Referer or Social
				if(paramMap.get("dr") != null){
					
					//Exclude self referal
					pattern = Pattern.compile(ignoredReferersPattern);
					matcher = pattern.matcher(paramMap.getOrDefault("drh", ""));
					if(matcher.find()){
						return;
					}
					
					//Organic search
					pattern = Pattern.compile(searchEnginesPattern);
        			matcher = pattern.matcher(paramMap.get("dr"));
					if(matcher.find()){
						campaignParameters.put("cn", "(not set)");
						campaignParameters.put("cs", paramMap.get("drh"));
						campaignParameters.put("cm", "organic");
						if(matcher.find()) campaignParameters.put("ck", (matcher.group(1) == null) ? "(not provided)" : matcher.group(1));
						else campaignParameters.put("ck", "(not provided)");
						campaignParameters.put("cc", "(not set)");
						return;
					}
					
					//Social
					pattern = Pattern.compile(socialNetworksPattern);
        			matcher = pattern.matcher(paramMap.get("dr"));
					if(matcher.find()){
						campaignParameters.put("cn", "(not set)");
						campaignParameters.put("cs", paramMap.get("drh"));
						campaignParameters.put("cm", "social");
						campaignParameters.put("ck", "(not set)");
						campaignParameters.put("cc", "(not set)");
						return;
					}
					
					//Referer
					campaignParameters.put("cn", "(referal)");
					campaignParameters.put("cs", paramMap.get("drh"));
					campaignParameters.put("cm", "referal");
					campaignParameters.put("ck", "(not set)");
					campaignParameters.put("cc", paramMap.get("drp"));
					return;
        		}
				
			}
			catch (MalformedURLException e) {
				LOG.error(e.toString());
			}
			return;
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
			paramMap.put("ht", "traffic");   		
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
		paramMap.putAll(campaignParameters);
		return super.builder(paramMap, eventBuilder, this.parameters);
	}
}
