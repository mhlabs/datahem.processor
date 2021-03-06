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


public class TrafficEntity extends BaseEntity{
	private List<Parameter> parameters;
	private Map<String, String> campaignParameters = new HashMap<String, String>();
	private Pattern pattern;
    private Matcher matcher;
	private static final Logger LOG = LoggerFactory.getLogger(TrafficEntity.class);
	private static String searchEnginesPattern = "";
	private static String ignoredReferersPattern = "";
	private static String socialNetworksPattern = "";
	
	
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
	
	
	public TrafficEntity(){
		super();
		parameters = Arrays.asList(
			new Parameter("cn", "String", null, 100, "campaign_name", false, "january_boots_promo"),
			new Parameter("cs", "String", null, 100, "campaign_source", false, "email_promo"),
			new Parameter("cm", "String", null, 50, "campaign_medium", false, "email"),
			new Parameter("ck", "String", null, 500, "campaign_keyword", false, "winter boots"),
			new Parameter("cc", "String", null, 500, "campaign_content", false, "email_variation1"),
			new Parameter("ci", "String", null, 100, "campaign_id", false, "12345"),
			new Parameter("gclid", "String", null, 1500, "google_adwords_id", false, "EAIaIQobChMI9unWrdjG3QIVXceyCh3cgAQ_EAEYASAAEgIQBfD_BwD"),
			new Parameter("dclid", "String", null, 1500, "google_display_id", false, "EAIaIQobChMI9unWrdjG3QIVXceyCh3cgAQ_EAEYASAAEgIQBfD_BwX")
		);
	}
	
	public List<Parameter> getParameters(){return parameters;}
	
	
	private boolean trigger(Map<String, String> paramMap){
		if(paramMap.get("t").equals("pageview")){
            parse(paramMap);
        }
		return (null != campaignParameters.getOrDefault("cm", null));
	}
	
	private void parse(Map<String, String> paramMap){
		try{
            URL url;
            if(paramMap.get("dl") != null){
                url = new URL(paramMap.get("dl"));
            } else if (paramMap.get("dh") != null && paramMap.get("dp") != null){
                url = new URL("https://" + paramMap.get("dh") + paramMap.get("dp"));
            }
			else{
                return;
            }
            if(paramMap.get("dr") != null){
                try{
                    URL referer = new URL(paramMap.get("dr"));
				    paramMap.put("drh", referer.getHost());
				    paramMap.put("drp", referer.getPath());
                }catch (MalformedURLException e) {
				   	LOG.error("dr: " + e.toString() + ", paramMap: " + paramMap.toString());
			    }
			}
            //Fix for Single Page Applications where dl and referrer stays the same for each hit but dp is updated
            //String documentLocation = (url.getQuery != null ? url.getPath() + "?" + url.getQuery() : url.getPath());
            //LOG.info(String.valueOf(url.getFile() == paramMap.get("dp")) + " url.getFile() " + url.getFile() + " paramMap.get(dp)" + paramMap.get("dp"));
            if(url.getFile().equals(paramMap.get("dp")) || paramMap.get("dp") == null){
                //LOG.info("traffic parsing, url.getFile() = " + url.getFile() + " and paramMap.get(dp) = " + paramMap.get("dp"));
                //LOG.info("traffic parsing paramMap:" + paramMap.toString());
				if(null != url.getQuery()){
					Map<String, String> campaignMap = FieldMapper.fieldMapFromURL(url);
					//Google Search Ads traffic
					if(campaignMap.get("gclid") != null){
						campaignParameters.put("cn", campaignMap.getOrDefault("utm_campaign", "(not set)"));
						campaignParameters.put("cs", campaignMap.getOrDefault("utm_source","google search ads"));
						campaignParameters.put("cm", campaignMap.getOrDefault("utm_medium","cpc"));
						campaignParameters.put("ck", campaignMap.getOrDefault("utm_term","(not set)"));
						campaignParameters.put("cc", campaignMap.getOrDefault("utm_content","(not set)"));
						campaignParameters.put("gclid", campaignMap.get("gclid"));
						return;
					}
					//Google Display & Video traffic
					if(campaignMap.get("dclid") != null){
						campaignParameters.put("cn", campaignMap.getOrDefault("utm_campaign", "(not set)"));
						campaignParameters.put("cs", campaignMap.getOrDefault("utm_source","google display & video"));
						campaignParameters.put("cm", campaignMap.getOrDefault("utm_medium", "cpm"));
						campaignParameters.put("ck", campaignMap.getOrDefault("utm_term", "(not set)"));
						campaignParameters.put("cc", campaignMap.getOrDefault("utm_content", "(not set)"));
						campaignParameters.put("dclid", campaignMap.get("dclid"));
						return;
					}
					//campaign traffic
					if(campaignMap.get("utm_source") != null){
						campaignParameters.put("cn", campaignMap.getOrDefault("utm_campaign", "(not set)"));
						campaignParameters.put("cs", campaignMap.get("utm_source"));
						campaignParameters.put("cm", campaignMap.getOrDefault("utm_medium", "(not set)"));
						campaignParameters.put("ck", campaignMap.getOrDefault("utm_term", "(not set)"));
						campaignParameters.put("cc", campaignMap.getOrDefault("utm_content", "(not set)"));
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
			}
			catch (MalformedURLException e) {
				LOG.error(e.toString() + ", paramMap: " + paramMap.toString());
			}
			return;
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
			paramMap.put("et", "traffic");   		
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
