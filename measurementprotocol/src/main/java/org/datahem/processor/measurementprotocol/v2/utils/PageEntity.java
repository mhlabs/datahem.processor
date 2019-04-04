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

import java.util.Map;
import org.datahem.protobuf.measurementprotocol.v2.Page;
import java.util.Optional;
import org.datahem.processor.utils.FieldMapper;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageEntity{
	private static final Logger LOG = LoggerFactory.getLogger(PageEntity.class);
    private static String siteSearchPattern = ".*q=(([^&#]*)|&|#|$)";
	
	public PageEntity(){}

    public String getSiteSearchPattern(){
    	return this.siteSearchPattern;
  	}

	public void setSiteSearchPattern(String pattern){
    	this.siteSearchPattern = pattern;
  	}
	
	private boolean trigger(Map<String, String> paramMap){
        return true;
    }
	
	public Page build(Map<String, String> pm){
		if(trigger(pm)){
            try{
                Page.Builder builder = Page.newBuilder();
                Pattern pattern = Pattern.compile(siteSearchPattern);
			    Matcher matcher = pattern.matcher(pm.get("dlu"));
			    if(matcher.find()){
				    //paramMap.put("sst", FieldMapper.decode(matcher.group(1)));
                    Optional.ofNullable(FieldMapper.decode(matcher.group(1))).ifPresent(builder::setSearchKeyword);
			    }
                Optional.ofNullable(pm.get("dt")).ifPresent(builder::setTitle);
                Optional.ofNullable(pm.get("dlu")).ifPresent(builder::setUrl);
                Optional.ofNullable(pm.get("dh")).ifPresent(builder::setHostname);
                Optional.ofNullable(pm.get("dp").split("\\?")[0]).ifPresent(builder::setPath);
                Optional.ofNullable(pm.get("dr")).ifPresent(builder::setReferer);
                Optional.ofNullable(pm.get("drh")).ifPresent(builder::setRefererHost);
                Optional.ofNullable(pm.get("drp")).ifPresent(builder::setRefererPath);
                Optional.ofNullable(pm.get("de")).ifPresent(builder::setEncoding);
                Optional.ofNullable(pm.get("linkid")).ifPresent(builder::setLinkId);
                //Optional.ofNullable(pm.get("sst")).ifPresent(builder::setSearchKeyword);
                return builder.build();
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
}
