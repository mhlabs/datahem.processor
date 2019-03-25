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


import org.datahem.processor.measurementprotocol.v1.utils.BaseEntity;
import org.datahem.processor.measurementprotocol.v1.utils.Parameter;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimingEntity extends BaseEntity{
	private List<Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(TimingEntity.class);

	public TimingEntity(){
		super();
		parameters = Arrays.asList(
			new Parameter("utc", "String", null, 150, "user_timing_category", true,"category"),
			new Parameter("utv", "String", null, 500, "user_timing_variable_name", true,"lookup"),
			new Parameter("utt", "Integer", null, 500, "user_timing_time", true, 123),
			new Parameter("utl", "String", null, 500, "user_Timing_label", false, "label"),
			new Parameter("plt", "Integer", null, 500, "page_load_time", false, 3554),
			new Parameter("dns", "Integer", null, 500, "dns_time", false, 43),
			new Parameter("pdt", "Integer", null, 500, "page_download_time", false, 500),
			new Parameter("rrt", "Integer", null, 500, "redirect_response_time", false, 500),
			new Parameter("tcp", "Integer", null, 500, "tcp_connect_time", false, 500),
			new Parameter("srt", "Integer", null, 500, "server_response_time", false, 500),
			new Parameter("dit", "Integer", null, 500, "dom_interactive_time", false, 500),
			new Parameter("clt", "Integer", null, 500, "content_load_time", false, 500)
		);
	}
	
	public List<Parameter> getParameters(){return parameters;}
	
	private boolean trigger(Map<String, String> paramMap){
		return (null != paramMap.get("utc") && null != paramMap.get("utv") && null != paramMap.get("utt"));
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
    		try{
				paramMap.put("et", "timing");
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
