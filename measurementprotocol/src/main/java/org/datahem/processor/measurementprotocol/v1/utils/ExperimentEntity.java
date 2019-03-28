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
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExperimentEntity extends BaseEntity{
	private List<Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(ExperimentEntity.class);

	public ExperimentEntity(){
		super();
		parameters = Arrays.asList(
			new Parameter("expi", "String", null, 150, "experiment_id", true, "fdslkjdflsj"),
			new Parameter("expv", "String", null, 500, "experiment_variant", true, "0")
		);
	}
	
	public List<Parameter> getParameters(){return parameters;}
	
	private boolean trigger(Map<String, String> paramMap){
		return (null != paramMap.get("exp") || (null != paramMap.get("xid") && (null != paramMap.get("xvar"))));
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
    		try{
				paramMap.put("et", "experiment");
				if(null != paramMap.get("exp")){
					try{
	    				Pattern.compile("!")
	    					.splitAsStream(paramMap.get("exp"))
	        				.map(s -> Arrays.copyOf(s.split("\\."), 2))
	        				.forEach(s -> {
	        					paramMap.put("expi",s[0]);
	        					paramMap.put("expv", s[1]);
	        					eventList.add(builder(paramMap).build());
	        				});
        			}catch(NullPointerException e) {
						return null;
					}	
				}
				if((null != paramMap.get("xid") && (null != paramMap.get("xvar")))){
					try{
	    				paramMap.put("expi",paramMap.get("xid"));
	        			paramMap.put("expv", paramMap.get("xvar"));
	        			eventList.add(builder(paramMap).build());
	        		}
        			catch(NullPointerException e) {
						return null;
					}	
				}

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
