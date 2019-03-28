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

import org.datahem.protobuf.measurementprotocol.v2.Experiment;

import java.util.Map;
import java.util.Optional;
import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
//import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;


public class ExperimentEntity{
	private static final Logger LOG = LoggerFactory.getLogger(ExperimentEntity.class);

	public ExperimentEntity(){}
	
	private boolean trigger(Map<String, String> paramMap){
		return (null != paramMap.get("exp") || (null != paramMap.get("xid") && (null != paramMap.get("xvar"))));
	}
	
	public ArrayList<Experiment> build(Map<String, String> pm){
		ArrayList<Experiment> eventList = new ArrayList<>();
		if(trigger(pm)){
    		try{
				if(null != pm.get("exp")){
					try{
	    				Pattern.compile("!")
	    					.splitAsStream(pm.get("exp"))
	        				.map(s -> Arrays.copyOf(s.split("\\."), 2))
	        				.forEach(s -> {
                                Experiment.Builder builder = Experiment.newBuilder();
                                Optional.ofNullable(pm.get("dt")).ifPresent(builder::setId);
                                Optional.ofNullable(pm.get("dlu")).ifPresent(builder::setVariant);
	        					eventList.add(builder.build());
	        				});
        			}catch(NullPointerException e) {
                        LOG.error(e.toString());
						return null;
					}	
				}
				if((null != pm.get("xid") && (null != pm.get("xvar")))){
					try{
                        Experiment.Builder builder = Experiment.newBuilder();
                        Optional.ofNullable(pm.get("xid")).ifPresent(builder::setId);
                        Optional.ofNullable(pm.get("xvar")).ifPresent(builder::setVariant);
	        			eventList.add(builder.build());
	        		}
        			catch(NullPointerException e) {
                        LOG.error(e.toString());
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
}
