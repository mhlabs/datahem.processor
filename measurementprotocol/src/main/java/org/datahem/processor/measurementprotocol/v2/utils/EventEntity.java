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


import org.datahem.protobuf.measurementprotocol.v2.Event;

import java.util.Map;
import java.util.Optional;
import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventEntity{
	
	private static final Logger LOG = LoggerFactory.getLogger(EventEntity.class);
	
	public EventEntity(){}
	
	private boolean trigger(Map<String, String> paramMap){
		return "event".equals(paramMap.get("t"));
	}
	
	public Event build(Map<String, String> pm){
		if(trigger(pm)){
            try{
                Event.Builder builder = Event.newBuilder();
                Optional.ofNullable(FieldMapper.stringVal(pm.get("ec"))).ifPresent(builder::setCategory);
                Optional.ofNullable(FieldMapper.stringVal(pm.get("ea"))).ifPresent(builder::setAction);
                Optional.ofNullable(FieldMapper.stringVal(pm.get("el"))).ifPresent(builder::setLabel);
                //Optional.ofNullable(FieldMapper.intVal(pm.get("ev"))).ifPresent(builder::setValue);
                //Optional.ofNullable(FieldMapper.intVal(pm.get("ev"))).ifPresent(g -> builder.setValue(g.get().intValue()));
                FieldMapper.intVal(pm.get("ev")).ifPresent(g -> builder.setValue(g.intValue()));
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
