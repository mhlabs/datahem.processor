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

import org.datahem.protobuf.measurementprotocol.v2.Exception;

import java.util.Map;
import java.util.Optional;
import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ExceptionEntity{
	private static final Logger LOG = LoggerFactory.getLogger(ExceptionEntity.class);
	
	public ExceptionEntity(){}
	
	private boolean trigger(Map<String, String> paramMap){
		return (null != paramMap.get("exd") || null != paramMap.get("exf"));
	}
	
	public Exception build(Map<String, String> pm){
		if(trigger(pm)){
            try{
                Exception.Builder builder = Exception.newBuilder();
                Optional.ofNullable(FieldMapper.stringVal(pm.get("exd"))).ifPresent(builder::setDescription);
                //Optional.ofNullable(FieldMapper.intVal(pm.get("exf"))).ifPresent(builder::setIsFatal);
                //Optional.ofNullable(FieldMapper.intVal(pm.get("exf"))).ifPresent(g -> builder.setIsFatal(g.get().intValue()));
                FieldMapper.intVal(pm.get("exf")).ifPresent(g -> builder.setIsFatal(g.intValue()));
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
