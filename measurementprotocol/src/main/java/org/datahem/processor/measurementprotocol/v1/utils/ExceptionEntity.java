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
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ExceptionEntity extends BaseEntity{
	private List<Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(ExceptionEntity.class);
	
	public ExceptionEntity(){
		super();
		parameters = Arrays.asList(
			new Parameter("exd", "String", null, 150, "exception_description", false, "Database Error"),
			new Parameter("exf", "Boolean", null, 150, "exception_fatal", false, 0)
		);
	}
	
	public List<Parameter> getParameters(){return parameters;}

	
	private boolean trigger(Map<String, String> paramMap){
		return (null != paramMap.get("exd") || null != paramMap.get("exf"));
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> mpEntities = new ArrayList<>();
		if(trigger(paramMap)){
			paramMap.put("et", "exception");
			try{
				mpEntities.add(builder(paramMap).build());
				return mpEntities;
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
	
	public MPEntity.Builder builder(Map<String, String> paramMap, MPEntity.Builder mpEntityBuilder) throws IllegalArgumentException{
		return super.builder(paramMap, mpEntityBuilder, this.parameters);
	}	
	
}
