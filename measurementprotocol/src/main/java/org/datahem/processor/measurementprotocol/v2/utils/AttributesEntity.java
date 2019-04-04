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


import org.datahem.protobuf.measurementprotocol.v2._ATTRIBUTES;

import java.util.Map;
import java.util.Optional;
//import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AttributesEntity{
	
	private static final Logger LOG = LoggerFactory.getLogger(AttributesEntity.class);
	
	public AttributesEntity(){}
	
	private boolean trigger(Map<String, String> paramMap){
        return true;
    }
	
	public _ATTRIBUTES build(Map<String, String> pm){
		//if(trigger(pm)){
            try{
                _ATTRIBUTES.Builder builder = _ATTRIBUTES.newBuilder();
                Optional.ofNullable(pm.get("uuid")).ifPresent(builder::setUuid);
                Optional.ofNullable(pm.get("source")).ifPresent(builder::setSource);
                Optional.ofNullable(pm.get("timestamp")).ifPresent(builder::setTimestamp);
                return builder.build();
			}
			catch(IllegalArgumentException e){
				LOG.error(e.toString());
				return null;
			}
		//}
		//else{
		//	return null;
		//}
	}
}
