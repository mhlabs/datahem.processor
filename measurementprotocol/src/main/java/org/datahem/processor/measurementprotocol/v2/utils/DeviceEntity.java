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


import org.datahem.protobuf.measurementprotocol.v2.Device;

import java.util.Map;
import java.util.Optional;
import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class DeviceEntity{
	
	private static final Logger LOG = LoggerFactory.getLogger(DeviceEntity.class);
	
	public DeviceEntity(){}
	
	private boolean trigger(Map<String, String> paramMap){
		return true;
	}
	
	public Device build(Map<String, String> pm){
		if(trigger(pm)){
            try{
                Device.Builder builder = Device.newBuilder();
                Optional.ofNullable(pm.get("vp")).ifPresent(builder::setBrowserSize);
                Optional.ofNullable(pm.get("fl")).ifPresent(builder::setFlashVersion);
                FieldMapper.intVal(pm.get("je")).ifPresent(g -> builder.setJavaEnabled(g.intValue()));
                Optional.ofNullable(pm.get("ul")).ifPresent(builder::setLanguage);
                Optional.ofNullable(pm.get("sd")).ifPresent(builder::setScreenColors);
                Optional.ofNullable(pm.get("sr")).ifPresent(builder::setScreenResolution);
                Optional.ofNullable(getFirstParameter(pm, "ua|user-agent|User-Agent")).ifPresent(builder::setUserAgent);
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

    private String getFirstParameter(Map<String, String> prParamMap, String parameterPattern){
        Pattern pattern = Pattern.compile(parameterPattern);
 		Optional<String> firstElement = prParamMap
 			.keySet()
 			.stream()
 			.filter(pattern.asPredicate())
			.findFirst();
        return prParamMap.get(firstElement.orElse(null));    
    }
}
