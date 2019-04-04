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

import org.datahem.protobuf.measurementprotocol.v2.Latency;

import java.util.Map;
import java.util.Optional;
import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyEntity{

	private static final Logger LOG = LoggerFactory.getLogger(LatencyEntity.class);

	public LatencyEntity(){}
	
	private boolean trigger(Map<String, String> paramMap){
		return ("timing".equals(paramMap.get("utc")) && null != paramMap.get("utc") && null != paramMap.get("utv") && null != paramMap.get("utt"));
	}
	
	public Latency build(Map<String, String> pm){
		if(trigger(pm)){
    		try{
                Latency.Builder builder = Latency.newBuilder();
                Optional.ofNullable(pm.get("utc")).ifPresent(builder::setUserTimingCategory);
                Optional.ofNullable(pm.get("utl")).ifPresent(builder::setUserTimingLabel);
                Optional.ofNullable(pm.get("utv")).ifPresent(builder::setUserTimingVariable);
                FieldMapper.intVal(pm.get("dns")).ifPresent(g -> builder.setDomainLookupTime(g.intValue()));
                FieldMapper.intVal(pm.get("clt")).ifPresent(g -> builder.setDomContentLoadedTime(g.intValue()));
                FieldMapper.intVal(pm.get("dit")).ifPresent(g -> builder.setDomInteractiveTime(g.intValue()));
                FieldMapper.intVal(pm.get("pdt")).ifPresent(g -> builder.setPageDownloadTime(g.intValue()));
                FieldMapper.intVal(pm.get("plt")).ifPresent(g -> builder.setPageLoadTime(g.intValue()));
                FieldMapper.intVal(pm.get("rrt")).ifPresent(g -> builder.setRedirectionTime(g.intValue()));
                FieldMapper.intVal(pm.get("tcp")).ifPresent(g -> builder.setServerConnectionTime(g.intValue()));
                FieldMapper.intVal(pm.get("srt")).ifPresent(g -> builder.setServerResponseTime(g.intValue()));
                FieldMapper.intVal(pm.get("utt")).ifPresent(g -> builder.setUserTimingValue(g.intValue()));
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
