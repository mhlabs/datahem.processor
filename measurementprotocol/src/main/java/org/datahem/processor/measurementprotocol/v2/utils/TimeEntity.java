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


import org.datahem.protobuf.measurementprotocol.v2.Time;

import java.util.Map;
import java.util.Optional;
import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimeEntity{
	
	private static final Logger LOG = LoggerFactory.getLogger(TimeEntity.class);
	
	public TimeEntity(){}
	
	private boolean trigger(Map<String, String> paramMap){
        return true;
    }
	
	public Time build(Map<String, String> pm){
		if(trigger(pm)){
            try{
                Time.Builder builder = Time.newBuilder();

                /*
                string dateTime = 1; // local datetime YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
                string date = 2; // local date
                string time = 3; // local time
                int32 year = 4; // local year
                int32 month = 5; // local month
                int32 week = 6; // local week number
                int32 day = 7; // local day number
                int32 hour = 8; // local hour in which the hit occurred (0 to 23).
                int32 minute = 9; // local minute in which the hit occurred (0 to 59).
                int32 second = 10; // local second
                string weekDay = 11; // local day of week
                string timeZone = 12; // local timeZone
                */

                Optional.ofNullable(pm.get("timestamp")).ifPresent(builder::setDateTime);
                Optional.ofNullable(pm.get("ctz")).ifPresent(builder::setTimeZone);
                //Optional.ofNullable(pm.get("el")).ifPresent(builder::setLabel);
                //FieldMapper.intVal(pm.get("ev")).ifPresent(g -> builder.setValue(g.intValue()));
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
