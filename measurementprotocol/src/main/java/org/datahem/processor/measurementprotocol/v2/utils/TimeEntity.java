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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimeEntity{
    private String timeZone;
	
	private static final Logger LOG = LoggerFactory.getLogger(TimeEntity.class);
	
    public String getTimeZone(){
    	return this.timeZone;
  	}

	public void setTimeZone(String tz){
    	this.timeZone = tz;
  	}

	public TimeEntity(){}
	
	public Time build(Map<String, String> pm){
        try{   
            DateTime localDateTime = DateTime.parse(pm.get("timestamp")).withZone(DateTimeZone.forID(getTimeZone()));
            Time.Builder builder = Time.newBuilder();
            Optional.ofNullable(localDateTime.toString(DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss"))).ifPresent(builder::setDateTime);
            Optional.ofNullable(localDateTime.toString(DateTimeFormat.forPattern("YYYY-MM-dd"))).ifPresent(builder::setDate);
            Optional.ofNullable(localDateTime.toString(DateTimeFormat.forPattern("HH:mm:ss"))).ifPresent(builder::setTime);
            /*
            FieldMapper.intVal(localDateTime.toString(DateTimeFormat.forPattern("YYYY"))).ifPresent(g -> builder.setYear(g.intValue()));
            FieldMapper.intVal(localDateTime.toString(DateTimeFormat.forPattern("MM"))).ifPresent(g -> builder.setMonth(g.intValue()));
            FieldMapper.intVal(localDateTime.toString(DateTimeFormat.forPattern("w"))).ifPresent(g -> builder.setWeek(g.intValue()));
            FieldMapper.intVal(localDateTime.toString(DateTimeFormat.forPattern("dd"))).ifPresent(g -> builder.setDay(g.intValue()));
            FieldMapper.intVal(localDateTime.toString(DateTimeFormat.forPattern("HH"))).ifPresent(g -> builder.setHour(g.intValue()));
            FieldMapper.intVal(localDateTime.toString(DateTimeFormat.forPattern("mm"))).ifPresent(g -> builder.setMinute(g.intValue()));
            FieldMapper.intVal(localDateTime.toString(DateTimeFormat.forPattern("ss"))).ifPresent(g -> builder.setSecond(g.intValue()));
            FieldMapper.intVal(localDateTime.toString(DateTimeFormat.forPattern("e"))).ifPresent(g -> builder.setWeekDay(g.intValue()));
            */
            Optional.ofNullable(timeZone).ifPresent(builder::setTimeZone);
            return builder.build();
        }
        catch(IllegalArgumentException e){
            LOG.error(e.toString());
            return null;
        }
	}
}
