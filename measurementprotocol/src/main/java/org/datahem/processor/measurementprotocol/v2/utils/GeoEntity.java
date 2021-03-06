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

import org.datahem.protobuf.measurementprotocol.v2.Geo;

import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoEntity{	
	private static final Logger LOG = LoggerFactory.getLogger(GeoEntity.class);
	
	public GeoEntity(){}
	
	public Geo build(Map<String, String> pm){
        try{
            Geo.Builder builder = Geo.newBuilder();
            // headers from appEngine
            Optional.ofNullable(pm.get("X-AppEngine-Country")).ifPresent(builder::setCountry);
            Optional.ofNullable(pm.get("X-AppEngine-Region")).ifPresent(builder::setRegion);
            Optional.ofNullable(pm.get("X-AppEngine-City")).ifPresent(builder::setCity);
            Optional.ofNullable(pm.get("X-AppEngine-CityLatLong")).ifPresent(builder::setCityLatLong);
            // headers from cloudfunctions are lowercase
            Optional.ofNullable(pm.get("x-appengine-country")).ifPresent(builder::setCountry);
            Optional.ofNullable(pm.get("x-appengine-region")).ifPresent(builder::setRegion);
            Optional.ofNullable(pm.get("x-appengine-city")).ifPresent(builder::setCity);
            Optional.ofNullable(pm.get("x-appengine-citylatlong")).ifPresent(builder::setCityLatLong);
            return builder.build();
        }
        catch(IllegalArgumentException e){
            LOG.error(e.toString());
            return null;
        }
	}
}
