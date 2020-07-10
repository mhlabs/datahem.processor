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


import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.MPEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PageviewEntity extends BaseEntity {
    private List<Parameter> parameters;
    private static final Logger LOG = LoggerFactory.getLogger(PageviewEntity.class);

    public PageviewEntity() {
        super();
        parameters = new ArrayList<>(Arrays.asList(
                new Parameter("de", "String", "UTF-8", 20, "encoding", false, "UTF-8"),
                new Parameter("fl", "String", null, 20, "flash_version", false, "10 1 r103"),
                new Parameter("sr", "String", null, 20, "screen_resolution", false, "800x600"),
                new Parameter("vp", "String", null, 20, "viewport_size", false, "123x456"),
                new Parameter("sd", "String", null, 20, "screen_colors", false, "24-bits"),
                new Parameter("ul", "String", null, 20, "language", false, "en-us"),
                new Parameter("je", "Integer", null, 20, "java_enabled", false, 1),
                new Parameter("linkid", "String", null, 2048, "link_id", false, "nav_bar")
        ));
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    private boolean trigger(Map<String, String> paramMap) {
        return "pageview".equals(paramMap.get("t"));
    }

    public List<MPEntity> build(Map<String, String> paramMap) {
        //LOG.info("pageview build 1");
        List<MPEntity> eventList = new ArrayList<>();
        if (trigger(paramMap)) {
            paramMap.put("et", "pageview");
            try {
                eventList.add(builder(paramMap).build());
                //LOG.info("pageview build 2");
                return eventList;
            } catch (IllegalArgumentException e) {
                LOG.error(e.toString());
                return null;
            }
        } else {
            return null;
        }
    }

    public MPEntity.Builder builder(Map<String, String> paramMap) throws IllegalArgumentException {
        return builder(paramMap, super.builder(paramMap));
    }

    public MPEntity.Builder builder(Map<String, String> paramMap, MPEntity.Builder eventBuilder) throws IllegalArgumentException {
        return super.builder(paramMap, eventBuilder, this.parameters);
    }
}
