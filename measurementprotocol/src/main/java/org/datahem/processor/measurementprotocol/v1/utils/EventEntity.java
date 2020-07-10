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


public class EventEntity extends BaseEntity {

    private List<Parameter> parameters;
    private static final Logger LOG = LoggerFactory.getLogger(EventEntity.class);

    public EventEntity() {
        super();
        parameters = Arrays.asList(
                new Parameter("ec", "String", null, 150, "event_category", true, "Category"),
                new Parameter("ea", "String", null, 500, "event_action", true, "Action"),
                new Parameter("el", "String", null, 500, "event_label", false, "Label"),
                new Parameter("ev", "Integer", null, 100, "event_value", false, 55)
        );
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    private boolean trigger(Map<String, String> paramMap) {
        return "event".equals(paramMap.get("t"));
    }

    public List<MPEntity> build(Map<String, String> paramMap) {
        List<MPEntity> mpEntities = new ArrayList<>();
        if (trigger(paramMap)) {
            paramMap.put("et", "event");
            try {
                mpEntities.add(builder(paramMap).build());
                return mpEntities;
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

    public MPEntity.Builder builder(Map<String, String> paramMap, MPEntity.Builder mpEntityBuilder) throws IllegalArgumentException {
        return super.builder(paramMap, mpEntityBuilder, this.parameters);
    }
}
