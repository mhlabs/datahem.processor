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

public class SocialEntity extends BaseEntity {
    private List<Parameter> parameters;
    private static final Logger LOG = LoggerFactory.getLogger(SocialEntity.class);


    public SocialEntity() {
        super();
        parameters = Arrays.asList(
                new Parameter("sn", "String", null, 50, "social_network", true, "facebook"),
                new Parameter("sa", "String", null, 50, "social_action", true, "like"),
                new Parameter("st", "String", null, 2048, "social_action_target", true, "http://foo.com")
        );
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    private boolean trigger(Map<String, String> paramMap) {
        return (null != paramMap.get("sn") && null != paramMap.get("sa") && null != paramMap.get("st"));
    }

    public List<MPEntity> build(Map<String, String> paramMap) {
        List<MPEntity> eventList = new ArrayList<>();
        if (trigger(paramMap)) {
            paramMap.put("et", "social");
            try {
                eventList.add(builder(paramMap).build());
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
