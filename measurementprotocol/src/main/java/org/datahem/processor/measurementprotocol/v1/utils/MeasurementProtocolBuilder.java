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


import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.datahem.processor.utils.FieldMapper;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.MPEntity;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MeasurementProtocolBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(MeasurementProtocolBuilder.class);

    private Map<String, String> paramMap;
    private List<MPEntity> events = new ArrayList<>();
    private PageviewEntity pageviewEntity = new PageviewEntity();
    private EventEntity eventEntity = new EventEntity();
    private ExceptionEntity exceptionEntity = new ExceptionEntity();
    private SocialEntity socialEntity = new SocialEntity();
    private TimingEntity timingEntity = new TimingEntity();
    private TransactionEntity transactionEntity = new TransactionEntity();
    private ProductEntity productEntity = new ProductEntity();
    private TrafficEntity trafficEntity = new TrafficEntity();
    private PromotionEntity promotionEntity = new PromotionEntity();
    private ExperimentEntity experimentEntity = new ExperimentEntity();

    private ProductImpressionEntity productImpressionEntity = new ProductImpressionEntity();

    private SiteSearchEntity siteSearchEntity = new SiteSearchEntity();
    private static String excludedBotsPattern;
    private static String includedHostnamesPattern;
    private static String timeZone;

    public MeasurementProtocolBuilder() {
    }


    public String getSearchEnginesPattern() {
        return this.trafficEntity.getSearchEnginesPattern();
    }

    public void setSearchEnginesPattern(String pattern) {
        this.trafficEntity.setSearchEnginesPattern(pattern);
    }

    public String getSocialNetworksPattern() {
        return this.trafficEntity.getSocialNetworksPattern();
    }

    public void setSocialNetworksPattern(String pattern) {
        this.trafficEntity.setSocialNetworksPattern(pattern);
    }

    public String getIgnoredReferersPattern() {
        return this.trafficEntity.getIgnoredReferersPattern();
    }

    public void setIgnoredReferersPattern(String pattern) {
        this.trafficEntity.setIgnoredReferersPattern(pattern);
    }


    public String getExcludedBotsPattern() {
        return this.excludedBotsPattern;
    }

    public void setExcludedBotsPattern(String pattern) {
        this.excludedBotsPattern = pattern;
    }

    public String getIncludedHostnamesPattern() {
        return this.includedHostnamesPattern;
    }

    public void setIncludedHostnamesPattern(String pattern) {
        this.includedHostnamesPattern = pattern;
    }


    public String getSiteSearchPattern() {
        return this.siteSearchEntity.getSiteSearchPattern();
    }

    public void setSiteSearchPattern(String pattern) {
        this.siteSearchEntity.setSiteSearchPattern(pattern);
    }


    public String getTimeZone() {
        return this.timeZone;
    }

    public void setTimeZone(String tz) {
        this.timeZone = tz;
    }

    public List<MPEntity> mpEntitiesFromPayload(PubsubMessage message) {
        try {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            //Check if post body contains payload and add parameters in a map
            if (!"".equals(payload)) {
                //Add header parameters to paramMap
                paramMap = FieldMapper.fieldMapFromQuery(payload);
                paramMap.putAll(message.getAttributeMap());

                //Exclude bots, spiders and crawlers
                if (paramMap.get("User-Agent") == null) {
                    paramMap.put("User-Agent", "");
                }

                if (!paramMap.get("User-Agent").matches(getExcludedBotsPattern()) && paramMap.get("dl").matches(getIncludedHostnamesPattern()) && !paramMap.get("t").equals("adtiming")) {
                    //Add epochMillis and timestamp to paramMap       
                    Instant payloadTimeStamp = new Instant(Long.parseLong(paramMap.get("MessageTimestamp")));
                    //DateTimeFormatter utc_timestamp = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZoneUTC();
                    DateTimeFormatter local_timestamp = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZone(DateTimeZone.forID(getTimeZone()));
                    paramMap.put("cpts", payloadTimeStamp.toString(local_timestamp));
                    paramMap.put("cpem", paramMap.get("MessageTimestamp"));

                    //Set local timezone for use as partition field
                    DateTimeFormatter partition = DateTimeFormat.forPattern("YYYY-MM-dd").withZone(DateTimeZone.forID(getTimeZone()));
                    paramMap.put("cpd", payloadTimeStamp.toString(partition));

                    addAllIfNotNull(events, pageviewEntity.build(paramMap));
                    addAllIfNotNull(events, eventEntity.build(paramMap));
                    addAllIfNotNull(events, exceptionEntity.build(paramMap));
                    addAllIfNotNull(events, siteSearchEntity.build(paramMap));
                    addAllIfNotNull(events, socialEntity.build(paramMap));
                    addAllIfNotNull(events, timingEntity.build(paramMap));
                    addAllIfNotNull(events, transactionEntity.build(paramMap));
                    addAllIfNotNull(events, productEntity.build(paramMap));
                    addAllIfNotNull(events, trafficEntity.build(paramMap));
                    addAllIfNotNull(events, promotionEntity.build(paramMap));
                    addAllIfNotNull(events, productImpressionEntity.build(paramMap));
                    addAllIfNotNull(events, experimentEntity.build(paramMap));
                }
            } else {
                LOG.info("not matching MeasurementProtocolBuilder conditions: User-Agent: " + paramMap.getOrDefault("User-Agent", "null") + ", document.location: " + paramMap.getOrDefault("dl", "null") + ", type:" + paramMap.getOrDefault("t", "null"));
            }
        } catch (Exception e) {
            LOG.error(e.toString() + " paramMap:" + paramMap.toString());
        }
        return events;
    }

    public static void addAllIfNotNull(List<MPEntity> list, List<MPEntity> c) {
        if (c != null) {
            list.addAll(c);
        }
    }
}
