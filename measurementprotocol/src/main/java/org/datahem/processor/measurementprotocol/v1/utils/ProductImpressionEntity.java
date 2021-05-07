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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class ProductImpressionEntity extends BaseEntity {
    private List<Parameter> parameters;
    private static final Logger LOG = LoggerFactory.getLogger(ProductImpressionEntity.class);

    public ProductImpressionEntity() {
        super();
        parameters = Arrays.asList(
                new Parameter("(il[0-9]{1,3}nm)", "String", null, 500, "product_list_name", false, "il1nm", "Search Results"),
                new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}id)", "String", null, 500, "product_id", false, "il1pi1id", "P67890"),
                new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}nm)", "String", null, 500, "product_name", false, "il1pi1nm", "Android T-Shirt"),
                new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}br)", "String", null, 500, "product_brand", false, "il1pi1br", "Google"),
                new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}ca)", "String", null, 500, "product_category", false, "il1pi1ca", "Apparel"),
                new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}va)", "String", null, 500, "product_variant", false, "il1pi1va", "Black"),
                new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}pr)", "Double", null, 500, "product_price", false, "il1pi1pr", 29.20),
                new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}ps)", "Integer", null, 500, "product_position", false, "il1pi1ps", 2),
                new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}cd[0-9]{1,3})", "String", null, 500, "product_custom_dimension", false, "il[0-9]{1,3}pi[0-9]{1,3}cd([0-9]{1,3})", "il1pi1cd1", "Member", "productCustomDimension1"),
                new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}cm[0-9]{1,3})", "Integer", null, 500, "product_custom_metric", false, "il[0-9]{1,3}pi[0-9]{1,3}cm([0-9]{1,3})", "il1pi1cm1", 28, "productCustomMetric1")
        );
    }


    public List<Parameter> getParameters() {
        return parameters;
    }

    private boolean trigger(Map<String, String> paramMap) {
        Pattern productImpressionIndexPattern = Pattern.compile("^il[0-9]{1,3}pi[0-9]{1,3}.*");
        Map<String, List<String>> entries = paramMap
                .keySet()
                .stream()
                .filter(productImpressionIndexPattern.asPredicate())
                .collect(Collectors.groupingBy(s -> s, Collectors.toList()));
        return (entries.size() > 0);
    }

    public List<MPEntity> build(Map<String, String> paramMap) {
        List<MPEntity> eventList = new ArrayList<>();
        if (trigger(paramMap)) {
            paramMap.put("et", "productImpression");

            Pattern productImpressionExclPattern = Pattern.compile("^(?!il[0-9]{1,3}pi.*).*$");
            Map<String, String> paramMapExclPr = paramMap
                    .keySet()
                    .stream()
                    .filter(productImpressionExclPattern.asPredicate())
                    .collect(Collectors.toMap(s -> s, s -> paramMap.get(s)));

            //Group product parameters by list and product index 
            final Pattern productImpressionIndexPattern = Pattern.compile("^il([0-9]{1,3})pi([0-9]{1,3}).*");
            Map<String, List<String>> entries = paramMap
                    .keySet()
                    .stream()
                    .filter(productImpressionIndexPattern.asPredicate())
                    .collect(Collectors.groupingBy(s -> {
                        final Matcher matcher = productImpressionIndexPattern.matcher(s);
                        matcher.find();
                        return matcher.group(1) + matcher.group(2);
                    }, Collectors.toList()));


            //Build a product hit for each produt
            for (Map.Entry<String, List<String>> entry : entries.entrySet()) {
                List<String> keys = entry.getValue();
                Map<String, String> prParamMap = keys
                        .stream()
                        .collect(Collectors.toMap(s -> s, s -> paramMap.get(s)));
                prParamMap.putAll(paramMapExclPr);
                try {
                    MPEntity evp = builder(prParamMap).build();
                    eventList.add(evp);
                } catch (IllegalArgumentException e) {
                    LOG.error(e.toString());
                }
            }
            return eventList;
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
