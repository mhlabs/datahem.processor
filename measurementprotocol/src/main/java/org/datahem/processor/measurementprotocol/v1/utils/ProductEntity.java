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
import java.util.stream.Stream;


public class ProductEntity extends BaseEntity {
    private List<Parameter> parameters;
    private static final Logger LOG = LoggerFactory.getLogger(ProductEntity.class);

    public ProductEntity() {
        super();
        parameters = Arrays.asList(
                new Parameter("(pr[0-9]{1,3}id)", "String", null, 500, "product_id", false, "pr1id", "P12345"),
                new Parameter("(pr[0-9]{1,3}nm)", "String", null, 500, "product_name", false, "pr1nm", "Android T-Shirt"),
                new Parameter("(pr[0-9]{1,3}br)", "String", null, 500, "product_brand", false, "pr1br", "Google"),
                new Parameter("(pr[0-9]{1,3}ca)", "String", null, 500, "product_category", false, "pr1ca", "Apparel/Mens/T-Shirts"),
                new Parameter("(pr[0-9]{1,3}va)", "String", null, 500, "product_variant", false, "pr1va", "Black"),
                new Parameter("(pr[0-9]{1,3}pr)", "Double", null, 500, "product_price", false, "pr1pr", 29.20),
                new Parameter("(pr[0-9]{1,3}qt)", "Integer", null, 500, "product_quantity", false, "pr1qt", 2),
                new Parameter("(pr[0-9]{1,3}cc)", "String", null, 500, "product_coupon_code", false, "pr1cc", "SUMMER_SALE13"),
                new Parameter("(pr[0-9]{1,3}ps)", "Integer", null, 500, "product_position", false, "pr1ps", 2),
                new Parameter("(pr[0-9]{1,3}cd[0-9]{1,3})", "String", null, 500, "product_custom_dimension_", false, "pr[0-9]{1,3}cd([0-9]{1,3})", "pr1cd1", "Member", "product_custom_dimension_1"),
                new Parameter("(pr[0-9]{1,3}cm[0-9]{1,3})", "Integer", null, 500, "product_custom_metric_", false, "pr[0-9]{1,3}cm([0-9]{1,3})", "pr1cm1", 28, "product_custom_metric_1"),
                new Parameter("pa", "String", null, 50, "product_action", true, "detail"),
                new Parameter("pal", "String", null, 500, "product_action_list", false, "Search Results"), //If pa == detail || click
                new Parameter("ti", "String", null, 50, "transaction_id", false, "T1234"), //If pa == purchase
                new Parameter("cos", "Integer", null, 50, "checkout_step", false, 2), //If pa == checkout
                new Parameter("col", "String", null, 50, "checkout_step_option", false, "Visa"), //If pa == checkout
                new Parameter("cu", "String", null, 10, "product_currency", false, "SEK")
        );
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    private boolean trigger(Map<String, String> paramMap) {
        return Stream.of("detail", "click", "add", "remove", "checkout", "purchase", "refund").collect(Collectors.toList()).contains(paramMap.get("pa"));
    }

    public List<MPEntity> build(Map<String, String> paramMap) {
        List<MPEntity> eventList = new ArrayList<>();
        if (trigger(paramMap)) {
            paramMap.put("et", "product_" + paramMap.get("pa"));

            Pattern productExclPattern = Pattern.compile("^(?!pr[0-9]{1,3}.*).*$");
            //final Matcher matcher;
            Map<String, String> paramMapExclPr = paramMap
                    .keySet()
                    .stream()
                    .filter(productExclPattern.asPredicate())
                    .collect(Collectors.toMap(s -> s, s -> paramMap.get(s)));

            //Group product parameters by product index 
            final Pattern productIndexPattern = Pattern.compile("^pr([0-9]{1,3}).*");
            Map<String, List<String>> entries = paramMap
                    .keySet()
                    .stream()
                    .filter(productIndexPattern.asPredicate())
                    .collect(Collectors.groupingBy(s -> {
                        final Matcher matcher = productIndexPattern.matcher(s);
                        matcher.find();
                        return matcher.group(1);
                    }, Collectors.toList()));


            //Build a product hit for each product
            for (Map.Entry<String, List<String>> entry : entries.entrySet()) {
                String prefix = entry.getKey();
                List<String> keys = entry.getValue();
                Map<String, String> prParamMap = keys
                        .stream()
                        .collect(Collectors.toMap(s -> s, s -> paramMap.get(s)));
                prParamMap.putAll(paramMapExclPr);
                try {
                    if ((null != prParamMap.get("pr" + prefix + "id")) ||
                            (null != prParamMap.get("pr" + prefix + "nm")) ||
                            (null != paramMap.get("ti") && "refund".equals(paramMap.get("pa")))
                    ) {
                        MPEntity evp = builder(prParamMap).build();
                        eventList.add(evp);
                    }
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
