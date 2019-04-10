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


import org.datahem.protobuf.measurementprotocol.v2.Promotion;

import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.Objects;


public class PromotionEntity{

	private static final Logger LOG = LoggerFactory.getLogger(PromotionEntity.class);
	
	public PromotionEntity(){}
	
	private boolean trigger(Map<String, String> paramMap){
       final Pattern promoIndexPattern = Pattern.compile("^promo([0-9]{1,3}).*");
				Map<String, List<String>> entries = paramMap
					.keySet()
        			.stream()
        			.filter(promoIndexPattern.asPredicate())
        			.collect(Collectors.groupingBy(s -> {
        				final Matcher matcher = promoIndexPattern.matcher(s);
        				matcher.find();
        				return matcher.group(1);
        				}, Collectors.toList()));
		return (entries.size() > 0);
	}
	
	public List<Promotion> build(Map<String, String> paramMap){
		List<Promotion> eventList = new ArrayList<>();
		if(trigger(paramMap)){
    			//create conditions and set parameters for different promotion actions
    			//Get map without product parameters
    			Pattern promoExclPattern = Pattern.compile("^(?!promo[0-9]{1,3}.*).*$");
    			HashMap<String, String> paramMapExclPromo = paramMap
					.keySet()
        			.stream()
                    //.filter(str -> str != null && str.length() > 0)
        			.filter(promoExclPattern.asPredicate())
                    .collect(HashMap::new, (m,v)->m.put(v, paramMap.get(v)), HashMap::putAll);
        			//.collect(Collectors.toMap(s -> s, s -> paramMap.getOrDefault(s,"(not set)")));
    			
    			//Group promo parameters by promo index 
    			final Pattern promoIndexPattern = Pattern.compile("^promo([0-9]{1,3}).*");
				Map<String, List<String>> entries = paramMap
					.keySet()
        			.stream()
        			.filter(promoIndexPattern.asPredicate())
        			.collect(Collectors.groupingBy(s -> {
        				final Matcher matcher = promoIndexPattern.matcher(s);
        				matcher.find();
        				return matcher.group(1);
        				}, Collectors.toList()));

    			//Build a promotion hit for each promotion
    			for(Map.Entry<String, List<String>> entry : entries.entrySet()){
		            List<String> keys = entry.getValue();
		            Map<String, String> prParamMap = keys
		            	.stream()
		            	.collect(Collectors.toMap(s -> s, s -> paramMap.get(s)));
		            prParamMap.putAll(paramMapExclPromo);
		            try{
                        Promotion.Builder builder = Promotion.newBuilder();
                        Optional.ofNullable(FieldMapper.getFirstParameterValue(paramMap, "promo[0-9]{1,3}id")).ifPresent(builder::setId);
                        Optional.ofNullable(FieldMapper.getFirstParameterValue(paramMap, "promo[0-9]{1,3}nm")).ifPresent(builder::setName);
                        Optional.ofNullable(paramMap.get("promoa")).ifPresent(builder::setAction);
                        Optional.ofNullable(FieldMapper.getFirstParameterValue(paramMap, "promo[0-9]{1,3}cr")).ifPresent(builder::setCreative);
                        Optional.ofNullable(FieldMapper.getFirstParameterValue(paramMap, "promo[0-9]{1,3}ps")).ifPresent(builder::setPosition);
                        eventList.add(builder.build());
					}
					catch(IllegalArgumentException e){
						LOG.error(e.toString());
					}
				}
				return eventList;
		}
		else{
			return null;
		}
	}
}