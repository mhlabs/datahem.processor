package org.datahem.processor.measurementprotocol.utils;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 Robert Sahlin and MatHem Sverige AB
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * =========================LICENSE_END==================================
 */

import org.datahem.processor.measurementprotocol.utils.BaseEntity;
import org.datahem.processor.measurementprotocol.utils.Parameter;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PromotionEntity extends BaseEntity{
	private List<Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(PromotionEntity.class);
	
	public PromotionEntity(){
		super();
		parameters = Arrays.asList(
			new Parameter("(promo[0-9]{1,3}id)", "String", null, 500, "promotionId", false, "promo1id", "SHIP"),
			new Parameter("(promo[0-9]{1,3}nm)", "String", null, 500, "promotionName", false, "promo1nm", "Shipping"),
			new Parameter("(promo[0-9]{1,3}cr)", "String", null, 500, "promotionCreative", false, "promo1cr", "Shipping Banner"),
			new Parameter("(promo[0-9]{1,3}ps)", "String", null, 500, "promotionPosition", false, "promo1ps", "banner_slot_1"),
			new Parameter("promoa", "String", "view", 50, "promotionAction", false, "click")
		);
	}
	
	public List<Parameter> getParameters(){return parameters;}
	
	private boolean trigger(Map<String, String> paramMap){
		Pattern promoIndexPattern = Pattern.compile("^promo[0-9]{1,3}.*");
		Map<String, List<String>> entries = paramMap
					.keySet()
        			.stream()
        			.filter(promoIndexPattern.asPredicate())
        			.collect(Collectors.groupingBy(s -> s, Collectors.toList()));
		return (entries.size() > 0);
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
    			paramMap.put("et", "promotion");
    			//create conditions and set parameters for different promotion actions
    			//Get map without product parameters
    			Pattern promoExclPattern = Pattern.compile("^(?!promo[0-9]{1,3}.*).*$");
    			Map<String, String> paramMapExclPromo = paramMap
					.keySet()
        			.stream()
        			.filter(promoExclPattern.asPredicate())
        			.collect(Collectors.toMap(s -> s, s -> paramMap.get(s)));
    			
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
		            		MPEntity evp = builder(prParamMap).build();
							eventList.add(evp);
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
	
	public MPEntity.Builder builder(Map<String, String> paramMap) throws IllegalArgumentException{
		return builder(paramMap, super.builder(paramMap));
	}
	
	public MPEntity.Builder builder(Map<String, String> paramMap, MPEntity.Builder eventBuilder) throws IllegalArgumentException{
		return super.builder(paramMap, eventBuilder, this.parameters);
	}	
}
