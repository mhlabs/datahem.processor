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

import org.datahem.protobuf.measurementprotocol.v2.Product;
import org.datahem.protobuf.measurementprotocol.v2.CustomDimension;
import org.datahem.protobuf.measurementprotocol.v2.CustomMetric;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ProductEntity{
	private static final Logger LOG = LoggerFactory.getLogger(ProductEntity.class);
	
	public ProductEntity(){}
	
	
	private boolean trigger(Map<String, String> paramMap){
        //check if payload contains product impression
        Pattern productImpressionIndexPattern = Pattern.compile("^il[0-9]{1,3}pi[0-9]{1,3}.*");
		Map<String, List<String>> entries = paramMap
					.keySet()
        			.stream()
        			.filter(productImpressionIndexPattern.asPredicate())
        			.collect(Collectors.groupingBy(s -> s, Collectors.toList()));
        
        //check if product impression or product action
		return (entries.size() > 0 || Stream.of("detail", "click", "add", "remove", "checkout", "purchase", "refund").collect(Collectors.toList()).contains(paramMap.get("pa")));
	}
	
	public ArrayList<Product> build(Map<String, String> paramMap){
		ArrayList<Product> eventList = new ArrayList<>();
		Map<String, String> impressionMap = new HashMap<>(paramMap);
        if(trigger(paramMap)){
    			
                //START product action
    			Pattern productExclPattern = Pattern.compile("^(?!pr[0-9]{1,3}.*).*$");
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
    			for(Map.Entry<String, List<String>> entry : entries.entrySet()){
		            String prefix = entry.getKey();
		            List<String> keys = entry.getValue();
		            Map<String, String> prParamMap = keys
		            	.stream()
		            	.collect(Collectors.toMap(s -> s, s -> paramMap.get(s)));
		            prParamMap.putAll(paramMapExclPr);
		            try{
		            	if((null != prParamMap.get("pr" + prefix + "id")) || 
		            		(null != prParamMap.get("pr" + prefix + "nm")) || 
		            		(null != paramMap.get("ti") && "refund".equals(paramMap.get("pa")))
		            	){
							Product.Builder builder = Product.newBuilder();
                                Optional.ofNullable(prParamMap.get("pr" + prefix + "id")).ifPresent(builder::setId);
                                Optional.ofNullable(prParamMap.get("pr" + prefix + "nm")).ifPresent(builder::setName);
                                Optional.ofNullable(prParamMap.get("pr" + prefix + "br")).ifPresent(builder::setBrand);
                                Optional.ofNullable(prParamMap.get("pr" + prefix + "va")).ifPresent(builder::setVariant);
                                Optional.ofNullable(prParamMap.get("pr" + prefix + "ca")).ifPresent(builder::setCategory);
                                Optional.ofNullable(prParamMap.get("pa")).ifPresent(builder::setAction);
                                Optional.ofNullable(prParamMap.get("pr" + prefix + "cc")).ifPresent(builder::setCouponCode);
                                Optional.ofNullable(prParamMap.get("pr" + prefix + "cu")).ifPresent(builder::setCurrency);
                                FieldMapper.intVal(prParamMap.get("pr" + prefix + "qt")).ifPresent(g -> builder.setQuantity(g.intValue()));
                                FieldMapper.doubleVal(prParamMap.get("pr" + prefix + "pr")).ifPresent(g -> builder.setPrice(g.doubleValue()));
                                //FieldMapper.doubleVal(prParamMap.get("pr" + prefix + "pr")).ifPresent(g -> builder.setRefundAmount(g.doubleValue()));
                                Optional.ofNullable(prParamMap.get("pal")).ifPresent(builder::setList);
                                FieldMapper.intVal(prParamMap.get("pr" + prefix + "ps")).ifPresent(g -> builder.setPosition(g.intValue()));
	        					Optional.ofNullable(getCustomDimensions(prParamMap)).ifPresent(builder::addAllCustomDimensions);
                                Optional.ofNullable(getCustomMetrics(prParamMap)).ifPresent(builder::addAllCustomMetrics);
                                eventList.add(builder.build());
						}
					}catch(IllegalArgumentException e){
						LOG.error(e.toString());
					}
                //END product action

                //START product impression
                
    			//Group product parameters by list and product index 
    			final Pattern productImpressionIndexPattern = Pattern.compile("^il([0-9]{1,3})pi([0-9]{1,3}).*");
				LOG.info(impressionMap.toString());
                Map<String, List<String>> ilEntries = impressionMap
					.keySet()
        			.stream()
        			.filter(productImpressionIndexPattern.asPredicate())
        			.collect(Collectors.groupingBy(s -> {
        				final Matcher matcher = productImpressionIndexPattern.matcher(s);
        				matcher.find();
        				return matcher.group(1)+matcher.group(2);
        				}, Collectors.toList()));
    			LOG.info(ilEntries.toString());
    			//Build a product hit for each produt
    			for(Map.Entry<String, List<String>> ilEntry : ilEntries.entrySet()){
		            List<String> ilKeys = ilEntry.getValue();
		            Map<String, String> ilParamMap = ilKeys
		            	.stream()
		            	.collect(Collectors.toMap(s -> s, s -> impressionMap.get(s)));
		            try{
                        LOG.info(ilParamMap.toString());
                        Product.Builder builder = Product.newBuilder();
                        //Optional.ofNullable(getFirstParameter(ilParamMap, "il[0-9]{1,3}nm")).ifPresent(builder::setList);
                        Optional.ofNullable(getFirstParameter(ilParamMap, "il[0-9]{1,3}pi[0-9]{1,3}id")).ifPresent(builder::setId);
                        Optional.ofNullable(getFirstParameter(ilParamMap, "il[0-9]{1,3}pi[0-9]{1,3}nm")).ifPresent(builder::setName);
                        Optional.ofNullable(getFirstParameter(ilParamMap, "il[0-9]{1,3}pi[0-9]{1,3}br")).ifPresent(builder::setBrand);
                        Optional.ofNullable(getFirstParameter(ilParamMap, "il[0-9]{1,3}pi[0-9]{1,3}va")).ifPresent(builder::setVariant);
                        Optional.ofNullable(getFirstParameter(ilParamMap, "il[0-9]{1,3}pi[0-9]{1,3}ca")).ifPresent(builder::setCategory);
                        FieldMapper.doubleVal(getFirstParameter(ilParamMap, "il[0-9]{1,3}pi[0-9]{1,3}pr")).ifPresent(g -> builder.setPrice(g.doubleValue()));
                        FieldMapper.intVal(getFirstParameter(ilParamMap, "il[0-9]{1,3}pi[0-9]{1,3}pr")).ifPresent(g -> builder.setPosition(g.intValue()));
                        Optional.ofNullable("impression").ifPresent(builder::setAction);
                        /*
                        Optional.ofNullable(getCustomDimensions(prParamMap)).ifPresent(builder::addAllCustomDimensions);
                        Optional.ofNullable(getCustomMetrics(prParamMap)).ifPresent(builder::addAllCustomMetrics);
                        */
                        eventList.add(builder.build());
					}
					catch(IllegalArgumentException e){
						LOG.error(e.toString());
					}
				}



				}
				return eventList;
		}
		else{
			return null;
		}
	}	

    private ArrayList<CustomDimension> getCustomDimensions(Map<String, String> prParamMap){
			ArrayList<CustomDimension> customDimensions = new ArrayList<>();
            List<String> params = getParameters(prParamMap, "^(pr[0-9]{1,3}cd[0-9]{1,3})$");
            for(String p : params){
                CustomDimension.Builder builder = CustomDimension.newBuilder();
                FieldMapper.intVal(getParameterIndex(p, "^pr[0-9]{1,3}cd([0-9]{1,3})$")).ifPresent(g -> builder.setIndex(g.intValue()));
                Optional.ofNullable(prParamMap.get(p)).ifPresent(builder::setValue);
                customDimensions.add(builder.build());
            }
            return customDimensions;
    }

    private ArrayList<CustomMetric> getCustomMetrics(Map<String, String> prParamMap){
			ArrayList<CustomMetric> customMetrics = new ArrayList<>();
            List<String> params = getParameters(prParamMap, "^(pr[0-9]{1,3}cm[0-9]{1,3})$");
            for(String p : params){
                CustomMetric.Builder builder = CustomMetric.newBuilder();
                FieldMapper.intVal(getParameterIndex(p, "^pr[0-9]{1,3}cm([0-9]{1,3})$")).ifPresent(g -> builder.setIndex(g.intValue()));
                FieldMapper.intVal(prParamMap.get(p)).ifPresent(g -> builder.setValue(g.intValue()));
                customMetrics.add(builder.build());
            }
            return customMetrics;
    }

    private String getFirstParameter(Map<String, String> prParamMap, String parameterPattern){
        Pattern pattern = Pattern.compile(parameterPattern);
 		Optional<String> firstElement = prParamMap
 			.keySet()
 			.stream()
 			.filter(pattern.asPredicate())
			.findFirst();
        return prParamMap.get(firstElement.orElse(null));    
    }

    private List<String> getParameters(Map<String, String> prParamMap, String parameterPattern){
        Pattern pattern = Pattern.compile(parameterPattern);
 		List<String> params = prParamMap
 			.keySet()
 			.stream()
 			.filter(pattern.asPredicate())
			.collect(Collectors.toList());
        return params;    
    }

    private String getParameterIndex(String param, String indexPattern){
		if(null == param){
			return null;
		}
		else{
			Pattern pattern = Pattern.compile(indexPattern);
			Matcher matcher = pattern.matcher(param);
			if(matcher.find() && matcher.group(1) != null){
				return matcher.group(1);
			}
			else {
				return null;
			}
		}
	}
}
