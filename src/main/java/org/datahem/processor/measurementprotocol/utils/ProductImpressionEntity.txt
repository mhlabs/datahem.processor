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
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProductImpressionEntity extends BaseEntity{
	private Map<String, Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(ProductImpressionEntity.class);
	
	public ProductImpressionEntity(){
		super();
		parameters = new HashMap<String, Parameter>();
		parameters.put("PRODUCT_IMPRESSION_LIST_NAME", new Parameter("(il[0-9]{1,3}nm)", "String", null, 500, "productListName", false));
		parameters.put("PRODUCT_IMPRESSION_SKU", new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}id)", "String", null, 500, "productSku", false));
		parameters.put("PRODUCT_IMPRESSION_NAME", new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}nm)", "String", null, 500, "productName", false));
		parameters.put("PRODUCT_IMPRESSION_BRAND", new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}br)", "String", null, 500, "productBrand", false));
		parameters.put("PRODUCT_IMPRESSION_CATEGORY", new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}ca)", "String", null, 500, "productCategory", false));
		parameters.put("PRODUCT_IMPRESSION_VARIANT", new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}va)", "String", null, 500, "productVariant", false));
		parameters.put("PRODUCT_IMPRESSION_PRICE", new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}pr)", "Double", null, 500, "productPrice", false));
		parameters.put("PRODUCT_IMPRESSION_POSITION", new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}ps)", "Integer", null, 500, "productPosition", false));
		parameters.put("PRODUCT_IMPRESSION_CUSTOM_DIMENSION", new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}cd[0-9]{1,3})", "String", null, 500, "productCustomDimension", false));
		parameters.put("PRODUCT_IMPRESSION_CUSTOM_METRIC", new Parameter("(il[0-9]{1,3}pi[0-9]{1,3}cm[0-9]{1,3})", "String", null, 500, "productCustomMetric", false));
	}
	
	private boolean trigger(Map<String, String> paramMap){
		Pattern productImpressionIndexPattern = Pattern.compile("^il[0-9]{1,3}pi[0-9]{1,3}.*");
		Map<String, List<String>> entries = paramMap
					.keySet()
        			.stream()
        			.filter(productImpressionIndexPattern.asPredicate())
        			.collect(Collectors.groupingBy(s -> s, Collectors.toList()));
		return (entries.size() > 0);
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
    			paramMap.put("ht", "productImpression");
    			
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
        				return matcher.group(1)+matcher.group(2);
        				}, Collectors.toList()));
    			
    			
    			//Build a product hit for each produt
    			for(Map.Entry<String, List<String>> entry : entries.entrySet()){
		            List<String> keys = entry.getValue();
		            Map<String, String> prParamMap = keys
		            	.stream()
		            	.collect(Collectors.toMap(s -> s, s -> paramMap.get(s)));
		            prParamMap.putAll(paramMapExclPr);
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
