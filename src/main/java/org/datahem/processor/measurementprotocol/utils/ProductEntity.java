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


public class ProductEntity extends BaseEntity{
	private List<Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(ProductEntity.class);
	
	public ProductEntity(){
		super();
		parameters = Arrays.asList(
			new Parameter("(pr[0-9]{1,3}id)", "String", null, 500, "productSku", false, "pr1id", "P12345"),
			new Parameter("(pr[0-9]{1,3}nm)", "String", null, 500, "productName", false, "pr1nm", "Android T-Shirt"),
			new Parameter("(pr[0-9]{1,3}br)", "String", null, 500, "productBrand", false, "pr1br", "Google"),
			new Parameter("(pr[0-9]{1,3}ca)", "String", null, 500, "productCategory", false, "pr1ca", "Apparel/Mens/T-Shirts"),
			new Parameter("(pr[0-9]{1,3}va)", "String", null, 500, "productVariant", false, "pr1va", "Black"),
			new Parameter("(pr[0-9]{1,3}pr)", "Double", null, 500, "productPrice", false, "pr1pr", 29.20),
			new Parameter("(pr[0-9]{1,3}qt)", "Integer", null, 500, "productQuantity", false, "pr1qt", 2),
			new Parameter("(pr[0-9]{1,3}cc)", "String", null, 500, "productCouponCode", false, "pr1cc", "SUMMER_SALE13"),
			new Parameter("(pr[0-9]{1,3}ps)", "Integer", null, 500, "productPosition", false, "pr1ps", 2),
			new Parameter("(pr[0-9]{1,3}cd[0-9]{1,3})", "String", null, 500, "productCustomDimension", false, "pr[0-9]{1,3}cd([0-9]{1,3})", "pr1cd1", "Member", "productCustomDimension1"),
			new Parameter("(pr[0-9]{1,3}cm[0-9]{1,3})", "Integer", null, 500, "productCustomMetric", false, "pr[0-9]{1,3}cm([0-9]{1,3})", "pr1cm1", 28, "productCustomMetric1"),
			new Parameter("pa", "String", null, 50, "productAction", true, "detail"),
			new Parameter("pal", "String", null, 500, "productActionList", false, "Search Results"), //If pa == detail || click
			new Parameter("ti", "String", null, 50, "transactionId", false, "T1234"), //If pa == purchase
			new Parameter("cos", "Integer", null, 50, "checkoutStep", false, 2), //If pa == checkout
			new Parameter("col", "String", null, 50, "checkoutStepOption", false, "Visa") //If pa == checkout
		);
	}
	
	public List<Parameter> getParameters(){return parameters;}
	
	private boolean trigger(Map<String, String> paramMap){
		return Stream.of("detail", "click", "add", "remove", "checkout", "purchase", "refund").collect(Collectors.toList()).contains(paramMap.get("pa"));
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
    			paramMap.put("et", "product");
    			
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
    			
    			
    			//Build a product hit for each produt
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
		            		MPEntity evp = builder(prParamMap).build();
							eventList.add(evp);
						}

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
