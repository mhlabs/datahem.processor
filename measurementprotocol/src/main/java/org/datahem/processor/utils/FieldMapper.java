package org.datahem.processor.utils;

/*-
 * #%L
 * DataHem
 * %%
 * Copyright (C) 2018 - 2019 MatHem Sverige AB
 * %%
 * DataHem is a serverless real-time data platform built for reporting, analytics and data/ML-products.
 * Copyright (C) 2019 Robert Sahlin
 * 
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * #L%
 */

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 Robert Sahlin and MatHem Sverige AB
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */


import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldMapper{
	
	private static final Logger LOG = LoggerFactory.getLogger(FieldMapper.class);
	  
    public static Map<String, String> fieldMapFromURL(URL url){ 
    	try{
	    	return   
	    		Pattern.compile("&")
	    		.splitAsStream(url.getQuery())
	        	.map(s -> Arrays.copyOf(s.split("="), 2))
	        	.collect(Collectors.toMap(s -> decode(s[0]), s -> decode(s[1])));
        }catch(NullPointerException e) {
    		return null;
		}
    }

    public static Map<String, String> fieldMapFromQuery(String query){ 
    	return   
     		Pattern
     			.compile("&")
     			.splitAsStream(query)
        		.map(s -> Arrays.copyOf(s.split("="), 2))
        		.collect(HashMap::new, (m,v)->m.put(decode(v[0]), decode(v[1])), HashMap::putAll);
    }
    
    public static String decode(final String encoded) {
    	try {
        	return encoded == null ? "(not set)" : URLDecoder.decode(encoded, "UTF-8");
    	} catch(final UnsupportedEncodingException e) {
        	throw new RuntimeException("Impossible: UTF-8 is a required encoding", e);
    	}
	}
	
	public static String encode(Object decoded) {
    	try {
        	return decoded == null ? "" : URLEncoder.encode(String.valueOf(decoded), "UTF-8").replace("+", "%20");
    	} catch(final UnsupportedEncodingException e) {
        	throw new RuntimeException("Impossible: UTF-8 is a required encoding", e);
    	}
	}
}
