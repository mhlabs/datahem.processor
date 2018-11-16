package org.datahem.processor.utils;

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
