package org.datahem.processor.utils;

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



import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;
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
            LOG.error(e.toString());
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
            //LOG.error(e.toString());
        	throw new RuntimeException("Impossible: UTF-8 is a required encoding", e);
    	}
	}
	
	public static String encode(Object decoded) {
    	try {
        	return decoded == null ? "" : URLEncoder.encode(String.valueOf(decoded), "UTF-8").replace("+", "%20");
    	} catch(final UnsupportedEncodingException e) {
            //LOG.error(e.toString());
        	throw new RuntimeException("Impossible: UTF-8 is a required encoding", e);
    	}
	}
    
    public static Optional<String> stringVal(String f){
        String field = Optional.ofNullable(f).orElse("");
        try{
            String s = new String(field);
            return Optional.of(s);
        }
        catch(NumberFormatException e){
            //LOG.error("FieldMapper.stringVal: " + e.toString());
            return Optional.empty();
        }
    }

    public static Optional<Boolean> booleanVal(String f){
        String field = Optional.ofNullable(f).orElse("");
        try{
            Boolean b = new Boolean(field);
            return Optional.of(b);
        }
        catch(NumberFormatException e){
            //LOG.error("FieldMapper.booleanVal: " + e.toString());
            return Optional.empty();
        }
    }

    public static Optional<Integer> intVal(String f){
        String field = Optional.ofNullable(f).orElse("");
        try{
            Integer i = new Integer(field);
            return Optional.of(i);
        }
        catch(NumberFormatException e){
            //LOG.error("FieldMapper.intVal: " + e.toString());
            return Optional.empty();
        }
    }

    public static Optional<Double> doubleVal(String f){
        String field = Optional.ofNullable(f).orElse("");
        try{
            Double d = new Double(field);
            return Optional.of(d);
        }
        catch(NumberFormatException e){
            //LOG.error("FieldMapper.doubleVal: " + e.toString());
            return Optional.empty();
        }
    }

    public static Optional<Long> longVal(String f){
        String field = Optional.ofNullable(f).orElse("");
        try{
            Long l = new Long(field);
            return Optional.of(l);
        }
        catch(NumberFormatException e){
            //LOG.error("FieldMapper.longVal: " + e.toString());
            return Optional.empty();
        }
    }
    
    public static Optional<Float> floatVal(String fl){
        String field = Optional.ofNullable(fl).orElse("");
        try{
            Float f = new Float(field);
            return Optional.of(f);
        }
        catch(NumberFormatException e){
            LOG.error("FieldMapper.floatVal: " + e.toString());
            return Optional.empty();
        }
    }
}
