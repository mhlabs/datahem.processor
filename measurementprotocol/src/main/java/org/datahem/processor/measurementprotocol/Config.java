package org.datahem.processor.measurementprotocol;

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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.stream.JsonReader;
import java.io.StringReader;

public class Config {
	private static final Logger LOG = LoggerFactory.getLogger(Config.class);
	
	static public class Account{
		public String name;
		public List<Property> properties;
		
		static public class Property {
			public String id;
			public List<View> views;
			
			static public class View{
				public String id;
				public String searchEnginesPattern;
				public String ignoredReferersPattern; 
				public String socialNetworksPattern;
				public String includedHostnamesPattern;
				public String excludedBotsPattern;
				public String siteSearchPattern;
				public String timeZone;
                public String pubSubTopic;
			}
		}
	}

	public static List<Config.Account.Property> read(String config) {
		LOG.info("config:" + config);
		Gson gson = new Gson();
		JsonReader reader = new JsonReader(new StringReader(config));
		reader.setLenient(true);
		try {
			Account account = gson.fromJson(reader, Account.class);
			return account.properties;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}