package org.datahem.processor.measurementprotocol.v2;

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
