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



import org.datahem.processor.measurementprotocol.v1.utils.BaseEntity;
import org.datahem.processor.measurementprotocol.v1.utils.Parameter;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransactionEntity extends BaseEntity{
	private List<Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(TransactionEntity.class);

	public TransactionEntity(){
		super();
		parameters = Arrays.asList(
			new Parameter("ti", "String", null, 50, "transaction_id", true, "OD564"),
			new Parameter("ta", "String", null, 500, "transaction_affiliation", false, "Member"),
			new Parameter("tr", "Double", null, 500, "transaction_revenue", false, 15.47),
			new Parameter("tt", "Double", null, 500, "transaction_tax", false, 11.20),
			new Parameter("ts", "Double", null, 500, "transaction_shipping", false, 3.50),
			new Parameter("tcc", "String", null, 500, "transaction_coupon_code", false, "SUMMER08"),
            new Parameter("cu", "String", null, 10, "transaction_currency", false,"SEK")
		);
	}
	
	public List<Parameter> getParameters(){return parameters;}
	
	private boolean trigger(Map<String, String> paramMap){
		return (null != paramMap.get("ti") && "purchase".equals(paramMap.get("pa")));
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
			paramMap.put("et", "ecommerce_" + paramMap.get("pa"));
    		try{
				eventList.add(builder(paramMap).build());
				return eventList;
			}
			catch(IllegalArgumentException e){
				LOG.error(e.toString());
				return null;
			}
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
