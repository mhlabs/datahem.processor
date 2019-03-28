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

import org.datahem.protobuf.measurementprotocol.v2.Transaction;

import java.util.Map;
import java.util.Optional;
import org.datahem.processor.utils.FieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionEntity{
	private static final Logger LOG = LoggerFactory.getLogger(TransactionEntity.class);

	public TransactionEntity(){}
	
	private boolean trigger(Map<String, String> paramMap){
		return (null != paramMap.get("ti") && "purchase".equals(paramMap.get("pa")));
	}
	
	public Transaction build(Map<String, String> pm){
		if(trigger(pm)){
    		try{
                Transaction.Builder builder = Transaction.newBuilder();
                Optional.ofNullable(pm.get("ti")).ifPresent(builder::setId);
                FieldMapper.doubleVal(pm.get("tr")).ifPresent(g -> builder.setRevenue(g.doubleValue()));
                FieldMapper.doubleVal(pm.get("tt")).ifPresent(g -> builder.setTax(g.doubleValue()));
                FieldMapper.doubleVal(pm.get("ts")).ifPresent(g -> builder.setShipping(g.doubleValue()));
                Optional.ofNullable(pm.get("ta")).ifPresent(builder::setAffiliation);
                Optional.ofNullable(pm.get("cu")).ifPresent(builder::setCurrency);
                Optional.ofNullable(pm.get("tcc")).ifPresent(builder::setCoupon);
                return builder.build();
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
}