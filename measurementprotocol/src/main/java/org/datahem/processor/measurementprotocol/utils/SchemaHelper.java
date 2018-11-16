package org.datahem.processor.measurementprotocol.utils;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto;
import org.datahem.processor.utils.ProtobufUtils;

import java.util.List;

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

public class SchemaHelper {
	
	private static final Logger LOG = LoggerFactory.getLogger(SchemaHelper.class);
	
	// Create schema from Event protocol buffer, replace type of timestamp field from STRING to TIMESTAMP to match BigQuery schema
	public static TableSchema mpEntityBigQuerySchema(){
		TableSchema mpEntitySchema = ProtobufUtils.makeTableSchema(MPEntityProto.MPEntity.getDescriptor());
    	List<TableFieldSchema> fieldsList = mpEntitySchema.getFields();
    	TableFieldSchema tfs = new TableFieldSchema().setName("utc_timestamp").setType("STRING").setMode("NULLABLE");
    	fieldsList.set(fieldsList.indexOf(tfs), tfs.setType("TIMESTAMP"));
    	TableSchema schema = new TableSchema().setFields(fieldsList);
    	return schema;
	}
}