package org.datahem.processor.generic.protobuf.serialize;

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

import org.apache.beam.sdk.transforms.SerializableFunction;
//import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
//import org.datahem.processor.generic.protobuf.utils.ProtobufUtils;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import com.google.protobuf.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import java.lang.reflect.*;
import com.google.protobuf.Descriptors;


public class ProtobufBigQueryToFn implements SerializableFunction<ValueInSingleWindow<Message>, TableDestination> {
	private static final Logger LOG = LoggerFactory.getLogger(ProtobufBigQueryToFn.class);
	private String tableField;
	
	public ProtobufBigQueryToFn(String tableField) {
		this.tableField = tableField;
	}
	
	
	public TableDestination apply(ValueInSingleWindow<Message> element) {
		Message message = element.getValue();
		Descriptors.FieldDescriptor field = message.getDescriptorForType().findFieldByName(tableField);
		String table = String.valueOf(message.getField(field));
		String project = "mathem-ml-datahem-test";
		String dataset = "generic_streams";
		//String table = "prototest2";
		return new TableDestination(dataset + "." + table, "Table for:" + table);
	}
}