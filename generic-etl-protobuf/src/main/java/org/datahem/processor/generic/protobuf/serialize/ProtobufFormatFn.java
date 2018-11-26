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
import com.google.api.services.bigquery.model.TableRow;
import org.datahem.processor.generic.protobuf.utils.ProtobufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import com.google.protobuf.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import java.lang.reflect.*;


public class ProtobufFormatFn implements SerializableFunction<PubsubMessage, TableRow> {
		private static final Logger LOG = LoggerFactory.getLogger(ProtobufFormatFn.class);
		private static String protobufClassName;
	
	public ProtobufFormatFn(String protobufClassName) {
			this.protobufClassName = protobufClassName;
		}
	
	public TableRow apply(PubsubMessage received) {
		//String protobufClassName = received.getAttribute("protoJavaFullName");
		try{
		// Use reflection to deserialize bytes to protobuf message
			Class<?> clazz = Class.forName(protobufClassName);
			Method parseFromMethod = clazz.getMethod("parseFrom", byte[].class);
			Message message = (Message) parseFromMethod.invoke(null, received.getPayload());
			return ProtobufUtils.makeTableRow(message);
		}catch(Exception e){
			LOG.error(e.toString());
		}
		return null;
	}
}