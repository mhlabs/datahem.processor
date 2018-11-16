package org.datahem.processor.kinesis.generic;

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


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.options.ValueProvider;
//import org.datahem.protobuf.kinesis.order.v1.OrderEntityProto.*;
//import org.datahem.protobuf.kinesis.order.v1.OrderEntityProto;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.Instant;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.InvalidProtocolBufferException;
//import java.util.List;
import java.lang.reflect.*;
import com.google.api.services.bigquery.model.TableRow;
import org.datahem.processor.utils.ProtobufUtils;

public class JsonToEntityToTableRowFn extends DoFn<String, TableRow> {
		private static final Logger LOG = LoggerFactory.getLogger(JsonToEntityToTableRowFn.class);
		private static Pattern pattern;
    	private static Matcher matcher;
    	
		ValueProvider<String> bigQueryPartitionTimeZone;
		ValueProvider<String> bigQueryPartitionDatePattern;
		
	  	public JsonToEntityToTableRowFn(
	  		ValueProvider<String> bigQueryPartitionTimeZone,
	  		ValueProvider<String> bigQueryPartitionDatePattern) {
		     	this.bigQueryPartitionTimeZone = bigQueryPartitionTimeZone;
		     	this.bigQueryPartitionDatePattern = bigQueryPartitionDatePattern;
	   	}
		
		@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
	        	String json = c.element();
				//OrderEntity.Builder builder = OrderEntity.newBuilder();
				/*
				try{
					Class cl = Class.forName("org.datahem.protobuf.kinesis.order.v1.OrderEntityProto$OrderEntity");
					Class builder = Class.forName("org.datahem.protobuf.kinesis.order.v1.OrderEntityProto$OrderEntityOrBuilder");
					Method m[] = cl.getDeclaredMethods();
					LOG.info("declared methods: " + m[0].toString());
					
					//Method method = cl.getMethod("newBuilder");
					//Builder builder = method.invoke(cl);
					

					JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
					pattern = Pattern.compile(bigQueryPartitionDatePattern.get());
					matcher = pattern.matcher(builder.getLastModifiedDate());
					if(matcher.find()){
						builder.setDate(matcher.group(0));
						//LOG.info("LastModifiedDate: " + matcher.group(0));
					}
					else {
						Instant payloadTimeStamp = c.timestamp();
						DateTimeFormatter partition = DateTimeFormat.forPattern("YYYY-MM-dd").withZone(DateTimeZone.forID(bigQueryPartitionTimeZone.get()));
						builder.setDate(payloadTimeStamp.toString(partition)); 
						//LOG.info("element time: " + payloadTimeStamp.toString(partition));
					}
					OrderEntity orderEntity = builder.build();
					TableRow tr = ProtobufUtils.makeTableRow(orderEntity);
					//c.output(orderEntity);
					c.output(tr);
				}catch(InvalidProtocolBufferException e){
					LOG.error(e.toString());
					LOG.error(json);
				}*/
    		}
  }
