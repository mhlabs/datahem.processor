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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.stream.Collectors;

import java.util.List;
import java.io.IOException;

import org.apache.beam.sdk.Pipeline;


import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import java.lang.reflect.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.joda.time.Duration;
import org.apache.beam.sdk.transforms.SerializableFunction;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.datahem.processor.generic.protobuf.utils.ProtobufUtils;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericStreamPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(GenericStreamPipeline.class);

	public interface Options extends PipelineOptions, GcpOptions {
		
		@Description("Protobuf Java package")
		ValueProvider<String> getProtoJavaPackage();
		void setProtoJavaPackage(ValueProvider<String> value);
		
		@Description("Protobuf Java Outer Class Name")
		ValueProvider<String> getProtoJavaOuterClassName();
		void setProtoJavaOuterClassName(ValueProvider<String> value);
		
		@Description("Protobuf Java Class Name")
		ValueProvider<String> getProtoJavaClassName();
		void setProtoJavaClassName(ValueProvider<String> value);
	
		@Description("Pub/Sub topic")
		ValueProvider<String> getPubsubTopic();
		void setPubsubTopic(ValueProvider<String> value);
		
		@Description("Pub/Sub subscription")
		ValueProvider<String> getPubsubSubscription();
		void setPubsubSubscription(ValueProvider<String> subscription);
	}

	public static void main(String[] args) throws IOException {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		
		try{
			//Generic
			String protoJavaFullName = options.getProtoJavaPackage() + "." + options.getProtoJavaOuterClassName() +"$" + options.getProtoJavaClassName();
			
			Class<?> clazz = Class.forName(protoJavaFullName);
			Method getDescriptor = clazz.getMethod("getDescriptor");
			Descriptor descriptor = (Descriptor) getDescriptor.invoke(null);
			TableSchema schema = ProtobufUtils.makeTableSchema(descriptor);
			
		PCollection<Message> incomingMessages =
		pipeline
		.apply("Read pubsub messages", 
			PubsubIO
				.readMessagesWithAttributes()
				.fromSubscription(options.getPubsubSubscription()))
		.apply("Fixed Windows",
			Window.<PubsubMessage>into(FixedWindows.of(Duration.standardMinutes(1)))
				.withAllowedLateness(Duration.standardDays(7))
				.discardingFiredPanes())
		.apply("Convert payload from Json to Protobuf Message", 
			ParDo.of(new JsonToProtobufMessageFn(protoJavaFullName)));
		
		incomingMessages
			.apply("Write to pubsub",
				PubsubIO
					.writeProtos(Message.class)
					.withIdAttribute("uuid")
					.withTimestampAttribute("timestamp")
					.to(options.getPubsubTopic())
		);
		
		incomingMessages
			.apply(
				"Write to BigQuery",
				BigQueryIO.<Message>write()
					.to(new ProtobufBigQueryToFn("backup","payload"))
					.withFormatFunction(new ProtobufBigQueryFormatFn())
					.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
					.withWriteDisposition(WriteDisposition.WRITE_APPEND)
					.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
					.withSchema(schema));		
		}catch(Exception e){}
		pipeline.run();
	}
}