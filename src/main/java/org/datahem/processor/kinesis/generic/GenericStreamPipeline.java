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

import org.datahem.processor.kinesis.generic.Config;
import org.datahem.processor.utils.JsonToTableRowFn;
//import org.datahem.protobuf.kinesis.order.v1.OrderEntityProto.*;
//import org.datahem.protobuf.kinesis.order.v1.OrderEntityProto;
import org.datahem.processor.utils.ProtobufUtils;
import org.datahem.processor.utils.KmsUtils;
import org.datahem.processor.kinesis.generic.JsonToEntityToTableRowFn;

import com.google.protobuf.util.JsonFormat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.List;
import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.io.kinesis.KinesisRecord;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.regions.Regions;

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
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.DateTimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericStreamPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(GenericStreamPipeline.class);
	static final String[] WORDS_ARRAY =
      new String[] {
        "hi there", "hi", "hi sue bob",
        "hi sue", "bob hi"
      };
      static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

	public interface Options extends PipelineOptions, GcpOptions {
		@Description("JSON Configuration string")
		//ValueProvider<String> getConfig();
		//void setConfig(ValueProvider<String> value);
		String getConfig();
		void setConfig(String value);
		
		@Description("Pub/Sub topic: ")
  	//@Default.String("projects/mathem-data/topics/test")
  	ValueProvider<String> getPubsubTopic();
  	void setPubsubTopic(ValueProvider<String> topic);
	}

	public static void main(String[] args) throws IOException {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);

		PCollectionList<String> kinesis = PCollectionList.empty(pipeline);
		
		for (Config.KinesisStream kinesisStream : Config.read(options.getConfig())) {
			LOG.info("Stream name: " + kinesisStream.name);
			LOG.info("Record namespace: " + kinesisStream.recordNamespace);
			LOG.info("Record name: " + kinesisStream.recordName);
			PCollection<String> pass = pipeline.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()));
			kinesis = kinesis.and(pass);
		}
		
		kinesis
			.apply(Flatten.<String>pCollections())
			.apply("Fixed Windows", Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))))
			.apply("Write to pubsub",
				PubsubIO
					.writeStrings()
					.to(options.getPubsubTopic())
		);
		
		//skapa attributes: UUID, timestamp, stream, namespace, record

		pipeline.run();
	}
}
