package org.datahem.processor.kinesis.collector;

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

import org.datahem.processor.utils.JsonToTableRowFn;
import org.datahem.processor.utils.ProtobufUtils;
import org.datahem.processor.utils.KmsUtils;

import com.google.protobuf.util.JsonFormat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;

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

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.DateTimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamCollectorPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(StreamCollectorPipeline.class);
	private static Pattern pattern = Pattern.compile("^(\\d{4})-(\\d{2})-(\\d{2})");
    private static Matcher matcher;
    
    public interface Options extends PipelineOptions, GcpOptions {

	@Description("AWS KEY")
    String getAwsKey();
    void setAwsKey(String value);
    
    @Description("AWS SECRET")
    String getAwsSecret();
    void setAwsSecret(String value);
    
    @Description("AWS REGION")
    @Default.String("eu-west-1")
    String getAwsRegion();
    void setAwsRegion(String value);
  	
  	@Description("AWS STREAM")
    String getAwsStream();
    void setAwsStream(String value);
  	
	@Description("GCP KMS project ID")
  	@Default.String("")
  	String getKmsProjectId();
  	void setKmsProjectId(String value);
  	
  	@Description("GCP KMS Location ID")
  	@Default.String("global")
  	String getKmsLocationId();
  	void setKmsLocationId(String value);
  	
  	@Description("GCP KMS Key Ring ID")
  	@Default.String("")
  	String getKmsKeyRingId();
  	void setKmsKeyRingId(String value);
  	
  	@Description("GCP KMS Crypto Key ID")
  	@Default.String("")
  	String getKmsCryptoKeyId();
  	void setKmsCryptoKeyId(String value);
    
    @Description("Pub/Sub topic")
  	ValueProvider<String> getPubsubTopic();
  	void setPubsubTopic(ValueProvider<String> value);
  
  	/*
  	LATEST: Start after the most recent data record (fetch new data).
    TRIM_HORIZON: Start from the oldest available data record.
    AT_TIMESTAMP: Start from the record at or after the specified server-side timestamp.
    */ 
  	@Description("Initial Position In AWS Kinesis Stream, i.e. LATEST | TRIM_HORIZON | AT_TIMESTAMP")
  	@Default.String("LATEST")
  	String getInitialPositionInStream();
  	void setInitialPositionInStream(String value);
  
  }
  

	public static void main(String[] args) throws IOException {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);

		pipeline
			.apply(KinesisIO.read()
				.withStreamName(options.getAwsStream())
				.withInitialPositionInStream(InitialPositionInStream.valueOf(options.getInitialPositionInStream()))
				//Encrypt AWS Key and Secret with GCP KMS to keep credentials safe, you can use GCP API explorer for that https://developers.google.com/apis-explorer/?hl=en_US#p/cloudkms/v1/cloudkms.projects.locations.keyRings.cryptoKeys.encrypt
				.withAWSClientsProvider(
					KmsUtils.decrypt(options.getKmsProjectId(), options.getKmsLocationId(), options.getKmsKeyRingId(), options.getKmsCryptoKeyId(), options.getAwsKey()), 
					KmsUtils.decrypt(options.getKmsProjectId(),options.getKmsLocationId(),options.getKmsKeyRingId(),options.getKmsCryptoKeyId(),options.getAwsSecret()),
					Regions.fromName(options.getAwsRegion())))
			.apply("KinesisRecordToString", ParDo.of(new DoFn<KinesisRecord, String>() {
				@ProcessElement
				public void processElement(ProcessContext c) throws Exception {
					KinesisRecord kr = c.element();
					byte[] bytes = kr.getDataAsBytes();
					String s = new String(bytes, "UTF-8");
					c.output(s);
				}}))
			.apply("Write to pubsub",
				PubsubIO
					.writeStrings()
					.to(options.getPubsubTopic()));

		pipeline.run();
	}
}
