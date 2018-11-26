package org.datahem.processor.generic.protobuf.load;

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
import java.util.stream.Collectors;

import java.util.List;
import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SerializableFunction;

import org.datahem.processor.generic.protobuf.utils.ProtobufUtils;
import com.google.protobuf.util.JsonFormat;

import com.google.auth.oauth2.GoogleCredentials;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
//import com.google.cloud.bigquery.Schema;

import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
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
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
/*
import org.datahem.avro.message.Converters;
import org.datahem.avro.message.DatastoreCache;
import org.datahem.avro.message.DynamicBinaryMessageDecoder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData;
import org.apache.avro.SchemaNormalization;
*/
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.Coder;

import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import java.lang.reflect.*;

import java.util.HashMap;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.DateTimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadStreamPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(LoadStreamPipeline.class);
	//private static Map tableFingerprintLabel = new HashMap<String, String>();

	public interface Options extends PipelineOptions, GcpOptions {
		
		@Description("Pub/Sub subscription")
		//@Default.String("projects/mathem-data/subscriptions/measurementprotocol-1-dev")
		ValueProvider<String> getPubsubSubscription();
		void setPubsubSubscription(ValueProvider<String> subscription);
	}

	public static void main(String[] args) throws IOException {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
//		CoderRegistry cr = pipeline.getCoderRegistry();
//		GenericRecordCoder coder = new GenericRecordCoder();
//		cr.registerCoderForClass(Record.class, coder);
//		DatastoreCache cache = new DatastoreCache();
		//Map tableFingerprintLabel = new HashMap<String, String>();

		PCollection<PubsubMessage> incomingRecords = 
			pipeline
				.apply("Read pubsub messages", 
					PubsubIO
						.readMessagesWithAttributes()
						.fromSubscription(options.getPubsubSubscription()))
				.apply("1mWindow", Window.into(FixedWindows.of(Duration.standardMinutes(1L))));
			
			WriteResult writeResult = 
				incomingRecords.apply(
					"Wite to dynamic BigQuery destinations", 
					BigQueryIO.<PubsubMessage>write()
					.to(new DynamicDestinations<PubsubMessage, PubsubMessage>() {
						public PubsubMessage getDestination(ValueInSingleWindow<PubsubMessage> element) {
							LOG.info("record: " + element.getValue().getAttributeMap().toString());
							return element.getValue();
						}
						public TableDestination getTable(PubsubMessage psm) {
							String project = "mathem-ml-datahem-test";
							String dataset = "generic_streams";
							String table = "prototest";
							return new TableDestination(dataset + "." + table, "Table for:" + table);
						}
						public TableSchema getSchema(PubsubMessage received) {
							//String protobufClassName = received.getAttribute("protobufClassName");
							String protobufClassName = received.getAttribute("proto");
							try{
								// Use reflection to create and serialize protobuf message
								Class<?> clazz = Class.forName(protobufClassName);
								Method getDescriptor = clazz.getMethod("getDescriptor");
								Descriptor descriptor = (Descriptor) getDescriptor.invoke(null);
								return ProtobufUtils.makeTableSchema(descriptor);
								//TableSchema schema = ProtobufUtils.makeTableSchema(descriptor);
								//return schema;	
							}catch(Exception e){
								LOG.error(e.toString());
							}
							return null;
						}
					})
					.withFormatFunction(new SerializableFunction<PubsubMessage, TableRow>() {
						public TableRow apply(PubsubMessage received) {
							String protobufClassName = received.getAttribute("proto");
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
						})
					.withWriteDisposition(WriteDisposition.WRITE_APPEND)
					.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));
					
			pipeline.run();
			
			/*
			// Create side-input to join records with their incoming schema
			PCollectionView<Map<Integer, Record>> incomingRecordsView =
				incomingRecords
					//.apply("KeyIncomingByHash", WithKeys.of(record -> new Integer(AvroToBigQuery.getTableRow(record).hashCode())))
					.apply("KeyIncomingByHash", WithKeys.of(new SerializableFunction<Record, Integer>() {
         				public Integer apply(Record record) {
         						return new Integer(AvroToBigQuery.getTableRow(record).hashCode()); 
         					}}))
					.apply("CreateView", View.asMap());
			
			writeResult
				.getFailedInserts()
				//.apply("MutateSchema", BigQuerySchemaMutator.mutateWithSchema(incomingRecordsView))
				.apply("Mutate schema", 
					ParDo.of(new DoFn<TableRow,Record>() {
						//private DatastoreCache cache;
						private DynamicBinaryMessageDecoder<Record> decoder;
						private transient BigQuery bigQuery;
						
						@Setup
						public void setup() throws Exception {
							//cache = new DatastoreCache();
							String SCHEMA_STR_V1 = "{\"type\":\"record\", \"namespace\":\"foo\", \"name\":\"Man\", \"fields\":[ { \"name\":\"name\", \"type\":\"string\" }, { \"name\":\"age\", \"type\":[\"null\",\"double\"] } ] }";
							Schema SCHEMA_V1 = new Schema.Parser().parse(SCHEMA_STR_V1);
							decoder = new DynamicBinaryMessageDecoder<>(GenericData.get(), SCHEMA_V1, cache);
							bigQuery =
								BigQueryOptions.newBuilder()
									.setCredentials(GoogleCredentials.getApplicationDefault())
									.build()
									.getService();
						}
						
						@ProcessElement
						public void processElement(ProcessContext c) {
							//first part, get record from tablerow-lookup
							TableRow tablerow = c.element();
							Map<Integer, Record> irv = c.sideInput(incomingRecordsView);
							Record record = irv.get(tablerow.hashCode());
							
							//second part, group records by fingerprint
							
							//third part, modify schema for group of records
							
							//fourth part, transform records to tablerows and write to bq
							
							Schema writer = record.getSchema();
							//Schema writer = cache.findByFingerprint(Long.parseLong(fingerprint));
							String project = "mathem-ml-datahem-test";
							String dataset = "generic_streams";
							
							String table = writer.getName();
							TableId tableId = TableId.of(project, dataset, table);
							String fingerprintLabel = (String) tableFingerprintLabel.get(tableId.toString());
							Schema reader = cache.findByFingerprint(Long.parseLong(fingerprintLabel));
							SchemaPairCompatibility compatibility = SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
							if(compatibility.getType() == SchemaCompatibilityType.COMPATIBLE){
								LOG.info("Compatible schemas!");
								Table bqTable = bigQuery.getTable(tableId);
								TableDefinition bqTableDef = bqTable.getDefinition();
								bqTable
									.toBuilder()
									.setDefinition(bqTableDef.toBuilder().setSchema(AvroToBigQuery2.getTableSchemaRecord(writer)).build())
									.build()
									.update();
							}
							try{
								c.output(record);
								//c.output(decoder.decode(received.getPayload()));	
							}catch(Exception e){
								LOG.error(e.toString());
							}
						}
					})
					.withSideInputs(incomingRecordsView))
				.apply(
					"RetryWriteMutatedRows",
					BigQueryIO.<Record>write()
					.to(new DynamicDestinations<Record, String>() {
						public String getDestination(ValueInSingleWindow<Record> element) {
							String fingerprint = Long.toString(SchemaNormalization.parsingFingerprint64(element.getValue().getSchema()));
							return fingerprint;
						}
						public TableDestination getTable(String fingerprint) {
							Schema schema = cache.findByFingerprint(Long.parseLong(fingerprint));
							String project = "mathem-ml-datahem-test";
							String dataset = "generic_streams";
							String table = schema.getName();
							labelTableWithFingerprint(fingerprint, project, dataset, table);
							return new TableDestination(dataset + "." + table, "Table for:" + fingerprint);
						}
						public TableSchema getSchema(String fingerprint) {
							String SCHEMA_STR_V1 = "{\"type\":\"record\", \"namespace\":\"foo\", \"name\":\"Man\", \"fields\":[ { \"name\":\"name\", \"type\":\"string\" }, { \"name\":\"age\", \"type\":[\"null\",\"double\"] } ] }";
							Schema SCHEMA_V1 = new Schema.Parser().parse(SCHEMA_STR_V1);
							return AvroToBigQuery.getTableSchemaRecord(SCHEMA_V1);
						}
					})
					.withFormatFunction(new SerializableFunction<Record, TableRow>() {
						public TableRow apply(Record record) {
							return AvroToBigQuery.getTableRow(record);
						}
					})
					.withCreateDisposition(CreateDisposition.CREATE_NEVER)
					.withWriteDisposition(WriteDisposition.WRITE_APPEND));
						

		pipeline.run();*/
	}
}
