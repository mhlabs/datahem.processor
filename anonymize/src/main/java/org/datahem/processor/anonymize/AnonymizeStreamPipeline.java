package org.datahem.processor.anonymize;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 - 2019 MatHem Sverige AB
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */

import org.datahem.processor.utils.ProtobufUtils;
import org.datahem.processor.utils.TablePartition;
import org.datahem.processor.utils.Failure;
import io.anemos.metastore.core.proto.*;


import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.ByteString;
import com.google.common.collect.HashMultimap;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.UUID;
import java.util.Base64;
//import java.util.regex.Pattern;
//import java.util.regex.Matcher;
//import com.google.api.services.bigquery.model.TableRow;
//import com.google.api.services.bigquery.model.TableSchema;

import java.util.List;
import java.util.HashMap;
import java.util.Optional;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import com.google.api.services.bigquery.model.Clustering;

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
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ByteString;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageOptions;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.BufferedReader;

import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.DateTimeZone;

import org.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnonymizeStreamPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(AnonymizeStreamPipeline.class);

    final static TupleTag<PubsubMessage> successTag = new TupleTag<PubsubMessage>(){};
    final static TupleTag<TableRow> deadLetterTag = new TupleTag<TableRow>(){};

	public interface Options extends PipelineOptions, GcpOptions {

        @Description("BigQuery query to extract fields to anonymize")
	  	String getQuery();
	  	void setQuery(String query);

        @Description("Pub/Sub topic to push anonymized messages to")
	    ValueProvider<String> getPubsubTopic();
	    void setPubsubTopic(ValueProvider<String> subscription);  

        @Description("bucketName where schema resides")
		ValueProvider<String> getBucketName();
		void setBucketName(ValueProvider<String> value);
	
		@Description("fileDescriptorName")
		ValueProvider<String> getFileDescriptorName();
		void setFileDescriptorName(ValueProvider<String> value);
	
		@Description("descriptorFullName")
		ValueProvider<String> getDescriptorFullName();
		void setDescriptorFullName(ValueProvider<String> value);

        @Description("taxonomyResourcePattern to filter out fields to anonymize")
		@Default.String(".*")
		ValueProvider<String> getTaxonomyResourcePattern();
		void setTaxonomyResourcePattern(ValueProvider<String> value);
		
        @Description("BigQuery Table Spec [project_id]:[dataset_id].[table_id] or [dataset_id].[table_id]")
		ValueProvider<String> getRecoveryTableSpec();
		void setRecoveryTableSpec(ValueProvider<String> value);

        @Description("BigQuery Table Spec [project_id]:[dataset_id].[table_id] or [dataset_id].[table_id]")
		ValueProvider<String> getAnonymizedTableSpec();
		void setAnonymizedTableSpec(ValueProvider<String> value);
	}


    public static class CleanPubsubMessageFn extends DoFn<PubsubMessage,PubsubMessage> {
        private Descriptor messageDescriptor;
        private ProtoDescriptor protoDescriptor;
        ValueProvider<String> bucketName;
        ValueProvider<String> fileDescriptorName;
        ValueProvider<String> descriptorFullName;
        ValueProvider<String> taxonomyResourcePattern;
		
	  	public CleanPubsubMessageFn(
	  		ValueProvider<String> bucketName,
	  		ValueProvider<String> fileDescriptorName,
            ValueProvider<String> descriptorFullName,
            ValueProvider<String> taxonomyResourcePattern) {
		     	this.bucketName = bucketName;
		     	this.fileDescriptorName = fileDescriptorName;
                this.descriptorFullName = descriptorFullName;
                this.taxonomyResourcePattern = taxonomyResourcePattern;
	   	}
        
        @Setup
        public void setup() throws Exception {

            messageDescriptor = ProtobufUtils.getDescriptorFromCloudStorage(bucketName.get(), fileDescriptorName.get(), descriptorFullName.get());
            protoDescriptor = ProtobufUtils.getProtoDescriptorFromCloudStorage(bucketName.get(), fileDescriptorName.get());
        }
		
		@ProcessElement
        public void processElement(@Element PubsubMessage pubsubMessage, OutputReceiver<PubsubMessage> out) throws Exception {
            // Get pubsub message payload and attributes
            String pubsubPayload = new String(pubsubMessage.getPayload(), StandardCharsets.UTF_8);
            HashMap<String, String> attributes = new HashMap<String,String>();
            attributes.putAll(pubsubMessage.getAttributeMap());

            JSONObject DynamoDbStreamObject = new JSONObject(pubsubPayload);
            JSONObject payloadObject = new JSONObject();
            String payload = "";
            try{
                // add operation and payload according to dynamodb 'NEW_AND_OLD_IMAGES' stream view type
                DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageDescriptor);
                if(!DynamoDbStreamObject.isNull("NewImage")){
                    JsonFormat.parser().ignoringUnknownFields().merge(DynamoDbStreamObject.getJSONObject("NewImage").toString(), builder);
                    DynamicMessage message = builder.build();
                    Message cleanMessage = ProtobufUtils.forgetFields(message, messageDescriptor, protoDescriptor, taxonomyResourcePattern.get());
                    String json = JsonFormat.printer().omittingInsignificantWhitespace().includingDefaultValueFields().print(cleanMessage);
                    payloadObject.put("NewImage", new JSONObject(json));
                    payloadObject.getJSONObject("NewImage").remove("ATTRIBUTES");
                    builder.clear();
                }
                if(!DynamoDbStreamObject.isNull("OldImage")){
                    JsonFormat.parser().ignoringUnknownFields().merge(DynamoDbStreamObject.getJSONObject("OldImage").toString(), builder);
                    DynamicMessage message = builder.build();
                    Message cleanMessage = ProtobufUtils.forgetFields(message, messageDescriptor, protoDescriptor, taxonomyResourcePattern.get());
                    String json = JsonFormat.printer().omittingInsignificantWhitespace().includingDefaultValueFields().print(cleanMessage);
                    payloadObject.put("OldImage", new JSONObject(json));
                    payloadObject.getJSONObject("OldImage").remove("ATTRIBUTES");
                    builder.clear();
                }
                if(DynamoDbStreamObject.isNull("OldImage") && DynamoDbStreamObject.isNull("NewImage")){
                    JsonFormat.parser().ignoringUnknownFields().merge(DynamoDbStreamObject.toString(), builder);
                    DynamicMessage message = builder.build();
                    Message cleanMessage = ProtobufUtils.forgetFields(message, messageDescriptor, protoDescriptor, taxonomyResourcePattern.get());
                    String json = JsonFormat.printer().omittingInsignificantWhitespace().includingDefaultValueFields().print(cleanMessage);
                    payloadObject = new JSONObject(json);
                    payloadObject.remove("ATTRIBUTES");
                    builder.clear();
                }
                
                payload = payloadObject.toString();
                ByteString bs = ByteString.copyFromUtf8(payload);
                byte[] jsonPayload = bs.toByteArray();

				PubsubMessage newPubsubMessage = new PubsubMessage(jsonPayload, attributes);

                out.output(newPubsubMessage);
            }catch(java.lang.NullPointerException e){
                LOG.error("No descriptor?", e);
                LOG.error("Message payload: " + payload + ", Message attributes: " + attributes.toString());
            }catch(InvalidProtocolBufferException e){
                LOG.error("invalid protocol buffer exception: ", e);
                LOG.error("Message payload: " + payload + ", Message attributes: " + attributes.toString());
            }catch(IllegalArgumentException e){
                LOG.error("IllegalArgumentException: ", e);
                LOG.error("Message payload: " + payload + ", Message attributes: " + attributes.toString());
            }catch(org.json.JSONException e){
                LOG.error("org.json.JSONException: ", e);
                LOG.error("Message payload: " + payload + ", Message attributes: " + attributes.toString());
            }catch(Exception e){
                LOG.error("Exception: ", e);
                LOG.error("Message payload: " + payload + ", Message attributes: " + attributes.toString());
            }
        }
  }

	public static void main(String[] args) throws IOException {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
        TableSchema eventSchema = null;
        String tableDescription = "";
        
        TableSchema errorSchema = Failure.getTableSchema();

        try{
            ProtoDescriptor protoDescriptor = ProtobufUtils.getProtoDescriptorFromCloudStorage(options.getBucketName().get(), options.getFileDescriptorName().get());
            Descriptor descriptor = protoDescriptor.getDescriptorByName(options.getDescriptorFullName().get());
            eventSchema = ProtobufUtils.makeTableSchema(protoDescriptor, descriptor, options.getTaxonomyResourcePattern().get());
            HashMultimap<String, String> messageOptions = ProtobufUtils.getMessageOptions(protoDescriptor, descriptor);
            tableDescription = ((Set<String>) messageOptions.get("BigQueryTableDescription")).stream().findFirst().orElse("");
        }catch (Exception e) {
            e.printStackTrace();
        }

        //Attributes tablefield
		List<TableFieldSchema> attributeFieldSchemaList = new ArrayList<>();
		attributeFieldSchemaList.add(new TableFieldSchema().setName("key").setType("STRING"));
		attributeFieldSchemaList.add(new TableFieldSchema().setName("value").setType("STRING"));
		
		//TableFields
		List<TableFieldSchema> fieldsSchemaList = new ArrayList<>();
	    fieldsSchemaList.add(new TableFieldSchema().setName("publish_time").setType("TIMESTAMP").setMode("REQUIRED"));
        fieldsSchemaList.add(new TableFieldSchema().setName("topic").setType("STRING"));
	    fieldsSchemaList.add(new TableFieldSchema().setName("attributes").setType("RECORD").setMode("REPEATED").setFields(attributeFieldSchemaList));
	    fieldsSchemaList.add(new TableFieldSchema().setName("data").setType("BYTES").setMode("REQUIRED"));
		
	    TableSchema schema = new TableSchema().setFields(fieldsSchemaList);
	    TimePartitioning partition = new TimePartitioning().setField("publish_time");

        Clustering cluster = new Clustering();
        cluster.setFields(Arrays.asList("topic"));

        PCollection<TableRow> rawRows = pipeline
            .apply("BigQuery SELECT job",
    		    BigQueryIO
                    .readTableRows()
                    .fromQuery(options.getQuery())
                    .usingStandardSql());
        
        rawRows.apply("InsertToRecoveryTable",
      		BigQueryIO
				.<TableRow>write()
				.to(NestedValueProvider.of(
					options.getRecoveryTableSpec(),
					new SerializableFunction<String, String>() {
						@Override
						public String apply(String tableSpec) {
							return tableSpec.replaceAll("[^A-Za-z0-9._]", "");
						}
				}))
				.withFormatFunction(tr -> tr)
      			.withSchema(schema)
      			.withTimePartitioning(partition)
                .withClustering(cluster)
      			.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
      			.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        
        PCollection<PubsubMessage> anonymized = rawRows
            .apply("TableRow to PubSubMessage", 
                ParDo.of(new DoFn<TableRow,PubsubMessage>() {
                    @ProcessElement
                    public void processElement(@Element TableRow row, OutputReceiver<PubsubMessage> out)  {
                        String b = (String) row.get("data");
                        byte[] payload = Base64.getDecoder().decode(b.getBytes());
                        List<TableRow> repeated = (List<TableRow>) row.get("attributes");
                        HashMap<String, String> attributes = repeated
                            .stream()
                            .collect(HashMap::new, (map,record)-> map.put((String) record.get("key"), (String) record.get("value")), HashMap::putAll);
                        PubsubMessage pubSubMessage = new PubsubMessage(payload, attributes); 
                        out.output(pubSubMessage);
            }}))
            .apply("Anonymize pubsubMessage", ParDo.of(new CleanPubsubMessageFn(
				options.getBucketName(),
                options.getFileDescriptorName(),
                options.getDescriptorFullName(),
                options.getTaxonomyResourcePattern()
            )));
        
        anonymized
            .apply("Write to live pubsub topic",
				PubsubIO
					.writeMessages()
					.to(options.getPubsubTopic()));
        
        anonymized
            .apply("ConvertDataToTableRows", ParDo.of(new DoFn<PubsubMessage, TableRow>() {
			@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
	        	PubsubMessage pubsubMessage = c.element();
				
				Map<String, String> attributeMap = pubsubMessage.getAttributeMap();
	        	List<TableRow> attributes = new ArrayList<>();
	        	if (attributeMap != null) {
	        		attributeMap.forEach((k,v)-> {
	        			attributes.add(new TableRow().set("key",k).set("value", v));
        			});
				}
	        	DateTimeFormatter partition = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZoneUTC();
	        	TableRow tableRow = new TableRow()
	        		.set("publish_time", attributeMap.get("timestamp"))
	        		.set("topic", attributeMap.get("topic"))
                    .set("attributes", attributes)
	        		.set("data", pubsubMessage.getPayload());
	        	
	        	c.output(tableRow);
    		}
		}))
    	.apply("InsertToAnonymizedTable",
      		BigQueryIO
				.<TableRow>write()
				.to(NestedValueProvider.of(
					options.getAnonymizedTableSpec(),
					new SerializableFunction<String, String>() {
						@Override
						public String apply(String tableSpec) {
							return tableSpec.replaceAll("[^A-Za-z0-9._]", "");
						}
				}))
				.withFormatFunction(tr -> tr)
      			.withSchema(schema)
      			.withTimePartitioning(partition)
                .withClustering(cluster)
      			.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
      			.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run().waitUntilFinish();
    }
}