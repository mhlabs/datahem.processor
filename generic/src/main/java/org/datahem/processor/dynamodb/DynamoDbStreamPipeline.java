package org.datahem.processor.dynamodb;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;
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
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;

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

public class DynamoDbStreamPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(DynamoDbStreamPipeline.class);

    final static TupleTag<TableRow> successTag = new TupleTag<TableRow>(){};
    final static TupleTag<TableRow> deadLetterTag = new TupleTag<TableRow>(){};

	public interface Options extends PipelineOptions, GcpOptions {

        @Description("bucketName")
		ValueProvider<String> getBucketName();
		void setBucketName(ValueProvider<String> value);
	
		@Description("fileDescriptorName")
		ValueProvider<String> getFileDescriptorName();
		void setFileDescriptorName(ValueProvider<String> value);
	
		@Description("descriptorFullName")
		ValueProvider<String> getDescriptorFullName();
		void setDescriptorFullName(ValueProvider<String> value);

        @Description("taxonomyResourcePattern")
		@Default.String(".*")
		ValueProvider<String> getTaxonomyResourcePattern();
		void setTaxonomyResourcePattern(ValueProvider<String> value);
		
		@Description("pubsubSubscription")
		ValueProvider<String> getPubsubSubscription();
		void setPubsubSubscription(ValueProvider<String> subscription);

/*
        @Description("config")
		ValueProvider<String> getConfig();
		void setConfig(ValueProvider<String> config);
*/

        @Description("BigQuery Table Spec [project_id]:[dataset_id].[table_id] or [dataset_id].[table_id]")
		ValueProvider<String> getBigQueryTableSpec();
		void setBigQueryTableSpec(ValueProvider<String> value);

        @Description("BigQuery insert error table")
		@Default.String("backup.error")
		ValueProvider<String> getBigQueryErrorTableSpec();
		void setBigQueryErrorTableSpec(ValueProvider<String> value);
	}

    public static class PubsubMessageToTableRowFn extends DoFn<PubsubMessage,TableRow> {
        private Descriptor messageDescriptor;
        private ProtoDescriptor protoDescriptor;
        ValueProvider<String> bucketName;
        ValueProvider<String> fileDescriptorName;
        ValueProvider<String> descriptorFullName;
		
	  	public PubsubMessageToTableRowFn(
	  		ValueProvider<String> bucketName,
	  		ValueProvider<String> fileDescriptorName,
            ValueProvider<String> descriptorFullName) {
		     	this.bucketName = bucketName;
		     	this.fileDescriptorName = fileDescriptorName;
                this.descriptorFullName = descriptorFullName;
	   	}
        
        @Setup
        public void setup() throws Exception {
            messageDescriptor = ProtobufUtils.getDescriptorFromCloudStorage(bucketName.get(), fileDescriptorName.get(), descriptorFullName.get());
            protoDescriptor = ProtobufUtils.getProtoDescriptorFromCloudStorage(bucketName.get(), fileDescriptorName.get());
        }
		
		@ProcessElement
			//public void processElement(ProcessContext c) throws Exception {
            public void processElement(@Element PubsubMessage pubsubMessage, MultiOutputReceiver out) throws Exception {
                // Get pubsub message payload and attributes
                //PubsubMessage pubsubMessage = c.element();
                String pubsubPayload = new String(pubsubMessage.getPayload(), StandardCharsets.UTF_8);
                HashMap<String, String> attributes = new HashMap<String,String>();
                attributes.putAll(pubsubMessage.getAttributeMap());

                JSONObject DynamoDbStreamObject = new JSONObject(pubsubPayload);
                JSONObject payloadObject;
                // add operation and payload according to dynamodb 'NEW_AND_OLD_IMAGES' stream view type
                if((DynamoDbStreamObject.isNull("OldImage") || DynamoDbStreamObject.getJSONObject("OldImage").isNull("Id"))){
                    attributes.put("operation", "INSERT");
                    payloadObject = DynamoDbStreamObject.getJSONObject("NewImage");
                }else if(DynamoDbStreamObject.isNull("NewImage") || DynamoDbStreamObject.getJSONObject("NewImage").isNull("Id")){
                    attributes.put("operation", "REMOVE");
                    payloadObject = DynamoDbStreamObject.getJSONObject("OldImage");
                }else {
                    attributes.put("operation", "MODIFY");
                    payloadObject = DynamoDbStreamObject.getJSONObject("NewImage");
                }

                // Add meta-data from dynamoDB stream event as attributes
                if(!DynamoDbStreamObject.isNull("Published")){
                    attributes.put("dynamoDbStreamPublished",DynamoDbStreamObject.getString("Published"));
                }
                if(!DynamoDbStreamObject.isNull("EventId")){
                    attributes.put("dynamoDbStreamEventId",DynamoDbStreamObject.getString("EventId"));
                }

                String payload = payloadObject.toString();

                // Parse json to protobuf
                if(messageDescriptor == null){
                    // fetch the message descriptor if current one is null for some reason
                    LOG.warn("message descriptor is null, creating new from descriptor in cloud storage...");
                    messageDescriptor = ProtobufUtils.getDescriptorFromCloudStorage(bucketName.get(), fileDescriptorName.get(), descriptorFullName.get());
                }
                if(protoDescriptor == null){
                    // fetch the proto descriptor if current one is null for some reason
                    LOG.warn("protoDescriptor is null, creating new from descriptor in cloud storage...");
                    protoDescriptor = ProtobufUtils.getProtoDescriptorFromCloudStorage(bucketName.get(), fileDescriptorName.get());
                }
				try{
                    DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageDescriptor);
                    try{
                        // Parse but don't allow unknown fields
                        JsonFormat.parser().merge(payload, builder);
                    }catch(InvalidProtocolBufferException e){
                        // Alert if unknown fields exist in message
                        LOG.error("Unknown fields in message, doesn't match current schema " + descriptorFullName.get(), e);
                        builder.clear();
                        // Parse but allow for unknown fields
                        JsonFormat.parser().ignoringUnknownFields().merge(payload, builder);
                    }
					try{
                        // Add pubsub message attributes in a protobuf map
                        attributes.entrySet().forEach(attribute -> {
                            DynamicMessage.Builder attributeBuilder = DynamicMessage.newBuilder(messageDescriptor.findNestedTypeByName("ATTRIBUTESEntry")); // ATTRIBUTESEntry type is generated by protoc
                            attributeBuilder.setField(messageDescriptor.findNestedTypeByName("ATTRIBUTESEntry").findFieldByName("key"), attribute.getKey());
                            attributeBuilder.setField(messageDescriptor.findNestedTypeByName("ATTRIBUTESEntry").findFieldByName("value"), attribute.getValue());
                            builder.addRepeatedField(messageDescriptor.findFieldByName("_ATTRIBUTES"),attributeBuilder.build());
                            });
                    }catch(java.lang.NullPointerException e){
                        LOG.error("No _ATTRIBUTES field in message", e);
                        LOG.error("Message payload: " + payload + ", Message attributes: " + attributes.toString());
                    }
					DynamicMessage message = builder.build();
                    //transform protobuf to tablerow
                    TableRow tr = ProtobufUtils.makeTableRow(message, messageDescriptor, protoDescriptor);
                    out.get(successTag).output(tr);
                    //c.output(tr);
                }catch(java.lang.NullPointerException e){
                    out.get(deadLetterTag).output(new Failure(descriptorFullName.get(), payload, e.toString(), "BEAM_PROCESSING_ERROR").getAsTableRow());
                    LOG.error("No descriptor?", e);
                    LOG.error("Message payload: " + payload + ", Message attributes: " + attributes.toString());
				}catch(InvalidProtocolBufferException e){
					LOG.error("invalid protocol buffer exception: ", e);
                    LOG.error("Message payload: " + payload + ", Message attributes: " + attributes.toString());
				}catch(IllegalArgumentException e){
                    LOG.error("IllegalArgumentException: ", e);
                    LOG.error("Message payload: " + payload + ", Message attributes: " + attributes.toString());
				}catch(Exception e){
                    LOG.error("Exception: ", e);
                    LOG.error("Message payload: " + payload + ", Message attributes: " + attributes.toString());
				}
    		}
  }

  public static class Failure{
        private String target;
        private String message;
        private String error;
        private String errorType;

    public Failure(String target, String message, String error, String errorType){
        this.target = target;
        this.message = message;
        this.error = error;
        this.errorType = errorType;
    }

    public String getTarget(){return target;}
    public String getMessage(){return message;}
    public String getError(){return error;}
    public String getErrorType(){return errorType;}

    public TableRow getAsTableRow(){
        TableRow outputRow = new TableRow();
        outputRow.set("Target", this.getTarget());
        outputRow.set("Message", this.getMessage());
        outputRow.set("Error", this.getError());
        outputRow.set("ErrorType", this.getError());
        return outputRow;
    }

  }

	public static void main(String[] args) throws IOException {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
        TableSchema eventSchema = null;
        String tableDescription = "";
        
        TableSchema errorSchema = new TableSchema();
        List<TableFieldSchema> errorSchemaFields = new ArrayList<TableFieldSchema>();
        errorSchemaFields.add(new TableFieldSchema().setName("Target").setType("STRING").setMode("NULLABLE").setDescription(""));
        errorSchemaFields.add(new TableFieldSchema().setName("Message").setType("STRING").setMode("NULLABLE").setDescription(""));
        errorSchemaFields.add(new TableFieldSchema().setName("Error").setType("STRING").setMode("NULLABLE").setDescription(""));
        errorSchemaFields.add(new TableFieldSchema().setName("ErrorType").setType("STRING").setMode("NULLABLE").setDescription(""));
        errorSchema.setFields(errorSchemaFields);

        try{
            ProtoDescriptor protoDescriptor = ProtobufUtils.getProtoDescriptorFromCloudStorage(options.getBucketName().get(), options.getFileDescriptorName().get());
            Descriptor descriptor = protoDescriptor.getDescriptorByName(options.getDescriptorFullName().get());
            eventSchema = ProtobufUtils.makeTableSchema(protoDescriptor, descriptor, options.getTaxonomyResourcePattern().get());
            LOG.info("eventSchema: " + eventSchema.toString());
            HashMultimap<String, String> messageOptions = ProtobufUtils.getMessageOptions(protoDescriptor, descriptor);
            tableDescription = ((Set<String>) messageOptions.get("BigQueryTableDescription")).stream().findFirst().orElse("");
        }catch (Exception e) {
            e.printStackTrace();
        }

		//WriteResult writeResult = pipeline
        PCollectionTuple results = pipeline
            .apply("Read json as string from pubsub", 
                PubsubIO
                    .readMessagesWithAttributes()
                    .fromSubscription(options.getPubsubSubscription())
                    .withIdAttribute("uuid")
                    )
            .apply("Fixed Windows",
                Window.<PubsubMessage>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .withAllowedLateness(Duration.standardDays(7))
                    .discardingFiredPanes()
                )
            // Combine steps PubsubMessage -> DynamicMessage -> TableRow into one step and make generic (beam 2.X will fix dynamic messages in proto coder)
            .apply("PubsubMessage to TableRow", ParDo.of(new PubsubMessageToTableRowFn(
				options.getBucketName(),
                options.getFileDescriptorName(),
                options.getDescriptorFullName()
            )).withOutputTags(
                successTag,
                TupleTagList.of(deadLetterTag)
            ));
            
            
            WriteResult writeResult = results.get(successTag)
            .apply("Write to bigquery", 
                BigQueryIO
                    .writeTableRows()
                    .to(new TablePartition(options.getBigQueryTableSpec(), tableDescription))
                    .withSchema(eventSchema)
                    .skipInvalidRows()
                    .ignoreUnknownValues()
                    .withExtendedErrorInfo()
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry())
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        writeResult
            /*.getFailedInserts()
            .apply("LogFailedData", ParDo.of(new DoFn<TableRow, TableRow>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    TableRow row = c.element();
                    LOG.error("Failed to insert: " + row.toString());
                }
            }))*/
            .getFailedInsertsWithErr()
            .apply("Transform failed inserts", ParDo.of(new DoFn<BigQueryInsertError, TableRow>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    BigQueryInsertError bqError = c.element();
                    LOG.error("Failed to insert: " + bqError.getError().toString());
                    c.output(new Failure(
                        bqError.getTable().toString(), 
                        bqError.getRow().toString(), 
                        bqError.getError().toString(), 
                        "BIGQUERY_INSERT_ERROR").getAsTableRow());
                    /*TableRow bqErrorRow = new TableRow();
                    bqErrorRow.set("Target", bqError.getTable().toString());
                    bqErrorRow.set("Message", bqError.getRow().toString());
                    bqErrorRow.set("Error", bqError.getError().toString());
                    bqErrorRow.set("ErrorType", "BIGQUERY_INSERT_ERROR");*/
                    //LOG.error("Failed to insert: " + bqError.getError().toString());
                    //c.output(bqErrorRow);
                }
            }))
            .apply("Write errors to bigquery error table", 
                BigQueryIO
                    .writeTableRows()
                    .to(new TablePartition(options.getBigQueryErrorTableSpec(), "BigQuery Insert error table"))
                    .withSchema(errorSchema)
                    .skipInvalidRows()
                    .ignoreUnknownValues()
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry())
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        
        results.get(deadLetterTag)
        /*
            .apply("Transform processing errors", ParDo.of(new DoFn<TableRow, TableRow>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    Failure failure = c.element();
                    TableRow outputRow = new TableRow();
                    outputRow.set("Table", failure.getTable().toString());
                    outputRow.set("TableRow", failure.toString());
                    outputRow.set("Error", failure.getError().toString());
                    outputRow.set("ErrorType", "BEAM_PROCESSING_ERROR");
                    c.output(outputRow);
                }
            }))*/
            .apply("Write processing errors to bigquery error table", 
                BigQueryIO
                    .writeTableRows()
                    .to(new TablePartition(options.getBigQueryErrorTableSpec(), "BigQuery Insert error table"))
                    .withSchema(errorSchema)
                    .skipInvalidRows()
                    .ignoreUnknownValues()
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry())
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        
        pipeline.run();
    }
}