package org.datahem.processor.generic;

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
//import java.util.regex.Pattern;
//import java.util.regex.Matcher;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.List;
import java.util.Optional;
import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericStreamPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(GenericStreamPipeline.class);

	public interface Options extends PipelineOptions, GcpOptions {

        @Description("bucketName")
		//@Default.String("")
		ValueProvider<String> getBucketName();
		void setBucketName(ValueProvider<String> value);
	
		@Description("fileDescriptorName")
		//@Default.String("")
		ValueProvider<String> getFileDescriptorName();
		void setFileDescriptorName(ValueProvider<String> value);
	
		@Description("fileDescriptorProtoName")
		//@Default.String("")
		ValueProvider<String> getFileDescriptorProtoName();
		void setFileDescriptorProtoName(ValueProvider<String> value);

        @Description("messageType")
		//@Default.String("")
		ValueProvider<String> getMessageType();
		void setMessageType(ValueProvider<String> value);
		
		@Description("pubsubSubscription")
		//@Default.String("")
		ValueProvider<String> getPubsubSubscription();
		void setPubsubSubscription(ValueProvider<String> subscription);

        @Description("BigQuery Table Spec [project_id]:[dataset_id].[table_id] or [dataset_id].[table_id]")
		//@Default.String("")
		ValueProvider<String> getBigQueryTableSpec();
		void setBigQueryTableSpec(ValueProvider<String> value);
	}

    public static Descriptor getDescriptorFromCloudStorage(String bucketName, String fileDescriptorName, String fileDescriptorProtoName, String messageType) throws Exception {
            try{
                Storage storage = StorageOptions.getDefaultInstance().getService();
                Blob blob = storage.get(BlobId.of(bucketName, fileDescriptorName));
                ReadChannel reader = blob.reader();
                InputStream inputStream = Channels.newInputStream(reader);

                FileDescriptorSet descriptorSetObject = FileDescriptorSet.parseFrom(inputStream);
                List<FileDescriptorProto> fdpl = descriptorSetObject.getFileList();
                Optional<FileDescriptorProto> fdp = fdpl.stream()
                    .filter(m -> m.getName().equals(fileDescriptorProtoName))
                    .findFirst();
                //System.out.println(fdp.orElse(null));
                FileDescriptor[] empty = new FileDescriptor[0];
                FileDescriptor fd = FileDescriptor.buildFrom(fdp.orElse(null), empty);
                Optional<Descriptor> d = fd.getMessageTypes().stream()
                    .filter(m -> m.getName().equals(messageType))
                    .findFirst();
                return d.orElse(null);
            }catch (Exception e){
                e.printStackTrace();
                return null;
            }
        }

    public static class PubsubMessageToTableRowFn extends DoFn<PubsubMessage,TableRow> {
		//private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageToTableRowFn.class);
		//private static Pattern pattern;
    	//private static Matcher matcher;
        private Descriptor messageDescriptor;
        ValueProvider<String> bucketName;
        ValueProvider<String> fileDescriptorName;
        ValueProvider<String> fileDescriptorProtoName;
        ValueProvider<String> messageType;
		
	  	public PubsubMessageToTableRowFn(
	  		ValueProvider<String> bucketName,
	  		ValueProvider<String> fileDescriptorName,
            ValueProvider<String> fileDescriptorProtoName,
            ValueProvider<String> messageType) {
		     	this.bucketName = bucketName;
		     	this.fileDescriptorName = fileDescriptorName;
                this.fileDescriptorProtoName = fileDescriptorProtoName;
                this.messageType = messageType;
	   	}
        
        @Setup
        public void setup() throws Exception {
            messageDescriptor = getDescriptorFromCloudStorage(bucketName.get(), fileDescriptorName.get(), fileDescriptorProtoName.get(), messageType.get());
        }
		
		@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
                // Get pubsub message payload and attributes
                PubsubMessage pubsubMessage = c.element();
                String payload = new String(pubsubMessage.getPayload(), StandardCharsets.UTF_8);
                Map<String, String> attributes = pubsubMessage.getAttributeMap();
                //LOG.info("payload: " + payload);

                // Parse json to protobuf
                DynamicMessage.Builder builder = DynamicMessage.newBuilder(messageDescriptor);
				try{
                    JsonFormat.parser().ignoringUnknownFields().merge(payload, builder);
					attributes.entrySet().forEach(attribute -> {
                        DynamicMessage.Builder attributeBuilder = DynamicMessage.newBuilder(messageDescriptor.findNestedTypeByName("ATTRIBUTESEntry"));
                        attributeBuilder.setField(messageDescriptor.findNestedTypeByName("ATTRIBUTESEntry").findFieldByName("key"), attribute.getKey());
                        attributeBuilder.setField(messageDescriptor.findNestedTypeByName("ATTRIBUTESEntry").findFieldByName("value"), attribute.getValue());
                        builder.addRepeatedField(messageDescriptor.findFieldByName("_ATTRIBUTES"),attributeBuilder.build());
                        });
					DynamicMessage message = builder.build();
                    //LOG.info(message.toString());

                    //transform protobuf to tablerow
                    TableRow tr = ProtobufUtils.makeTableRow(message);
                    /*try{
                        LOG.info(tr.toPrettyString());
                    }catch(Exception e){}*/
                    c.output(tr);
				}catch(InvalidProtocolBufferException e){
					LOG.error("invalid protocol buffer exception: ", e);
					LOG.error(payload);
				}catch(IllegalArgumentException e){
                    LOG.error("IllegalArgumentException: ", e);
				}
    		}
  }

	public static void main(String[] args) throws IOException {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
        TableSchema eventSchema = null;
        
        try{
            eventSchema = ProtobufUtils.makeTableSchema(getDescriptorFromCloudStorage(options.getBucketName().get(), options.getFileDescriptorName().get(), options.getFileDescriptorProtoName().get(), options.getMessageType().get()));
        }catch (Exception e) {
            e.printStackTrace();
        }

		WriteResult writeResult = pipeline
            .apply("Read json as string from pubsub", 
                PubsubIO
                    .readMessagesWithAttributes()
                    .fromSubscription(options.getPubsubSubscription()))
            //combine steps PubsubMessage -> DynamicMessage -> TableRow into one step and make generic
            .apply("PubsubMessage to TableRow", ParDo.of(new PubsubMessageToTableRowFn(
				options.getBucketName(),
                options.getFileDescriptorName(),
                options.getFileDescriptorProtoName(),
                options.getMessageType()
            )))
            .apply("Fixed Windows",
                Window.<TableRow>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .withAllowedLateness(Duration.standardDays(7))
                    .discardingFiredPanes()
                )
            .apply("Write to bigquery", 
                BigQueryIO
                    .writeTableRows()
                    //.to(new TablePartition(options.getBigQueryTableSpec()))
                    .to(options.getBigQueryTableSpec())
                    .withSchema(eventSchema)
                    .withTimePartitioning(new TimePartitioning()) //.setField("_PARTITIONTIME").setType("TIMESTAMP"))
                    //.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        writeResult
            .getFailedInserts()
            .apply("LogFailedData", ParDo.of(new DoFn<TableRow, TableRow>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    TableRow row = c.element();
                    LOG.error("Failed to insert: " + row.toString());
                }
            }));
        
        pipeline.run();
    }
}