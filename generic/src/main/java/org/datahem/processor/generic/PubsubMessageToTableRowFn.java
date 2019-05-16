package org.datahem.processor.generic;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 - 2019 MatHem Sverige AB
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

import com.google.gson.Gson;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import com.google.protobuf.InvalidProtocolBufferException;

import org.datahem.processor.utils.ProtobufUtils;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableReference;

import com.google.protobuf.util.JsonFormat;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.DateTimeZone;
import org.joda.time.DateTime;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Optional;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;

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

public class PubsubMessageToTableRowFn extends DoFn<PubsubMessage,TableRow> {
		private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageToTableRowFn.class);
		private static Pattern pattern;
    	private static Matcher matcher;
        private Descriptor desc;
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
            try{
                Storage storage = StorageOptions.getDefaultInstance().getService();
                Blob blob = storage.get(BlobId.of(bucketName.get(), fileDescriptorName.get()));
                ReadChannel reader = blob.reader();
                InputStream inputStream = Channels.newInputStream(reader);

                FileDescriptorSet descriptorSetObject = FileDescriptorSet.parseFrom(inputStream);
                List<FileDescriptorProto> fdpl = descriptorSetObject.getFileList();
                Optional<FileDescriptorProto> fdp = fdpl.stream()
                    .filter(m -> m.getName().equals(fileDescriptorProtoName.get()))
                    .findFirst();
                System.out.println(fdp.orElse(null));
                FileDescriptor[] empty = new FileDescriptor[0];
                FileDescriptor fd = FileDescriptor.buildFrom(fdp.orElse(null), empty);
                Optional<Descriptor> d = fd.getMessageTypes().stream()
                    .filter(m -> m.getName().equals(messageType.get()))
                    .findFirst();
                desc = d.orElse(null);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
		
		@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
                // Get pubsub message payload and attributes
                PubsubMessage pubsubMessage = c.element();
                String payload = new String(pubsubMessage.getPayload(), StandardCharsets.UTF_8);
                Map<String, String> attributes = pubsubMessage.getAttributeMap();
                //LOG.info("payload: " + payload);

                // Parse json to protobuf
                DynamicMessage.Builder builder = DynamicMessage.newBuilder(desc);
				try{
                    JsonFormat.parser().ignoringUnknownFields().merge(payload, builder);
					attributes.entrySet().forEach(attribute -> {
                        DynamicMessage.Builder attributeBuilder = DynamicMessage.newBuilder(desc.findNestedTypeByName("ATTRIBUTESEntry"));
                        attributeBuilder.setField(desc.findNestedTypeByName("ATTRIBUTESEntry").findFieldByName("key"), attribute.getKey());
                        attributeBuilder.setField(desc.findNestedTypeByName("ATTRIBUTESEntry").findFieldByName("value"), attribute.getValue());
                        builder.addRepeatedField(desc.findFieldByName("_ATTRIBUTES"),attributeBuilder.build());
                        });
					DynamicMessage message = builder.build();
                    //LOG.info(message.toString());

                    //transform protobuf to tablerow
                    TableRow tr = ProtobufUtils.makeTableRow(message);
                    try{
                        //LOG.info(tr.toPrettyString());
                    }catch(Exception e){}
                    c.output(tr);
				}catch(InvalidProtocolBufferException e){
					LOG.error("invalid protocol buffer exception: ", e);
					LOG.error(payload);
				}catch(IllegalArgumentException e){
                    LOG.error("IllegalArgumentException: ", e);
				}
    		}
  }
