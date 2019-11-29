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


import io.anemos.metastore.core.proto.*;
import org.datahem.processor.generic.GenericStreamPipeline.PubsubMessageToTableRowFn;
import org.datahem.processor.utils.ProtobufUtils;

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
import java.util.Set;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.HashMultimap;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.BufferedReader;

import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

@RunWith(JUnit4.class)
public class GenericStreamPipelineTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(GenericStreamPipelineTest.class);
	
	@Rule public transient TestPipeline p = TestPipeline.create();

    String testPayload = "{\"StringField\":\"hello world\"}";

    byte[] payload = testPayload.getBytes(StandardCharsets.UTF_8);
	private Map<String,String> attributes = new HashMap<String, String>(){
		{
            put("timestamp", "2013-08-16T23:36:32.444Z");
            put("uuid", "123-456-abc");
            put("source", "test");
		}
	};

    PubsubMessage pm = new PubsubMessage(payload, attributes);

    

	@Test
	public void userPageviewTest(){

        List<TableRow> attributes = new ArrayList<TableRow>();

        attributes.add(new TableRow().set("key", "source").set("value", "test"));
        attributes.add(new TableRow().set("key", "uuid").set("value", "123-456-abc"));
        attributes.add(new TableRow().set("key", "timestamp").set("value", "2013-08-16T23:36:32.444Z"));

        TableRow temperatureTR = new TableRow()
            .set("StringField", "hello world")
            .set("_ATTRIBUTES", attributes);


        LOG.info("payload: " + testPayload);
        TableSchema eventSchema = null;
        String tableDescription = "";
        try{
            ProtoDescriptor protoDescriptor = ProtobufUtils.getProtoDescriptorFromCloudStorage("mathem-ml-datahem-test-descriptor", "testSchemas.desc");
            Descriptor descriptor = protoDescriptor.getDescriptorByName("datahem.test.TestWithoutOptions");
            eventSchema = ProtobufUtils.makeTableSchema(protoDescriptor, descriptor, ".*590903188537942776.*");
            LOG.info("eventSchema: " + eventSchema.toString());
            HashMultimap<String, String> messageOptions = ProtobufUtils.getMessageOptions(protoDescriptor, descriptor);
            tableDescription = ((Set<String>) messageOptions.get("BigQueryTableDescription")).stream().findFirst().orElse("");
        }catch (Exception e) {
            e.printStackTrace();
        }

        PCollection<TableRow> output = p
			.apply(Create.of(Arrays.asList(pm)))
			.apply(ParDo.of(new PubsubMessageToTableRowFn(
				StaticValueProvider.of("mathem-ml-datahem-test-descriptor"),
                StaticValueProvider.of("testSchemas.desc"),
                StaticValueProvider.of("datahem.test.TestWithoutOptions"))));
        //Assert.assertEquals(true, true);
        PAssert.that(output).containsInAnyOrder(temperatureTR);
        p.run();
    }
}