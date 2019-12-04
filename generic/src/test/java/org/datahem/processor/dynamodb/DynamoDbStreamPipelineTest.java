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


import io.anemos.metastore.core.proto.*;
import org.datahem.processor.dynamodb.DynamoDbStreamPipeline.*;
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

import org.json.JSONObject;
import org.json.JSONArray;

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
import java.util.Base64;

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
public class DynamoDbStreamPipelineTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(DynamoDbStreamPipelineTest.class);
	
	@Rule public transient TestPipeline p = TestPipeline.create();

    
	private Map<String,String> attributes = new HashMap<String, String>(){
		{
            put("timestamp", "2013-08-16T23:36:32.444Z");
            put("uuid", "123-456-abc");
            put("source", "test");
		}
	};


	@Test
	public void withoutOptionsTest(){

        JSONObject testPayloadObject = new JSONObject()
            .put("StringField", "a string")
            .put("Int32Field", 32) 
            .put("Int64Field", 64) 
            .put("DoubleField", 1.1) 
            .put("FloatField", 1) 
            .put("BoolField", true) 
            .put("BytesField",  Base64.getEncoder().encodeToString("bytes".getBytes())) 
            .put("EnumField", 1)
            .put("repeatedString", new JSONArray().put("one").put("two").put("three"))
            .put("repeatedInt32", new JSONArray().put(32).put(64).put(128))
            .put("repeatedInt64", new JSONArray().put(64).put(128).put(256))
            .put("repeatedDouble", new JSONArray().put(1.1).put(1.2).put(1.3))
            .put("repeatedFloat", new JSONArray().put(1).put(2).put(3))
            .put("repeatedBool", new JSONArray().put("true").put("false").put("true"))
            .put("repeatedBytes", new JSONArray()
                .put(Base64.getEncoder().encodeToString("bytes".getBytes()))
                .put(Base64.getEncoder().encodeToString("bytes".getBytes()))
                .put(Base64.getEncoder().encodeToString("bytes".getBytes())))
            .put("repeatedEnum", new JSONArray().put(0).put(1).put(2));
        
        JSONObject messageChild = new JSONObject(testPayloadObject.toString());
        messageChild.remove("_ATTRIBUTES");
        messageChild.put("stringMap", new JSONObject()
            .put("timestamp", "2013-08-16T23:36:32.444Z")
            .put("uuid", "123-456-abc")
            .put("source", "test"));

        testPayloadObject
            .put("messageChild", messageChild)
            .put("repeatedMessage", new JSONArray()
                .put(messageChild)
                .put(messageChild));

        String testPayload = new JSONObject().put("NewImage",testPayloadObject).toString();
        byte[] payload = testPayload.getBytes(StandardCharsets.UTF_8);
        PubsubMessage pm = new PubsubMessage(payload, attributes);
        //END INPUT

        //BEGIN OUTPUT TABLEROW
        List<TableRow> attributes = new ArrayList<TableRow>();
        attributes.add(new TableRow().set("key", "source").set("value", "test"));
        attributes.add(new TableRow().set("key", "uuid").set("value", "123-456-abc"));
        attributes.add(new TableRow().set("key", "timestamp").set("value", "2013-08-16T23:36:32.444Z"));

        TableRow childMessage = new TableRow()
                .set("StringField", "a string")
                .set("Int32Field", 32)
                .set("Int64Field", 64)
                .set("DoubleField", 1.1)
                .set("FloatField", 1.0)
                .set("BoolField", true)
                .set("BytesField", "Ynl0ZXM=")
                .set("EnumField", 1)
                .set("repeatedString", Stream.of("one", "two", "three").collect(Collectors.toList()))
                .set("repeatedInt32", Stream.of(32, 64, 128).collect(Collectors.toList()))
                .set("repeatedInt64", Stream.of(64, 128, 256).collect(Collectors.toList()))
                .set("repeatedDouble", Stream.of(1.1, 1.2, 1.3).collect(Collectors.toList()))
                .set("repeatedFloat", Stream.of(1.0, 2.0, 3.0).collect(Collectors.toList()))
                .set("repeatedBool", Stream.of(true, false, true).collect(Collectors.toList()))
                .set("repeatedBytes", Stream.of("Ynl0ZXM=", "Ynl0ZXM=", "Ynl0ZXM=").collect(Collectors.toList()))
                .set("repeatedEnum", Stream.of(0,1,2).collect(Collectors.toList()))
                .set("stringMap", attributes);
        
        attributes = new ArrayList<TableRow>();
        attributes.add(new TableRow().set("key", "source").set("value", "test"));
        attributes.add(new TableRow().set("key", "uuid").set("value", "123-456-abc"));
        attributes.add(new TableRow().set("key", "operation").set("value", "INSERT"));
        attributes.add(new TableRow().set("key", "timestamp").set("value", "2013-08-16T23:36:32.444Z"));

        TableRow assertTableRow = new TableRow()
            .set("StringField", "a string")
            .set("Int32Field", 32)
            .set("Int64Field", 64)
            .set("DoubleField", 1.1)
            .set("FloatField", 1.0)
            .set("BoolField", true)
            .set("BytesField", "Ynl0ZXM=")
            .set("EnumField", 1)
            .set("messageChild", childMessage)
            .set("repeatedMessage", Stream.of(childMessage,childMessage).collect(Collectors.toList()))
            .set("repeatedString", Stream.of("one", "two", "three").collect(Collectors.toList()))
            .set("repeatedInt32", Stream.of(32, 64, 128).collect(Collectors.toList()))
            .set("repeatedInt64", Stream.of(64, 128, 256).collect(Collectors.toList()))
            .set("repeatedDouble", Stream.of(1.1, 1.2, 1.3).collect(Collectors.toList()))
            .set("repeatedFloat", Stream.of(1.0, 2.0, 3.0).collect(Collectors.toList()))
            .set("repeatedBool", Stream.of(true, false, true).collect(Collectors.toList()))
            .set("repeatedBytes", Stream.of("Ynl0ZXM=", "Ynl0ZXM=", "Ynl0ZXM=").collect(Collectors.toList()))
            .set("repeatedEnum", Stream.of(0,1,2).collect(Collectors.toList()))
            .set("_ATTRIBUTES", attributes);
        
        
        TableFieldSchema.Categories fieldCategories = new TableFieldSchema.Categories();
        fieldCategories.setNames(null);
        
        TableSchema assertAttributesSchema = new TableSchema();
        List<TableFieldSchema> attributesFields = new ArrayList<TableFieldSchema>();
        attributesFields.add(new TableFieldSchema().setName("key").setType("STRING").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        attributesFields.add(new TableFieldSchema().setName("value").setType("STRING").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        assertAttributesSchema.setFields(attributesFields);
        
        TableSchema assertChildSchema = new TableSchema();
        List<TableFieldSchema> childFields = new ArrayList<TableFieldSchema>();
        childFields.add(new TableFieldSchema().setName("StringField").setType("STRING").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("Int32Field").setType("INTEGER").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("Int64Field").setType("INTEGER").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("DoubleField").setType("FLOAT").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("FloatField").setType("FLOAT").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("BoolField").setType("BOOLEAN").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("BytesField").setType("BYTES").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("EnumField").setType("INTEGER").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("repeatedString").setType("STRING").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("repeatedInt32").setType("INTEGER").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("repeatedInt64").setType("INTEGER").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("repeatedDouble").setType("FLOAT").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("repeatedFloat").setType("FLOAT").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("repeatedBool").setType("BOOLEAN").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("repeatedBytes").setType("BYTES").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("repeatedEnum").setType("INTEGER").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("stringMap").setType("RECORD").setMode("REPEATED").setDescription("").setFields(assertAttributesSchema.getFields()));
        assertChildSchema.setFields(childFields);

        TableSchema assertSchema = new TableSchema();
        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
        fields.add(new TableFieldSchema().setName("StringField").setType("STRING").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("Int32Field").setType("INTEGER").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("Int64Field").setType("INTEGER").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("DoubleField").setType("FLOAT").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("FloatField").setType("FLOAT").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("BoolField").setType("BOOLEAN").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("BytesField").setType("BYTES").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("EnumField").setType("INTEGER").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("messageChild").setType("RECORD").setMode("NULLABLE").setDescription("").setFields(assertChildSchema.getFields()));
        fields.add(new TableFieldSchema().setName("repeatedMessage").setType("RECORD").setMode("REPEATED").setDescription("").setFields(assertChildSchema.getFields()));
        fields.add(new TableFieldSchema().setName("repeatedString").setType("STRING").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("repeatedInt32").setType("INTEGER").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("repeatedInt64").setType("INTEGER").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("repeatedDouble").setType("FLOAT").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("repeatedFloat").setType("FLOAT").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("repeatedBool").setType("BOOLEAN").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("repeatedBytes").setType("BYTES").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("repeatedEnum").setType("INTEGER").setMode("REPEATED").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("_ATTRIBUTES").setType("RECORD").setMode("REPEATED").setDescription("").setFields(assertAttributesSchema.getFields()));
        assertSchema.setFields(fields);

        LOG.info("payload: " + testPayload);
        TableSchema eventSchema = null;
        String tableDescription = "";
        try{
            ProtoDescriptor protoDescriptor = ProtobufUtils.getProtoDescriptorFromCloudStorage("mathem-ml-datahem-test-descriptor", "testSchemas.desc");
            Descriptor descriptor = protoDescriptor.getDescriptorByName("datahem.test.TestWithoutOptions");
            eventSchema = ProtobufUtils.makeTableSchema(protoDescriptor, descriptor, ".*590903188537942776.*");
            LOG.info("eventSchema: " + eventSchema.toString());
        }catch (Exception e) {
            e.printStackTrace();
        }

        try{

            PCollection<TableRow> output = p
                .apply(Create.of(Arrays.asList(pm)))
                .apply(ParDo.of(new DynamoDbStreamPipeline.PubsubMessageToTableRowFn(
                    StaticValueProvider.of("mathem-ml-datahem-test-descriptor"),
                    StaticValueProvider.of("testSchemas.desc"),
                    StaticValueProvider.of("datahem.test.TestWithoutOptions"))));
            PAssert.that(output).containsInAnyOrder(assertTableRow);
            p.run();

            Assert.assertEquals(eventSchema, assertSchema);
        }catch (Exception e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }



    @Test
	public void withSchemaOptionsTest(){

        //BEGIN INPUT
        JSONObject messageChild = new JSONObject()
            .put("StringField", "a string")
            .put("Int32Field", 32) 
            .put("Int64Field", 64) 
            .put("DoubleField", 1.1) 
            .put("FloatField", 1.1) 
            .put("BoolField", true) 
            .put("HiddenStringField", "a hidden string");

        JSONObject testPayloadObject = new JSONObject()
            .put("StringField", "a string")
            .put("Int32Field", 32) 
            .put("Int64Field", 64) 
            .put("DoubleField", 1.1) 
            .put("FloatField", 1.1) 
            .put("BoolField", true) 
            .put("BytesField",  Base64.getEncoder().encodeToString("bytes".getBytes())) 
            .put("EnumField", 1)
            .put("messageChild", messageChild)
            .put("repeatedMessage", new JSONArray()
                .put(messageChild)
                .put(messageChild))
            .put("repeatedString", new JSONArray().put("one").put("two").put("three"))
            .put("repeatedInt32", new JSONArray().put(32).put(64).put(128))
            .put("repeatedInt64", new JSONArray().put(64).put(128).put(256))
            .put("repeatedDouble", new JSONArray().put(1.1).put(1.2).put(1.3))
            .put("repeatedFloat", new JSONArray().put(1.1).put(1.2).put(1.3))
            .put("repeatedBool", new JSONArray().put("true").put("false").put("true"))
            .put("repeatedBytes", new JSONArray()
                .put(Base64.getEncoder().encodeToString("bytes".getBytes()))
                .put(Base64.getEncoder().encodeToString("bytes".getBytes()))
                .put(Base64.getEncoder().encodeToString("bytes".getBytes())))
            .put("repeatedEnum", new JSONArray().put(0).put(1).put(2))
            .put("HiddenStringField", "a hidden string")
            .put("BigQueryTime", "19:00:00")
            .put("BigQueryDate", "2019-12-03")
            .put("BigQueryDatetime", "2019-12-03 19:00:00")
            .put("BigQueryTimestamp", "2019-12-03T21:00:00+02:00")
            .put("HiddenStringField", "a hidden string");
            

        String testPayload = new JSONObject().put("NewImage",testPayloadObject).toString();
        byte[] payload = testPayload.getBytes(StandardCharsets.UTF_8);
        PubsubMessage pm = new PubsubMessage(payload, attributes);
        //END INPUT

        //BEGIN OUTPUT TABLEROW
        List<TableRow> attributes = new ArrayList<TableRow>();
        attributes.add(new TableRow().set("key", "source").set("value", "test"));
        attributes.add(new TableRow().set("key", "uuid").set("value", "123-456-abc"));
        attributes.add(new TableRow().set("key", "operation").set("value", "INSERT"));
        attributes.add(new TableRow().set("key", "timestamp").set("value", "2013-08-16T23:36:32.444Z"));

        TableRow childMessage = new TableRow()
                .set("RenamedChildString", "a string")
                .set("RenamedChildInt32", 32)
                .set("RenamedChildInt64", 64)
                .set("RenamedChildDouble", 1.1d)
                .set("RenamedChildFloat", 1.1f)
                .set("RenamedChildBool", true);

        TableRow assertTableRow = new TableRow()
            .set("RenamedString", "a string")
            .set("RenamedInt32", 32)
            .set("RenamedInt64", 64L)
            .set("RenamedDouble", 1.1d)
            .set("RenamedFloat", 1.1f)
            .set("RenamedBool", true)
            .set("RenamedBytes", "Ynl0ZXM=")
            .set("RenamedEnum", 1)
            .set("RenamedMessageChild", childMessage)
            .set("RenamedRepeatedMessage", Stream.of(childMessage,childMessage).collect(Collectors.toList()))
            .set("RenamedRepeatedString", Stream.of("one", "two", "three").collect(Collectors.toList()))
            .set("RenamedRepeatedInt32", Stream.of(32, 64, 128).collect(Collectors.toList()))
            .set("RenamedRepeatedInt64", Stream.of(64L, 128L, 256L).collect(Collectors.toList()))
            .set("RenamedRepeatedDouble", Stream.of(1.1d, 1.2d, 1.3d).collect(Collectors.toList()))
            .set("RenamedRepeatedFloat", Stream.of(1.1f, 1.2f, 1.3f).collect(Collectors.toList()))
            .set("RenamedRepeatedBool", Stream.of(true, false, true).collect(Collectors.toList()))
            .set("RenamedRepeatedBytes", Stream.of("Ynl0ZXM=", "Ynl0ZXM=", "Ynl0ZXM=").collect(Collectors.toList()))
            .set("RenamedRepeatedEnum", Stream.of(0,1,2).collect(Collectors.toList()))
            .set("RenamedBigQueryTime", "19:00:00")
            .set("RenamedBigQueryDate", "2019-12-03")
            .set("RenamedBigQueryDatetime", "2019-12-03 19:00:00")
            .set("RenamedBigQueryTimestamp", "2019-12-03T21:00:00+02:00")
            .set("RenamedAttributesMap", attributes);
            
        //END OUTPUT TABLEROW
        
        //BEGIN OUTPUT TABLESCHEMA
        TableFieldSchema.Categories fieldCategories = new TableFieldSchema.Categories();
        fieldCategories.setNames(Stream.of("projects/datahem/taxonomies/1234567890/categories/1234567890").collect(Collectors.toList()));
        
        TableSchema assertAttributesSchema = new TableSchema();
        List<TableFieldSchema> attributesFields = new ArrayList<TableFieldSchema>();
        attributesFields.add(new TableFieldSchema().setName("key").setType("STRING").setMode("NULLABLE").setDescription("").setCategories(new TableFieldSchema.Categories().setNames(null)));
        attributesFields.add(new TableFieldSchema().setName("value").setType("STRING").setMode("NULLABLE").setDescription("").setCategories(new TableFieldSchema.Categories().setNames(null)));
        assertAttributesSchema.setFields(attributesFields);
        
        TableSchema assertChildSchema = new TableSchema();
        List<TableFieldSchema> childFields = new ArrayList<TableFieldSchema>();
        childFields.add(new TableFieldSchema().setName("RenamedChildString").setType("STRING").setMode("NULLABLE").setDescription("A String").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("RenamedChildInt32").setType("INTEGER").setMode("NULLABLE").setDescription("An int32").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("RenamedChildInt64").setType("INTEGER").setMode("NULLABLE").setDescription("An int64").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("RenamedChildDouble").setType("FLOAT").setMode("NULLABLE").setDescription("A double").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("RenamedChildFloat").setType("FLOAT").setMode("NULLABLE").setDescription("A float").setCategories(fieldCategories));
        childFields.add(new TableFieldSchema().setName("RenamedChildBool").setType("BOOLEAN").setMode("NULLABLE").setDescription("A bool").setCategories(fieldCategories));
        assertChildSchema.setFields(childFields);

        TableSchema assertSchema = new TableSchema();
        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
        fields.add(new TableFieldSchema().setName("RenamedString").setType("STRING").setMode("NULLABLE").setDescription("A String").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedInt32").setType("INTEGER").setMode("NULLABLE").setDescription("An int32").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedInt64").setType("INTEGER").setMode("NULLABLE").setDescription("An int64").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedDouble").setType("FLOAT").setMode("NULLABLE").setDescription("A double").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedFloat").setType("FLOAT").setMode("NULLABLE").setDescription("A float").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedBool").setType("BOOLEAN").setMode("NULLABLE").setDescription("A bool").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedBytes").setType("BYTES").setMode("NULLABLE").setDescription("A bytes").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedEnum").setType("INTEGER").setMode("NULLABLE").setDescription("An enum.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedMessageChild").setType("RECORD").setMode("NULLABLE").setDescription("A message (record)").setFields(assertChildSchema.getFields()));
        fields.add(new TableFieldSchema().setName("RenamedRepeatedMessage").setType("RECORD").setMode("REPEATED").setDescription("A repeated message.").setFields(assertChildSchema.getFields()));
        fields.add(new TableFieldSchema().setName("RenamedRepeatedString").setType("STRING").setMode("REPEATED").setDescription("A repeated string.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedRepeatedInt32").setType("INTEGER").setMode("REPEATED").setDescription("A repeated int32.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedRepeatedInt64").setType("INTEGER").setMode("REPEATED").setDescription("A repeated int64.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedRepeatedDouble").setType("FLOAT").setMode("REPEATED").setDescription("A repeated double.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedRepeatedFloat").setType("FLOAT").setMode("REPEATED").setDescription("A repeated float.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedRepeatedBool").setType("BOOLEAN").setMode("REPEATED").setDescription("A repeated bool.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedRepeatedBytes").setType("BYTES").setMode("REPEATED").setDescription("A repeated bytes.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedRepeatedEnum").setType("INTEGER").setMode("REPEATED").setDescription("A repeated enum.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedBigQueryTime").setType("TIME").setMode("NULLABLE").setDescription("A BigQuery TIME.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedBigQueryDate").setType("DATE").setMode("NULLABLE").setDescription("A BigQuery DATE.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedBigQueryDatetime").setType("DATETIME").setMode("NULLABLE").setDescription("A BigQuery DATETIME.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedBigQueryTimestamp").setType("TIMESTAMP").setMode("NULLABLE").setDescription("A BigQuery TIMESTAMP.").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("RenamedAttributesMap").setType("RECORD").setMode("REPEATED").setDescription("A string map.").setFields(assertAttributesSchema.getFields()));
        assertSchema.setFields(fields);
        
        //END OUTPUT TABLESCHEMA

        LOG.info("payload: " + testPayload);
        TableSchema eventSchema = null;
        String tableDescription = "";
        try{
            ProtoDescriptor protoDescriptor = ProtobufUtils.getProtoDescriptorFromCloudStorage("mathem-ml-datahem-test-descriptor", "testSchemas.desc");
            Descriptor descriptor = protoDescriptor.getDescriptorByName("datahem.test.TestSchemaOptions");
            eventSchema = ProtobufUtils.makeTableSchema(protoDescriptor, descriptor, ".*1234567890.*");
            LOG.info("eventSchema: " + eventSchema.toString());
        }catch (Exception e) {
            e.printStackTrace();
        }

        try{

            PCollection<TableRow> output = p
                .apply(Create.of(Arrays.asList(pm)))
                .apply(ParDo.of(new DynamoDbStreamPipeline.PubsubMessageToTableRowFn(
                    StaticValueProvider.of("mathem-ml-datahem-test-descriptor"),
                    StaticValueProvider.of("testSchemas.desc"),
                    StaticValueProvider.of("datahem.test.TestSchemaOptions"))));
            PAssert.that(output).containsInAnyOrder(assertTableRow);
            p.run();

            Assert.assertEquals(eventSchema, assertSchema);
        }catch (Exception e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }

    
    @Test
	public void withOptionsTest(){

        //BEGIN INPUT
        JSONObject messageChild = new JSONObject()
            .put("StringField", "a string")
            .put("Int32Field", 32) 
            .put("Int64Field", 64) 
            .put("DoubleField", 1.1) 
            .put("FloatField", 1.1) 
            .put("BoolField", true) 
            .put("HiddenStringField", "a hidden string");

        JSONObject testPayloadObject = new JSONObject()
            .put("StringAppendix", "a string")
            .put("StringFilter", "throw away")
            .put("StringCoalesce", "")
            .put("DoubleField", 100)
            .put("Int32Field", 10)
            .put("BigQueryDatetimeFiltered","0001-01-01 00:00:00")
            .put("BigQueryTimestampFiltered","0001-01-01T00:00:00+02:00")
            .put("BigQueryDatetimeNotFiltered","2019-01-01 00:00:00")
            .put("BigQueryTimestampNotFiltered","2019-01-01T00:00:00+02:00")
            .put("BigQueryTimeAppend","01:01")
            .put("BigQueryDatetimeAppend","2019-01-01 01:01")
            .put("BigQueryTimestampAppend","2019-01-01 01:01:00T01:01")
            .put("LocalTimestampWithWrongOffsetToUtc","2019-01-01T12:00:00+00:00")
            .put("DatetimeToDate","2019-01-01 00:00:00")
            .put("LocalTimestampWithoutOptionalTToUtc","2019-01-01 12:00:00")
            .put("LocalTimestampWithOptionalTToUtc","2019-01-01T12:00:00");
            
 
        String testPayload = new JSONObject().put("NewImage",testPayloadObject).toString();
        byte[] payload = testPayload.getBytes(StandardCharsets.UTF_8);
        PubsubMessage pm = new PubsubMessage(payload, attributes);
        //END INPUT

        //BEGIN OUTPUT TABLEROW
        List<TableRow> attributes = new ArrayList<TableRow>();
        attributes.add(new TableRow().set("key", "source").set("value", "test"));
        attributes.add(new TableRow().set("key", "uuid").set("value", "123-456-abc"));
        attributes.add(new TableRow().set("key", "operation").set("value", "INSERT"));
        attributes.add(new TableRow().set("key", "timestamp").set("value", "2013-08-16T23:36:32.444Z"));

        TableRow assertTableRow = new TableRow()
            .set("StringAppendix", "a stringAppendix")
            .set("StringCoalesce", "a string")
            .set("DoubleField", 10d)
            .set("Int32Field", 10)
            .set("Int32Coalesce", 10)
            .set("BigQueryDatetimeNotFiltered","2019-01-01 00:00:00")
            .set("BigQueryTimestampNotFiltered","2019-01-01T00:00:00+02:00")
            .set("BigQueryTimeAppend","01:01:00")
            .set("BigQueryDatetimeAppend","2019-01-01 01:01:00")
            .set("BigQueryTimestampAppend","2019-01-01 01:01:00T01:01:00")
            .set("LocalTimestampWithWrongOffsetToUtc","2019-01-01T11:00:00Z")
            .set("DatetimeToDate","2019-01-01")
            .set("LocalTimestampWithoutOptionalTToUtc","2019-01-01T11:00:00Z")
            .set("LocalTimestampWithOptionalTToUtc","2019-01-01T11:00:00Z")
            .set("_ATTRIBUTES", attributes);
            
        //END OUTPUT TABLEROW
        
        //BEGIN OUTPUT TABLESCHEMA
        TableFieldSchema.Categories fieldCategories = new TableFieldSchema.Categories().setNames(null);
        
        TableSchema assertAttributesSchema = new TableSchema();
        List<TableFieldSchema> attributesFields = new ArrayList<TableFieldSchema>();
        attributesFields.add(new TableFieldSchema().setName("key").setType("STRING").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        attributesFields.add(new TableFieldSchema().setName("value").setType("STRING").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        assertAttributesSchema.setFields(attributesFields);

        TableSchema assertSchema = new TableSchema();
        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
        fields.add(new TableFieldSchema().setName("StringAppendix").setType("STRING").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("StringFilter").setType("STRING").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("StringCoalesce").setType("STRING").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("DoubleField").setType("FLOAT").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("Int32Field").setType("INTEGER").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("Int32Coalesce").setType("INTEGER").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("BigQueryDatetimeFiltered").setType("DATETIME").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("BigQueryTimestampFiltered").setType("TIMESTAMP").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("BigQueryDatetimeNotFiltered").setType("DATETIME").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("BigQueryTimestampNotFiltered").setType("TIMESTAMP").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("StringDefaultFalse").setType("STRING").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("Int32DefaultFalse").setType("INTEGER").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("Int64DefaultFalse").setType("INTEGER").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("DoubleDefaultFalse").setType("FLOAT").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("FloatDefaultFalse").setType("FLOAT").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("BoolDefaultFalse").setType("BOOLEAN").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("BytesDefaultFalse").setType("BYTES").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("EnumDefaultFalse").setType("INTEGER").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("BigQueryTimeAppend").setType("TIME").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("BigQueryDatetimeAppend").setType("DATETIME").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("BigQueryTimestampAppend").setType("TIMESTAMP").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("LocalTimestampWithWrongOffsetToUtc").setType("TIMESTAMP").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("DatetimeToDate").setType("DATE").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("LocalTimestampWithoutOptionalTToUtc").setType("TIMESTAMP").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("LocalTimestampWithOptionalTToUtc").setType("TIMESTAMP").setMode("NULLABLE").setDescription("").setCategories(fieldCategories));
        fields.add(new TableFieldSchema().setName("_ATTRIBUTES").setType("RECORD").setMode("REPEATED").setDescription("").setFields(assertAttributesSchema.getFields()));
        assertSchema.setFields(fields);
        
        //END OUTPUT TABLESCHEMA

        LOG.info("payload: " + testPayload);
        
        //Test Schema generation
        TableSchema eventSchema = null;
        String tableDescription = "";
        try{
            ProtoDescriptor protoDescriptor = ProtobufUtils.getProtoDescriptorFromCloudStorage("mathem-ml-datahem-test-descriptor", "testSchemas.desc");
            Descriptor descriptor = protoDescriptor.getDescriptorByName("datahem.test.TestWithOptions");
            eventSchema = ProtobufUtils.makeTableSchema(protoDescriptor, descriptor, ".*1234567890.*");
            LOG.info("eventSchema: " + eventSchema.toString());
            Assert.assertEquals(eventSchema, assertSchema);
        }catch (Exception e) {
            e.printStackTrace();
        }

        //Test GenericStreamPipeline
        try{
            PCollection<TableRow> output = p
                .apply(Create.of(Arrays.asList(pm)))
                .apply(ParDo.of(new DynamoDbStreamPipeline.PubsubMessageToTableRowFn(
                    StaticValueProvider.of("mathem-ml-datahem-test-descriptor"),
                    StaticValueProvider.of("testSchemas.desc"),
                    StaticValueProvider.of("datahem.test.TestWithOptions"))));
            PAssert.that(output).containsInAnyOrder(assertTableRow);
            p.run();
        }catch (Exception e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }

}