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


import io.anemos.metastore.core.proto.*;
//import org.datahem.processor.anonymize.AnonymizeStreamPipeline.*;
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
import java.util.Iterator;

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

import com.google.auto.value.AutoValue;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;

@RunWith(JUnit4.class)
public class AnonymizePipelineTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(AnonymizePipelineTest.class);
	
	@Rule public transient TestPipeline p = TestPipeline.create();

    public static class ConvertToDLPRow extends DoFn<String, Table.Row> {
        @ProcessElement
        public void processElement(@Element String inputJson, OutputReceiver<Table.Row> out) {
         LOG.info("inputJson: " + inputJson);
        JSONObject jso = new JSONObject(inputJson);
        Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
        Iterator<String> row = jso.keys();
        while(row.hasNext()){
            tableRowBuilder.addValues(Value.newBuilder().setStringValue(jso.getString(row.next())));
            //tableRowBuilder.addValues(Value.newBuilder().setStringValue("hello"));
        }
        //tableRowBuilder.addValues(Value.newBuilder().setStringValue("hello"));
        Table.Row dlpRow = tableRowBuilder.build();
        LOG.info("DLPRow {}", dlpRow.toString());
        out.output(dlpRow);
        }
    }


    public static class DLPTokenizationDoFn extends DoFn<KV<String, Iterable<Table.Row>>, Table.Row> {
        private DlpServiceClient dlpServiceClient;
        private boolean inspectTemplateExist;
        private String dlpProjectId;
        private String deIdentifyTemplateName;
        private String inspectTemplateName;
        private DeidentifyContentRequest.Builder requestBuilder;

        public DLPTokenizationDoFn(
            String dlpProjectId, 
            String deIdentifyTemplateName, 
            String inspectTemplateName) {
        this.dlpProjectId = dlpProjectId;
        this.dlpServiceClient = null;
        this.deIdentifyTemplateName = deIdentifyTemplateName;
        this.inspectTemplateName = inspectTemplateName;
        this.inspectTemplateExist = false;
        }

        @Setup
        public void setup() {
            if (this.inspectTemplateName != null) {
                this.inspectTemplateExist = true;
            }
            if (this.deIdentifyTemplateName != null) {
                this.requestBuilder =
                    DeidentifyContentRequest.newBuilder()
                        .setParent(ProjectName.of(this.dlpProjectId).toString())
                        .setDeidentifyTemplateName(this.deIdentifyTemplateName);
                if (this.inspectTemplateExist) {
                this.requestBuilder.setInspectTemplateName(this.inspectTemplateName);
                }
            }
        }

        @StartBundle
        public void startBundle() throws SQLException {

            try {
                this.dlpServiceClient = DlpServiceClient.create();

            } catch (IOException e) {
                LOG.error("Failed to create DLP Service Client", e.getMessage());
                throw new RuntimeException(e);
            }
        }

        @FinishBundle
        public void finishBundle() throws Exception {
            if (this.dlpServiceClient != null) {
                this.dlpServiceClient.close();
            }
        }

        @ProcessElement
        public void processElement(ProcessContext c) {

            List<FieldId> dlpTableHeaders =
                Util.bqLogSchema.getFieldNames().stream()
                    .map(header -> FieldId.newBuilder().setName(header).build())
                    .collect(Collectors.toList());

            List<Table.Row> rows = new ArrayList<>();
            c.element().getValue().forEach(rows::add);
            Table dlpTable = Table.newBuilder().addAllHeaders(dlpTableHeaders).addAllRows(rows).build();
            ContentItem tableItem = ContentItem.newBuilder().setTable(dlpTable).build();
            this.requestBuilder.setItem(tableItem);
            DeidentifyContentResponse response =
                dlpServiceClient.deidentifyContent(this.requestBuilder.build());
            Table tokenizedData = response.getItem().getTable();
            List<Table.Row> outputRows = tokenizedData.getRowsList();
            outputRows.forEach(
                row -> {
                    LOG.debug("Tokenized Row {}", row);
                    c.output(row);
                });
        }
  }


	@Test
	public void withoutOptionsTest(){

        String testPayload = new JSONObject()
            .put("email", "foo@bar.com").toString();

        //String testPayload = new JSONObject().put("NewImage",testPayloadObject).toString();
        
        try{
            LOG.info("ok ");
            PCollection<Table.Row> output = p
                .apply(Create.of(Arrays.asList(testPayload)))
                .apply("Transform json to DlpRow",ParDo.of(new ConvertToDLPRow()))
                .apply("With Keys", WithKeys.of(UUID.randomUUID().toString()))
                .apply("Group Into Batches", GroupIntoBatches.<String, Table.Row>ofSize(batchSize()))
                .setCoder(
                    KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(ProtoCoder.of(Table.Row.class))))
                .apply("DLP Tokenization",
                    ParDo.of(new DLPTokenizationDoFn(projectId(), deidTemplateName(), inspectTemplateName())));

            //PAssert.that(output).containsInAnyOrder(assertTableRow);
            p.run();
            LOG.info("withoutOptionsTest assert TableRow without errors.");
        }catch (Exception e) {
            LOG.info("error");
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
	public void cleanPubsubMessageTest(){

        String json = new JSONObject().put("email", "foo@bar.com").toString();
                
                ByteString bs = ByteString.copyFromUtf8(json);
                byte[] jsonPayload = bs.toByteArray();

				PubsubMessage newPubsubMessage = new PubsubMessage(jsonPayload, attributes);

        //String testPayload = 
        
        try{
            LOG.info("ok ");
            PCollection<Table.Row> output = p
                .apply(Create.of(Arrays.asList(testPubsubMessage)))
                //.apply("Transform json to PubsubMessage",ParDo.of(new ConvertToDLPRow()))
                .apply("PubsubMessage to TableRow", ParDo.of(new CleanPubsubMessageFn(
                    options.getBucketName(),
                    options.getFileDescriptorName(),
                    options.getDescriptorFullName()
                )));

            //PAssert.that(output).containsInAnyOrder(assertTableRow);
            p.run();
            LOG.info("withoutOptionsTest assert TableRow without errors.");
        }catch (Exception e) {
            LOG.info("error");
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
}