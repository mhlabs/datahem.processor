package org.datahem.processor.pubsub.backup;

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

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableReference;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

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

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubBackupPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(PubSubBackupPipeline.class);

    //private interface Options extends PipelineOptions {}
    
    public interface Options extends PipelineOptions, GcpOptions {

	@Description("BigQuery Table Spec [project_id]:[dataset_id].[table_id] or [dataset_id].[table_id]")
    @Default.String("backup.events")
    ValueProvider<String> getBigQueryTableSpec();
    void setBigQueryTableSpec(ValueProvider<String> value);
  	
  	@Description("Pub/Sub subscription")
	@Default.String("projects/mathem-data/subscriptions/measurementprotocol-1-dev")
	ValueProvider<String> getPubsubSubscription();
	void setPubsubSubscription(ValueProvider<String> subscription);
  
  
  }

    public static void main(String[] args) throws IOException {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);
		
		//Attributes tablefield
		List<TableFieldSchema> attributeFieldSchemaList = new ArrayList<>();
		attributeFieldSchemaList.add(new TableFieldSchema().setName("key").setType("STRING"));
		attributeFieldSchemaList.add(new TableFieldSchema().setName("value").setType("STRING"));
		
		//TableFields
		List<TableFieldSchema> fieldsSchemaList = new ArrayList<>();
	    fieldsSchemaList.add(new TableFieldSchema().setName("publish_time").setType("TIMESTAMP").setMode("REQUIRED"));
	    fieldsSchemaList.add(new TableFieldSchema().setName("attributes").setType("RECORD").setMode("REPEATED").setFields(attributeFieldSchemaList));
	    fieldsSchemaList.add(new TableFieldSchema().setName("data").setType("BYTES").setMode("REQUIRED"));
		
	    TableSchema schema = new TableSchema().setFields(fieldsSchemaList);
			
    	pipeline
    	.apply("Read from pubsub",
    		PubsubIO
    			.readMessagesWithAttributes()
    			.fromSubscription(options.getPubsubSubscription())
    	)
		.apply("ConvertDataToTableRows", ParDo.of(new DoFn<PubsubMessage, TableRow>() {
			@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
	        	PubsubMessage pubsubMessage = c.element();
				
				Map<String, String> attributeMap = pubsubMessage.getAttributeMap();
	        	List<TableRow> attributes = new ArrayList<>();
	        	if (attributeMap != null) {
	        		attributeMap.forEach((k,v)-> {
	        			//LOG.info("key: " + k + ", value: " + v);
	        			attributes.add(new TableRow().set("key",k).set("value", v));
        			});

				}
	        	
	        	DateTimeFormatter partition = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZoneUTC();
	        	//LOG.info("data: " + pubsubMessage.getPayload());
				//LOG.info("publish_time: " + c.timestamp());
	        	TableRow tableRow = new TableRow()
	        		.set("publish_time", c.timestamp().toString(partition))
	        		.set("attributes", attributes)
	        		.set("data", pubsubMessage.getPayload());
	        	
	        	c.output(tableRow);
    		}
		}))
    	.apply("InsertTableRowsToBigQuery",
      		BigQueryIO
				.writeTableRows()
				//.to(options.getBigQueryTableSpec())
				.to(NestedValueProvider.of(
					options.getBigQueryTableSpec(),
					new SerializableFunction<String, String>() {
						@Override
						public String apply(String tableSpec) {
							return tableSpec.replaceAll("[^A-Za-z0-9]", "");
						}
					}))
      			.withSchema(schema)
      			.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
      			.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    	// Run the pipeline
    	pipeline.run();//.waitUntilFinish();

    }
}
