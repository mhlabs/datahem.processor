package org.datahem.processor.pubsub.backup;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 Robert Sahlin and MatHem Sverige AB
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableReference;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.Clustering;

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
import com.google.api.services.bigquery.model.TimePartitioning;
//import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;

public class PubSubBackupPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(PubSubBackupPipeline.class);

    
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
        fieldsSchemaList.add(new TableFieldSchema().setName("topic").setType("STRING"));
	    fieldsSchemaList.add(new TableFieldSchema().setName("attributes").setType("RECORD").setMode("REPEATED").setFields(attributeFieldSchemaList));
	    fieldsSchemaList.add(new TableFieldSchema().setName("data").setType("BYTES").setMode("REQUIRED"));
		
	    TableSchema schema = new TableSchema().setFields(fieldsSchemaList);
	    TimePartitioning partition = new TimePartitioning().setField("publish_time");

        Clustering cluster = new Clustering();
        cluster.setFields(Arrays.asList("topic"));
			
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
	        			attributes.add(new TableRow().set("key",k).set("value", v));
        			});
				}
	        	DateTimeFormatter partition = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZoneUTC();
	        	TableRow tableRow = new TableRow()
	        		.set("publish_time", c.timestamp().toString(partition))
	        		.set("topic", attributeMap.get("topic"))
                    .set("attributes", attributes)
	        		.set("data", pubsubMessage.getPayload());
	        	
	        	c.output(tableRow);
    		}
		}))
    	.apply("InsertTableRowsToBigQuery",
      		BigQueryIO
				.<TableRow>write()
				.to(NestedValueProvider.of(
					options.getBigQueryTableSpec(),
					new SerializableFunction<String, String>() {
						@Override
						public String apply(String tableSpec) {
							return tableSpec.replaceAll("[^A-Za-z0-9.]", "");
						}
				}))
				.withFormatFunction(tr -> tr)
      			.withSchema(schema)
      			.withTimePartitioning(partition)
                .withClustering(cluster)
      			.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
      			.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    	// Run the pipeline
    	pipeline.run();

    }
}
