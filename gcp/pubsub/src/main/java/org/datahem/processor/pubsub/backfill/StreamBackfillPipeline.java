
package org.datahem.processor.pubsub.backfill;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 - 2019 Robert Sahlin and MatHem Sverige AB
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

//import org.datahem.processor.utils.BackupToByteArrayFn;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
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
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.joda.time.Duration;
import org.joda.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.IOException;
import java.util.Base64;
import java.util.Arrays;

public class StreamBackfillPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(StreamBackfillPipeline.class);
  
  	public interface StreamBackfillPipelineOptions extends DataflowPipelineOptions{ 

        @Description("Pub/Sub topic")
	    String getPubsubTopic();
	    void setPubsubTopic(String topic);
	
	  	@Description("BigQuery query")
	  	String getQuery();
	  	void setQuery(String query);
  }
  

  public static void main(String[] args) {
    StreamBackfillPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamBackfillPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    
    pipeline
    	.apply("BigQuery SELECT job",
    		BigQueryIO
    			.readTableRows()
    			//.withTemplateCompatibility()
    			.fromQuery(options.getQuery())
    			.usingStandardSql())
    			//.withoutValidation())
    	.apply("TableRow to PubSubMessage", 
			ParDo.of(new DoFn<TableRow,PubsubMessage>() {
	      		@ProcessElement
	      		public void processElement(ProcessContext c)  {
                    TableRow row = c.element();
                    String b = (String) row.get("data");
			        byte[] payload = b.getBytes();
                    List<TableRow> repeated = (List<TableRow>) row.get("attributes");
                    HashMap<String, String> attributes = repeated
                        .stream()
                        .collect(HashMap::new, (map,record)-> map.put((String) record.get("key"), (String) record.get("value")), HashMap::putAll);
                    PubsubMessage pubSubMessage = new PubsubMessage(payload, attributes); 
                    c.output(pubSubMessage);
         }}))
         .apply("Write to pubsub",
				PubsubIO
					.writeMessages()
					.to(options.getPubsubTopic()));
    pipeline.run();
  }
}