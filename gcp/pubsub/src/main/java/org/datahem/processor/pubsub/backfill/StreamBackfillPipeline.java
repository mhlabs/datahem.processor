
package org.datahem.processor.pubsub.backfill;

import org.datahem.processor.utils.BackupToByteArrayFn;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
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
  
  	public interface StreamBackfillPipelineOptions extends PipelineOptions{ 

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
			ParDo.of(new DoFn<TableRow,PubSubMessage>() {
	      		@ProcessElement
	      		public void processElement(ProcessContext c)  {
                    TableRow row = c.element();
			        byte[] payload = (byte[]) row.get("data");
                    HashMap<String, String> attributes = (HashMap<String, String>) row.get("attributes");
                    PubsubMessage pubSubMessage = PubsubMessage(payload, attributes); 
                    c.output(pubSubMessage);
         }}))
         .apply("Write to pubsub",
				PubsubIO
					.writeMessages()
					.to(options.getPubsubTopic()));
    pipeline.run();
  }
}
