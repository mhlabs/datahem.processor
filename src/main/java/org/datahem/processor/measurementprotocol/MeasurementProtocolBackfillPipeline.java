package org.datahem.processor.measurementprotocol;

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

import org.datahem.protobuf.collector.v1.CollectorPayloadEntityProto.*;
import org.datahem.protobuf.collector.v1.CollectorPayloadEntityProto;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto;
import org.datahem.processor.measurementprotocol.utils.MeasurementProtocolBuilder;
import org.datahem.processor.measurementprotocol.utils.PayloadToMPEntityFn;
import org.datahem.processor.measurementprotocol.utils.MPEntityToTableRowFn;
import org.datahem.processor.measurementprotocol.utils.SchemaHelper;
import org.datahem.processor.measurementprotocol.utils.MeasurementProtocolOptions;
import org.datahem.processor.utils.ProtobufUtils;
import org.datahem.processor.pubsub.backup.utils.BackupToByteArrayFn;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
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

public class MeasurementProtocolBackfillPipeline {
	
	private static final Logger LOG = LoggerFactory.getLogger(MeasurementProtocolBackfillPipeline.class);
  
  	public interface MeasurementProtocolBackfillPipelineOptions extends MeasurementProtocolOptions{ 

		@Description("BigQuery Table Spec [project_id]:[dataset_id].[table_id] or [dataset_id].[table_id]")
	    @Default.String("ga.events")
	    ValueProvider<String> getBigQueryTableSpec();
	    void setBigQueryTableSpec(ValueProvider<String> value);
	
	  	@Description("BigQuery query")
	  	@Default.String("SELECT data FROM `mathem-data.robban.backup` LIMIT 100")
	  	ValueProvider<String> getQuery();
	  	void setQuery(ValueProvider<String> value);
  }
  
  /*
   mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.measurement.protocol.MeasurementProtocolBackfillPipeline \
      -Dexec.args=" \
      --project=mathem-data \
      --stagingLocation=gs://mathem-data/datahem/0.2/org/datahem/processor/staging \
      --gcpTempLocation=gs://mathem-data/datahem/gcptemp/ \
      --tempLocation=gs://mathem-data/datahem/temp/ \
      --runner=DataflowRunner \
      --zone=europe-west1-b \
      --region=europe-west1 \
      --numWorkers=1 \
      --maxNumWorkers=1 \
      --diskSizeGb=5 \
      --workerMachineType=n1-standard-1 \
      --pubsubTopic=projects/mathem-data/topics/test \
      --pubsubSubscription=projects/mathem-data/subscriptions/measurementprotocol-1-dev \
      --bigQueryTableSpec=robban.ga4"

mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.measurement.protocol.MeasurementProtocolBackfillPipeline \
      -Dexec.args=" \
      --project=mathem-data \
      --stagingLocation=gs://mathem-data/datahem/0.2/org/datahem/processor/staging \
      --gcpTempLocation=gs://mathem-data/datahem/gcptemp/ \
      --tempLocation=gs://mathem-data/datahem/temp/ \
      --runner=DataflowRunner \
      --zone=europe-west1-b \
      --region=europe-west1 \
      --bigQueryTableSpec=robban.ga4"


//Create template
 mvn compile exec:java \
     -Dexec.mainClass=org.datahem.processor.measurement.protocol.MeasurementProtocolBackfillPipeline \
     -Dexec.args="--runner=DataflowRunner \
                  --project=mathem-data \
                  --stagingLocation=gs://mathem-data/datahem/0.2/org/datahem/processor/measurement/protocol/staging/ \
                  --templateLocation=gs://mathem-data/datahem/0.2/org/datahem/processor/measurement/protocol/MeasurementProtocolPipeline \
                  --workerMachineType=n1-standard-1 \
                  --diskSizeGb=30"


gcloud beta dataflow jobs run hoppla5 \
        --gcs-location gs://mathem-data/datahem/0.2/org/datahem/processor/measurement/protocol/MeasurementProtocolPipeline \
        --zone=europe-west1-b \
        --max-workers=1 \
        --parameters pubsubSubscription=projects/mathem-data/subscriptions/measurementprotocol-1-dev,pubsubTopic=projects/mathem-data/topics/test,bigQueryTableSpec=robban.ga4,searchEnginesPattern=".*www\\.google\\..*|.*www\\.bing\\..*|.*search\\.yahoo\\..*"
*/



  public static void main(String[] args) {
    MeasurementProtocolBackfillPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MeasurementProtocolBackfillPipeline.MeasurementProtocolBackfillPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    
   	TableSchema schema = SchemaHelper.mpEntityBigQuerySchema();
    
    pipeline
    	.apply("BigQuery SELECT job",
    		BigQueryIO
    			.readTableRows()
    			.withTemplateCompatibility()
    			.fromQuery(options.getQuery())
    			.usingStandardSql()
    			.withoutValidation())
	    .apply("Backup to decoded byte array", 
    		ParDo.of(new BackupToByteArrayFn()))
    	.apply("Byte array to CollectorPayload", 
			ParDo.of(new DoFn<byte[], CollectorPayloadEntity>() {
	      		@ProcessElement
	      		public void processElement(ProcessContext c)  {
	      			byte[] decoded = c.element();
					try{
					    CollectorPayloadEntity cp = CollectorPayloadEntity.parseFrom(decoded);
					    c.output(cp);
					}
				    catch ( IOException e) {
						LOG.error(e.toString());
					}
	     			return;}}))	
	     .apply("Collector Payload to multiple Events", 
    		ParDo.of(new PayloadToMPEntityFn(
    			options.getSearchEnginesPattern(),
    			options.getIgnoredReferersPattern(), 
    			options.getSocialNetworksPattern(),
    			options.getIncludedHostnamesPattern(),
    			options.getExcludedBotsPattern(),
    			options.getSiteSearchPattern(),
    			options.getTimeZone())))
    	.apply("Event to tablerow", 
    		ParDo.of(new MPEntityToTableRowFn()))
		.apply("Write to bigquery", 
			BigQueryIO
				.writeTableRows()
				.to(options.getBigQueryTableSpec())
				.withSchema(schema)
        		.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            	.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    
    pipeline.run();
  }
}
