package org.datahem.processor.measurementprotocol.v2;

/*-
 * ========================LICENSE_START=================================
 * Datahem.processor.measurementprotocol
 * %%
 * Copyright (C) 2018 - 2019 Robert Sahlin
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */

import org.datahem.processor.utils.ProtobufUtils;
import org.datahem.processor.measurementprotocol.v2.utils.PayloadToMeasurementProtocolFn;
import org.datahem.processor.measurementprotocol.v2.utils.MeasurementProtocolToTableRowFn;
import org.datahem.protobuf.measurementprotocol.v2.*;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TimePartitioning;

import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;

import org.joda.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeasurementProtocolPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(MeasurementProtocolPipeline.class);

  public interface MeasurementProtocolPipelineOptions extends PipelineOptions{
	@Description("JSON Configuration string")
	ValueProvider<String> getConfig();
	void setConfig(ValueProvider<String> value);
  }

  public static void main(String[] args) {
    MeasurementProtocolPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MeasurementProtocolPipeline.MeasurementProtocolPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    // Create schemas from protocol buffers
    
        TableSchema eventSchema = ProtobufUtils.makeTableSchema(MeasurementProtocol.getDescriptor());
    	List<TableFieldSchema> fieldsList = eventSchema.getFields();

        //change date partition column from string to date type
        TableFieldSchema partition = new TableFieldSchema().setName("date").setType("STRING").setMode("NULLABLE");
    	fieldsList.set(fieldsList.indexOf(partition), partition.setType("DATE"));

        //Change date and time related dimensions from string to datetime, date and time types
        TableFieldSchema timeMessage = new TableFieldSchema().setName("time").setType("RECORD").setMode("NULLABLE");
        List<TableFieldSchema> timeMessageList = fieldsList.get(fieldsList.indexOf(timeMessage)).getFields();
        TableFieldSchema dateTime = new TableFieldSchema().setName("dateTime").setType("STRING").setMode("NULLABLE");
    	timeMessageList.set(timeMessageList.indexOf(dateTime), dateTime.setType("DATETIME"));
        TableFieldSchema time = new TableFieldSchema().setName("time").setType("STRING").setMode("NULLABLE");
    	timeMessageList.set(timeMessageList.indexOf(time), time.setType("TIME"));
        TableFieldSchema date = new TableFieldSchema().setName("date").setType("STRING").setMode("NULLABLE");
    	timeMessageList.set(timeMessageList.indexOf(date), date.setType("DATE"));
        fieldsList.set(fieldsList.indexOf(timeMessage), timeMessage.setFields(timeMessageList));
        /*
        fieldsList.set(fieldsList.indexOf(timeMessage), timeMessage.setFields(Arrays.asList(
                new TableFieldSchema().setName("dateTime").setType("DATETIME"),
                new TableFieldSchema().setName("time").setType("TIME"),
                new TableFieldSchema().setName("date").setType("DATE"),
                new TableFieldSchema().setName("timeZone").setType("STRING"),
          ))
        );*/
    	
    	TableSchema schema = new TableSchema().setFields(fieldsList);
    
	
	for (Config.Account.Property property : Config.read(options.getConfig().get())) { //Start account
			String pubsubSubscription = "projects/" + options.as(GcpOptions.class).getProject() + "/subscriptions/" + property.id;
			LOG.info("pubsibSubscription: " + pubsubSubscription);

    PCollection<PubsubMessage> payload = pipeline
    	.apply(property.id + " - Read payloads from pubsub", 
    		PubsubIO
    			.readMessagesWithAttributes()
    			.fromSubscription(pubsubSubscription));
    
        for(Config.Account.Property.View view : property.views){ //Start view
            String table = (view.tableSpec != null ? view.tableSpec : property.id + "." + view.id);
            PCollection<MeasurementProtocol> enrichedEntities = payload
                .apply(view.id + " - Payload to MeasurementProtocol", 
                    ParDo.of(new PayloadToMeasurementProtocolFn(
                        StaticValueProvider.of(view.searchEnginesPattern),
                        StaticValueProvider.of(view.ignoredReferersPattern), 
                        StaticValueProvider.of(view.socialNetworksPattern),
                        StaticValueProvider.of(view.includedHostnamesPattern),
                        StaticValueProvider.of(view.excludedBotsPattern),
                        StaticValueProvider.of(view.siteSearchPattern),
                        StaticValueProvider.of(view.timeZone)
                    )));
            
            enrichedEntities
                .apply(view.id + " - Event to tablerow", 
                    ParDo.of(new MeasurementProtocolToTableRowFn()))
                .apply(view.id + " - Fixed Windows",
                    Window.<TableRow>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .withAllowedLateness(Duration.standardDays(7))
                    .discardingFiredPanes())
                .apply(view.id + " - Write to bigquery", 
                    BigQueryIO
                        .writeTableRows()
                        .to(table)
                        .withSchema(schema)
                        .withTimePartitioning(new TimePartitioning().setField("date").setType("DAY"))
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
            
            if(view.pubSubTopic != null){
                String pubSubTopic = "projects/" + options.as(GcpOptions.class).getProject() + "/topics/" + view.pubSubTopic;
                enrichedEntities
                    .apply(view.id + " - Write to pubsub",
                        PubsubIO
                            .writeProtos(MeasurementProtocol.class)
                            .to(pubSubTopic));
            }
        } //End View
    } //End account
    pipeline.run();
  }
}
