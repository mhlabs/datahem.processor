package org.datahem.processor.generic.protobuf.serialize;

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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SerializePubSubMessageStreamPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(SerializePubSubMessageStreamPipeline.class);

    public interface Options extends PipelineOptions, GcpOptions {
        /*
         * Configure mapping between stream name (pubsub message attribute set by the extract job) and the protobufclassname to be used for serialization of the JSON.
         * --config='[{"streamName":"collector", "protoJavaClassName":"org.datahem.protobuf.collector.v1", "protoJavaOuterClassName":"CollectorPayloadEntityProto", "protoJavaClassName":"CollectorPayloadEntity"}]'
         */

        @Description("JSON Configuration string")
        String getConfig();

        void setConfig(String value);

        /*
        @Description("Protobuf Java package")
        ValueProvider<String> getProtoJavaPackage();
        void setProtoJavaPackage(ValueProvider<String> value);

        @Description("Protobuf Java Outer Class Name")
        ValueProvider<String> getProtoJavaOuterClassName();
        void setProtoJavaOuterClassName(ValueProvider<String> value);

        @Description("Protobuf Java Class Name")
        ValueProvider<String> getProtoJavaClassName();
        void setProtoJavaClassName(ValueProvider<String> value);
    */
        @Description("Pub/Sub topic")
        ValueProvider<String> getPubsubTopic();

        void setPubsubTopic(ValueProvider<String> value);

        @Description("Pub/Sub subscription")
        ValueProvider<String> getPubsubSubscription();

        void setPubsubSubscription(ValueProvider<String> subscription);
    }

    public static void main(String[] args) throws IOException {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        Map<String, Config.StreamConfig> streamConfigLookup = new HashMap<String, Config.StreamConfig>();
        for (Config.StreamConfig streamConfig : Config.read(options.getConfig())) {
            //streamConfigLookup.put(streamConfig.streamName, streamConfig.getProtoJavaFullName());
            streamConfigLookup.put(streamConfig.streamName, streamConfig);
        }

        pipeline
                .apply("Read pubsub messages",
                        PubsubIO
                                .readMessagesWithAttributes()
                                .fromSubscription(options.getPubsubSubscription()))
                .apply("Convert payload from Json to Protobuf Binary",
                        ParDo.of(new JsonToProtobufBinaryPubSubMessageFn(streamConfigLookup)))
                //ParDo.of(new JsonToProtobufBinaryFn(builder)))
                .apply("Fixed Windows",
                        Window.<PubsubMessage>into(FixedWindows.of(Duration.standardMinutes(1)))
                                .withAllowedLateness(Duration.standardDays(7))
                                .discardingFiredPanes())
                .apply("Write to pubsub",
                        PubsubIO
                                .writeMessages()
                                .withIdAttribute("uuid")
                                .withTimestampAttribute("timestamp")
                                .to(options.getPubsubTopic())
                );

        pipeline.run();
    }
}
