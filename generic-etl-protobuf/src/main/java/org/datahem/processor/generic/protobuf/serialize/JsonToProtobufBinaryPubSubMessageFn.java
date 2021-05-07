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


import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class JsonToProtobufBinaryPubSubMessageFn extends DoFn<PubsubMessage, PubsubMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonToProtobufBinaryPubSubMessageFn.class);


    private Map<String, Config.StreamConfig> streamConfigLookup = new HashMap<String, Config.StreamConfig>();

    public JsonToProtobufBinaryPubSubMessageFn(Map<String, Config.StreamConfig> streamConfigLookup) {
        this.streamConfigLookup = streamConfigLookup;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        PubsubMessage received = c.element();
        String protobufClassName = streamConfigLookup.get(received.getAttribute("stream")).getProtoJavaFullName();

        try {
            String json = new String(received.getPayload(), StandardCharsets.UTF_8);
            // Use reflection to create and serialize protobuf message
            Class<?> clazz = Class.forName(protobufClassName);

            //Class<?> clazz = Class.forName(protoJavaFullName);
            Method newBuilderMethod = clazz.getMethod("newBuilder");
            Message.Builder builder = (Message.Builder) newBuilderMethod.invoke(null);

            JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
            Message message = builder.build();
            byte[] payload = message.toByteArray();
            Map<String, String> attributes =
                    ImmutableMap.<String, String>builder()
                            .putAll(received.getAttributeMap())
                            .put("protobufClassName", protobufClassName)
                            //.put("protoJavaFullName", protoJavaFullName)
                            .build();
            //Create PubSubMessage with the serialized message as payload
            PubsubMessage pubsubMessage = new PubsubMessage(payload, attributes);
            c.output(pubsubMessage);
        } catch (Exception e) {
            LOG.error(new String(received.getPayload(), StandardCharsets.UTF_8));
            LOG.error(e.toString());
        }
    }
}
