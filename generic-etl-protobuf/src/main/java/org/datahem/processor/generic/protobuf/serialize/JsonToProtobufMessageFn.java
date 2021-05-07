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


import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

public class JsonToProtobufMessageFn extends DoFn<PubsubMessage, Message> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonToProtobufMessageFn.class);
    //private Message.Builder builder = null;
    private String protoJavaFullName;

    public JsonToProtobufMessageFn(String protoJavaFullName) {
        this.protoJavaFullName = protoJavaFullName;
			/*try{
			Class<?> clazz = Class.forName(protoJavaFullName);
			Method newBuilderMethod = clazz.getMethod("newBuilder");
			Message.Builder builder = (Message.Builder) newBuilderMethod.invoke(null);
			this.builder = builder;
			}catch(Exception e){}*/
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        PubsubMessage received = c.element();
        try {
            Class<?> clazz = Class.forName(protoJavaFullName);
            Method newBuilderMethod = clazz.getMethod("newBuilder");
            Message.Builder builder = (Message.Builder) newBuilderMethod.invoke(null);
            String json = new String(received.getPayload(), StandardCharsets.UTF_8);
            JsonFormat.parser().ignoringUnknownFields().merge(json, builder);

            Descriptors.FieldDescriptor messageUuid = builder.getDescriptorForType().findFieldByName("MessageUuid");
            if (messageUuid != null) {
                builder.setField(messageUuid, received.getAttribute("MessageUuid"));
            }
            Descriptors.FieldDescriptor messageTimestamp = builder.getDescriptorForType().findFieldByName("MessageTimestamp");
            if (messageTimestamp != null) {
                builder.setField(messageTimestamp, received.getAttribute("MessageTimestamp"));
            }
            Message message = builder.build();
            c.output(message);
        } catch (Exception e) {
            LOG.error(new String(received.getPayload(), StandardCharsets.UTF_8));
            LOG.error(e.toString());
        }
    }
}
