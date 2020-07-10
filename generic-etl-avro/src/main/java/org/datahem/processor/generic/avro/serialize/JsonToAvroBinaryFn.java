package org.datahem.processor.generic;

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
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.datahem.avro.message.Converters;
import org.datahem.avro.message.DatastoreCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

//import org.apache.beam.sdk.options.ValueProvider;


public class JsonToAvroBinaryFn extends DoFn<PubsubMessage, PubsubMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonToAvroBinaryFn.class);
    private DatastoreCache cache;

    @Setup
    public void setup() throws Exception {
        cache = new DatastoreCache();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        PubsubMessage received = c.element();
        //String fingerprint = received.getAttribute("fingerprint");
        //Schema schema = cache.findByFingerprint(Long.parseLong(fingerprint));
        try {
            String json = new String(received.getPayload(), StandardCharsets.UTF_8);
            byte[] payload = Converters.jsonToAvroBinary(json, cache.findByFingerprint(Long.parseLong(received.getAttribute("fingerprint"))));
            Map<String, String> attributes =
                    ImmutableMap.<String, String>builder()
                            .put("timestamp", received.getAttribute("timestamp"))
                            .put("fingerprint", received.getAttribute("fingerprint"))
                            .put("uuid", received.getAttribute("uuid"))
                            .build();

            PubsubMessage pubsubMessage = new PubsubMessage(payload, attributes);
            c.output(pubsubMessage);
        } catch (Exception e) {
            LOG.error(new String(received.getPayload(), StandardCharsets.UTF_8));
            LOG.error(e.toString());
        }
    }
}
