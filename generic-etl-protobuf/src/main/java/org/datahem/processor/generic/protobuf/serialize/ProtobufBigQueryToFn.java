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

import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.google.api.services.bigquery.model.TableRow;
//import org.datahem.processor.generic.protobuf.utils.ProtobufUtils;


public class ProtobufBigQueryToFn implements SerializableFunction<ValueInSingleWindow<Message>, TableDestination> {
    private static final Logger LOG = LoggerFactory.getLogger(ProtobufBigQueryToFn.class);
    private String dataset;
    private String tableField;
    private String partitionField;

    public ProtobufBigQueryToFn(String dataset, String tableField, String partitionField) {
        this.dataset = dataset;
        this.tableField = tableField;
        this.partitionField = partitionField;
    }

    public ProtobufBigQueryToFn(String dataset, String tableField) {
        this(dataset, tableField, "");
    }

    public TableDestination apply(ValueInSingleWindow<Message> element) {
        Message message = element.getValue();
        Descriptors.FieldDescriptor field = message.getDescriptorForType().findFieldByName(tableField);
        String messageField = String.valueOf(message.getField(field));
        String table = (messageField != null) ? messageField : tableField;
        String destination = (dataset + "." + table).replaceAll("[^A-Za-z0-9.]", "");
        TimePartitioning partition = new TimePartitioning();
        if (partitionField != null && partitionField != "") {
            partition.setField(partitionField);
        }
        return new TableDestination(dataset + "." + table, "Table for:" + table, partition);
    }
}