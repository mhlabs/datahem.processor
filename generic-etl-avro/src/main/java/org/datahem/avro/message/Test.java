package org.datahem.avro.message;

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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;

import java.io.IOException;

public class Test {
    //private static DatastoreCache cache = new DatastoreCache();

    private static final String JSON_V1 = "{ \"name\":\"Frank\", \"age\":47}";
    private static final String JSON_V2 = "{\"name\":\"Frank\", \"age\":47, \"gender\":\"male\"}";
    private static final String JSON_V3 = "{\"children\":[{\"name\":\"bill\"},{\"name\":\"bull\"}]}";
    private static final String JSON_V4 = "{\"Id\":\"12345\", \"MemberId\":\"abcdef\", \"Store\":{\"Id\":\"10\", \"StoreName\":\"Stockholm\"}}";
    private static final String JSON_V5 = "{\"Id\":\"1\"}";
    private static final String JSON_V6 = "{\"S\": {\"b\":1}}";
    private static final String SCHEMA_STR_V1 = "{\"type\":\"record\", \"namespace\":\"foo\", \"name\":\"Man\", \"fields\":[ { \"name\":\"name\", \"type\":\"string\" }, { \"name\":\"age\", \"type\":[\"null\",\"double\"] } ] }";
    private static final String SCHEMA_STR_V2 = "{\"type\":\"record\", \"namespace\":\"foo\", \"name\":\"Man\", \"fields\":[ { \"name\":\"name\", \"type\":\"string\" }, { \"name\":\"age\", \"type\":\"int\" }, { \"name\":\"gender\", \"type\":\"string\" } ] }";
    private static final String SCHEMA_STR_V3 = "{ \"name\": \"Parent\", \"type\":\"record\", \"fields\":[{\"name\":\"children\", \"type\":{ \"type\": \"array\",  \"items\":{\"name\":\"Child\", \"type\":\"record\", \"fields\":[{\"name\":\"name\", \"type\":\"string\"}]}}}]}";
    //private static final String SCHEMA_STR_V4 = "[{\"type\": \"record\",\"name\": \"Store\",\"namespace\": \"com.datahem.avro.order\",\"doc\": \"details about store\", \"fields\":[{\"name\": \"Id\", \"doc\": \"store id\", \"type\": \"string\"},{\"name\": \"StoreName\", \"doc\": \"Name of store\", \"type\": \"string\"}]},{\"type\": \"record\",\"name\": \"Order\",\"namespace\": \"com.datahem.avro.order\",\"doc\": \"An order entity published on kinesis\",\"fields\": [{\"name\": \"Id\", \"type\": \"string\", \"doc\": \"order id\"},{\"name\": \"MemberId\", \"type\": \"string\", \"doc\": \"member id\"},{\"name\": \"Store\", \"type\": \"com.datahem.avro.order.Store\", \"doc\": \"details about store\"}]}]";
    private static final String SCHEMA_STR_V4 = "{\"type\": \"record\",\"name\": \"Order\",\"namespace\": \"com.datahem.avro.order\",\"doc\": \"An order entity published on kinesis\",\"fields\": [{\"name\": \"Id\", \"type\": [\"null\", \"string\"], \"default\":null, \"doc\": \"order id\"},{\"name\": \"MemberId\", \"type\": \"string\", \"doc\": \"member id\"},{\"name\": \"Store\", \"type\":{\"type\": \"record\",\"name\": \"Store\",\"namespace\": \"com.datahem.avro.order\",\"doc\": \"details about store\",\"fields\":[{\"name\": \"Id\", \"doc\": \"store id\", \"type\": \"string\"},{\"name\": \"StoreName\", \"doc\": \"Name of store\", \"type\": \"string\"}]}}]}";
    private static final String SCHEMA_STR_V5 = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"type\":\"string\",\"name\":\"Id\"}, {\"type\":[\"null\",\"string\"],\"name\":\"Name\", \"default\":null}]}";  //"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"type\":\"string\",\"name\":\"Id\"},{\"name\":\"Store\",\"type\":{\"type\":\"record\",\"name\":\"Store\",\"fields\":[{\"type\":[\"null\",\"long\"],\"name\":\"c\", \"default\":0},{\"type\":\"string\",\"name\":\"d\"}]}}]}";
    private static final String SCHEMA_STR_V6 = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"S\",\"type\":{\"type\":\"record\",\"name\":\"T\",\"fields\":[{\"type\":[\"null\",\"long\"],\"name\":\"a\",\"default\":null},{\"type\":\"long\",\"name\":\"b\"}]}}]}";

    private static final Schema SCHEMA_V1 = new Schema.Parser().parse(SCHEMA_STR_V1);
    private static final Schema SCHEMA_V2 = new Schema.Parser().parse(SCHEMA_STR_V2);
    private static final Schema SCHEMA_V3 = new Schema.Parser().parse(SCHEMA_STR_V3);
    private static final Schema SCHEMA_V4 = new Schema.Parser().parse(SCHEMA_STR_V4);
    private static final Schema SCHEMA_V5 = new Schema.Parser().parse(SCHEMA_STR_V5);
    private static final Schema SCHEMA_V6 = new Schema.Parser().parse(SCHEMA_STR_V6);

    //mvn compile exec:java -Dexec.mainClass="com.datahem.avro.message.Test"
    public static void main(String[] args) throws IOException {
        DatastoreCache cache = new DatastoreCache();
        cache.addSchema(SCHEMA_V1);
        cache.addSchema(SCHEMA_V2);
        cache.addSchema(SCHEMA_V3);
        cache.addSchema(SCHEMA_V4);
        cache.addSchema(SCHEMA_V5);
        cache.addSchema(SCHEMA_V6);
        DynamicBinaryMessageDecoder<Record> decoder = new DynamicBinaryMessageDecoder<>(GenericData.get(), SCHEMA_V1, cache);

        System.out.println("V1");
        System.out.println(JSON_V1);
        System.out.println(Converters.avroBinaryToJson(Converters.jsonToAvroBinary(JSON_V1, SCHEMA_V1), decoder));
        System.out.println(AvroToBigQuery.getTableRow(Converters.jsonToAvroRecord(JSON_V1, SCHEMA_V1)).toPrettyString());
        System.out.println(AvroToBigQuery.getTableSchemaRecord(Converters.jsonToAvroRecord(JSON_V1, SCHEMA_V1).getSchema()).toString());

        System.out.println("V2");
        System.out.println(JSON_V2);
        System.out.println(Converters.avroBinaryToJson(Converters.jsonToAvroBinary(JSON_V2, SCHEMA_V2), decoder));
        System.out.println(AvroToBigQuery.getTableRow(Converters.jsonToAvroRecord(JSON_V2, SCHEMA_V2)).toPrettyString());
        System.out.println(AvroToBigQuery.getTableSchemaRecord(Converters.jsonToAvroRecord(JSON_V2, SCHEMA_V2).getSchema()).toString());

        System.out.println("V3");
        System.out.println(JSON_V3);
        System.out.println(Converters.avroBinaryToJson(Converters.jsonToAvroBinary(JSON_V3, SCHEMA_V3), decoder));
        System.out.println(AvroToBigQuery.getTableRow(Converters.jsonToAvroRecord(JSON_V3, SCHEMA_V3)).toPrettyString());
        System.out.println(AvroToBigQuery.getTableSchemaRecord(Converters.jsonToAvroRecord(JSON_V3, SCHEMA_V3).getSchema()).toString());
    }
}
