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

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

//import java.util.List;

public class Test3 {

    //private static final String json_string = "{\"S\": {\"b\":1}}";
//private static final String schema_str = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"S\",\"type\":{\"type\":\"record\",\"name\":\"T\",\"fields\":[{\"type\":[\"null\",\"long\"],\"name\":\"a\",\"default\":null},{\"type\":\"long\",\"name\":\"b\"}]}}]}";
    private static final String json_string = "{\"S\":[1,2]}";
    private static final String schema_str = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"S\",\"type\":{\"type\":\"array\", \"items\": \"int\"}}]}";

    private static final Schema SCHEMA = new Schema.Parser().parse(schema_str);

    public static void main(String[] args) throws IOException {

        try (JsonReader reader = new JsonReader(new StringReader(json_string))) {
            if (JsonToken.BEGIN_OBJECT.equals(reader.peek())) {
                reader.beginObject();
            }
            GenericData.Record record = readObject(reader, SCHEMA);

            System.out.println(record.toString());
            System.out.println(AvroToBigQuery.getTableRow(record).toString());
        }
    }

    private static GenericData.Record readObject(JsonReader reader, Schema schema /*GenericData.Record hm*/) throws IOException {
        GenericData.Record hm = new GenericData.Record(schema);
        String name = "";
        Object value = null;
        JsonToken nextToken = reader.peek();
        while (reader.hasNext() && !JsonToken.END_DOCUMENT.equals(nextToken)) {

            nextToken = reader.peek();
            switch (nextToken) {
                case BEGIN_OBJECT:
                    reader.beginObject();
                    //value = readObject(reader, new GenericData.Record(hm.getSchema().getField(name).schema()));
                    value = readObject(reader, schema.getField(name).schema());
                    hm.put(name, value);
                    break;
                case BEGIN_ARRAY:
                    reader.beginArray();
                    //value = readArray(reader, new ArrayList<Object>());
                    value = readArray(reader, schema.getField(name).schema().getElementType());
                    hm.put(name, value);
                    break;
                case END_ARRAY:
                    reader.endArray();
                    break;
                case NAME:
                    name = reader.nextName();
                    break;
                case STRING:
                    value = reader.nextString();
                    hm.put(name, value);
                    break;
                case NUMBER:
                    value = reader.nextDouble();
                    hm.put(name, value);
                    break;
                case BOOLEAN:
                    value = reader.nextBoolean();
                    hm.put(name, value);
                    break;
            }
            nextToken = reader.peek();
            if (JsonToken.END_OBJECT.equals(nextToken)) {
                reader.endObject();
                return hm;
            }
        }
        return hm;
    }


    private static ArrayList<Object> readArray(JsonReader reader, Schema schema /*ArrayList<Object> al*/) throws IOException {
        ArrayList<Object> al = new ArrayList<Object>();
        Object value = null;
        JsonToken nextToken = reader.peek();
        while (reader.hasNext() && !JsonToken.END_DOCUMENT.equals(nextToken)) {
            nextToken = reader.peek();
            switch (nextToken) {
                case BEGIN_OBJECT:
                    reader.beginObject();
                    //value = readObject(reader, new GenericData.Record(hm.getSchema().getField(name).schema()));
                    //value = readObject(reader, schema.getField(name).schema());
                    value = readObject(reader, schema);
                    al.add(value);
                    break;
                case BEGIN_ARRAY:
                    reader.beginArray();
                    //value = readArray(reader, schema.getField(name).schema().getElementType());
                    value = readArray(reader, schema.getElementType());
                    break;
                case STRING:
                    value = reader.nextString();
                    al.add(value);
                    break;
                case NUMBER:
                    value = reader.nextDouble();
                    al.add(value);
                    break;
                case BOOLEAN:
                    value = reader.nextBoolean();
                    al.add(value);
                    break;
            }
            nextToken = reader.peek();
            if (JsonToken.END_ARRAY.equals(nextToken)) {
                reader.endArray();
                return al;
            }
            System.out.println(nextToken.toString());
        }
        return al;
    }
}
