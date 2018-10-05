package org.datahem.avro.message;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData;
import java.io.IOException;
import com.datahem.avro.message.Converters;
import com.datahem.avro.message.DatastoreCache;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import java.util.Map;
import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

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