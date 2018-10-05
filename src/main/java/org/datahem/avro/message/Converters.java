package com.datahem.avro.message;

import org.apache.avro.Schema;
import java.io.IOException;
import java.io.StringReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import java.io.ByteArrayOutputStream;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.MessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.MessageEncoder;
import org.apache.avro.AvroRuntimeException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import javax.xml.bind.DatatypeConverter;
import org.apache.avro.SchemaNormalization;
import com.datahem.avro.message.DynamicBinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.message.MessageDecoder.BaseDecoder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

public class Converters {

	public static byte[] jsonToAvroBinary(String json, Schema schema) throws IOException {
		return avroRecordToBinary(jsonToAvroRecord(json, schema), schema);
	}
	
	public static GenericData.Record jsonToAvroRecord(String json, Schema schema) throws IOException {
		try (JsonReader reader = new JsonReader(new StringReader(json))) {
        	if (JsonToken.BEGIN_OBJECT.equals(reader.peek())) {
    			reader.beginObject();
    		}
        	return readObject(reader, schema);
		}
	}

	public static byte[] avroRecordToBinary(GenericData.Record genericRecord, Schema schema) throws IOException {
		MessageEncoder<Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), schema);
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		encoder.encode(genericRecord, output);
		output.flush();
		return output.toByteArray();
	}

	public static String avroBinaryToJson(byte[] avro, MessageDecoder.BaseDecoder<Record> decoder) throws IOException {
		return avroRecordToJson(avroBinaryToRecord(avro, decoder));
	}
	
	public static Record avroBinaryToRecord(byte[] avro, MessageDecoder.BaseDecoder<Record> decoder) throws IOException {
		return decoder.decode(avro);
	}

	public static String avroRecordToJson(GenericRecord record) {
		try {
 			boolean pretty = false;
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), output, pretty);
			new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, jsonEncoder);
			jsonEncoder.flush();
			return new String(output.toByteArray(), "UTF-8");
		} catch (IOException e) {
			throw new AvroConversionException("Failed to convert to JSON.", e);
		}
	}
	
	private static GenericData.Record readObject(JsonReader reader, Schema schema) throws IOException{
    	GenericData.Record record = new GenericData.Record(schema);
    	String name = "";
        JsonToken nextToken = reader.peek();
    	while (reader.hasNext() && !JsonToken.END_DOCUMENT.equals(nextToken)) {

                nextToken = reader.peek();
                switch(nextToken){
                	case BEGIN_OBJECT: 
                    	reader.beginObject();
                    	record.put(name,readObject(reader, schema.getField(name).schema()));
                    	break;
                	case BEGIN_ARRAY:
                    	reader.beginArray();
						record.put(name,readArray(reader, schema.getField(name).schema().getElementType()));
						break;
                	case END_ARRAY:
                    	reader.endArray();
                    	break;
                	case NAME:
                    	name = reader.nextName();
                    	break;
                	case STRING:
                    	record.put(name,reader.nextString());
                    	break;
                	case NUMBER:
                		double d = reader.nextDouble();
                    	record.put(name,((d % 1 == 0) ? (int) d : d));
                    	break;
                    case BOOLEAN:
                    	record.put(name,reader.nextBoolean());
                    	break;
                }	
                nextToken = reader.peek();
                if(JsonToken.END_OBJECT.equals(nextToken)){
                    reader.endObject();
                    return record;
                }
            }
            return record;
      }

      
      private static ArrayList<Object> readArray(JsonReader reader, Schema schema) throws IOException{
        ArrayList<Object> al = new ArrayList<Object>();
        Object value = null;
        JsonToken nextToken = reader.peek();
    	while (reader.hasNext() && !JsonToken.END_DOCUMENT.equals(nextToken)) {
                nextToken = reader.peek();
                switch(nextToken){
                	case BEGIN_OBJECT:
                    	reader.beginObject();
                    	al.add(readObject(reader, schema));
                    	break;
                    case BEGIN_ARRAY:
                    	reader.beginArray();
                    	al.add(readArray(reader, schema.getElementType()));
                    	break;
					case STRING:
                    	al.add(reader.nextString());
                    	break;
					case NUMBER:
                    	al.add(reader.nextDouble());
                    	break;
                    case BOOLEAN:
                    	al.add(reader.nextBoolean());
                    	break;
                } 
                nextToken = reader.peek();
                if (JsonToken.END_ARRAY.equals(nextToken)) {
                    reader.endArray();
                    return al;
                }
            }
            return al;
      }

public static class AvroConversionException extends AvroRuntimeException {

    public AvroConversionException(String message) {
        super(message);
    }

    public AvroConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}

}