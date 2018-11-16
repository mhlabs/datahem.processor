package org.datahem.processor.generic;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.Coder;

import org.apache.beam.sdk.coders.CustomCoder;
//import org.apache.avro.message.BinaryMessageDecoder;
import org.datahem.avro.message.DynamicBinaryMessageDecoder;
//import org.apache.avro.message.MessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.MessageEncoder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.CoderException;
import java.util.List;
import java.util.Collections;
import java.io.ByteArrayOutputStream;
import org.datahem.avro.message.DatastoreCache;

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
public class GenericRecordCoder extends CustomCoder<Record> {

	@Override
	public void encode(Record record, OutputStream outStream) throws IOException {
		MessageEncoder<Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), record.getSchema());
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		encoder.encode(record, output);
		output.flush();
		output.writeTo(outStream);
	}

  @Override
  public Record decode(InputStream inStream) throws CoderException, IOException {
  	String SCHEMA_STR_V1 = "{\"type\":\"record\", \"namespace\":\"foo\", \"name\":\"Man\", \"fields\":[ { \"name\":\"name\", \"type\":\"string\" }, { \"name\":\"age\", \"type\":[\"null\",\"double\"] } ] }";
  	Schema SCHEMA_V1 = new Schema.Parser().parse(SCHEMA_STR_V1);
  	DatastoreCache cache = new DatastoreCache();
  	DynamicBinaryMessageDecoder<Record> decoder = new DynamicBinaryMessageDecoder<>(GenericData.get(), SCHEMA_V1, cache);
  	return decoder.decode(toByteArray(inStream));
  }
 
  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }
 
  @Override
  public void verifyDeterministic() throws NonDeterministicException {}
  
  public static byte[] toByteArray(InputStream in) throws IOException {

		ByteArrayOutputStream os = new ByteArrayOutputStream();

		byte[] buffer = new byte[1024];
		int len;

		// read bytes from the input stream and store them in buffer
		while ((len = in.read(buffer)) != -1) {
			// write bytes from the buffer into output stream
			os.write(buffer, 0, len);
		}

		return os.toByteArray();
	}
}
