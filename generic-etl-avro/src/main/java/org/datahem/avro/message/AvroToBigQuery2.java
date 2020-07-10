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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import org.apache.avro.Schema;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type;

//import java.util.Arrays;

//public class AvroToBigQuery<TRecord extends SpecificRecord>  extends DoFn<TRecord, TableRow> {
public class AvroToBigQuery2 {

    public static com.google.cloud.bigquery.Schema getTableSchemaRecord(Schema schema) {
        return com.google.cloud.bigquery.Schema.of(getFieldsSchema(schema.getFields()));
    }

    static Iterable<Field> getFieldsSchema(List<Schema.Field> fields) {
        return fields.stream().map(field -> {
            Field.Builder fieldBuilder = Field.newBuilder("default", LegacySQLTypeName.STRING);// = new Field.Builder();
            fieldBuilder.setName(field.name().replace(".", "_")).setMode(Mode.REQUIRED);
            Type type = field.schema().getType();
            if (type == Schema.Type.UNION) {
                for (Schema possible : field.schema().getTypes()) {
                    if (possible.getType() == Schema.Type.NULL) {
                        fieldBuilder.setMode(Mode.NULLABLE);
                    } else {
                        type = possible.getType();
                    }
                }
            }
            switch (type) {
                case ARRAY:
                    fieldBuilder
                            .setMode(Mode.REPEATED)
                            .setType(LegacySQLTypeName.RECORD, FieldList.of(getFieldsSchema(field.schema().getElementType().getFields())));
                    break;
                case RECORD:
                    fieldBuilder
                            .setType(LegacySQLTypeName.RECORD, FieldList.of(getFieldsSchema(field.schema().getFields())));
                    break;
                case INT:
                case LONG:
                    fieldBuilder.setType(LegacySQLTypeName.INTEGER);
                    break;
                case BOOLEAN:
                    fieldBuilder.setType(LegacySQLTypeName.BOOLEAN);
                    break;
                case FLOAT:
                case DOUBLE:
                    fieldBuilder.setType(LegacySQLTypeName.FLOAT);
                    break;
                case BYTES:
                    fieldBuilder.setType(LegacySQLTypeName.BYTES);
                    break;
                case MAP:
                    fieldBuilder
                            .setMode(Mode.REPEATED)
                            .setType(LegacySQLTypeName.RECORD, FieldList.of(getFieldsSchema(field.schema().getValueType().getFields())));
                    break;
                case NULL:
                    fieldBuilder
                            .setMode(Mode.NULLABLE);
                case ENUM:
                case FIXED:
                case STRING:
                default:
                    fieldBuilder.setType(LegacySQLTypeName.STRING);
            }
            return fieldBuilder.build();
        }).collect(Collectors.toList());
    }
}
