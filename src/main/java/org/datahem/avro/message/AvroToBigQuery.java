package com.datahem.avro.message;

import com.google.api.client.json.GenericJson;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
//import com.google.cloud.dataflow.sdk.transforms.DoFn;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.generic.GenericRecord;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.*;

//public class AvroToBigQuery<TRecord extends SpecificRecord>  extends DoFn<TRecord, TableRow> {
public class AvroToBigQuery {

	static TableRow getTableRow(GenericRecord record) {
        TableRow row = new TableRow();
        encode(record, row);
        return row;
    }


    static TableCell getTableCell(GenericRecord record) {
        TableCell cell = new TableCell();
        encode(record, cell);
        return cell;
    }

    private static void encode(GenericRecord record, GenericJson row) {
        Schema schema = record.getSchema();
        schema.getFields().forEach(field -> {
            Type type = field.schema().getType();
            String name = field.name().replace(".", "_");
            //System.out.println(type);
            switch (type) {
                case RECORD:
                    row.set(name, getTableCell((GenericRecord) record.get(field.pos())));
                    break;
                case INT:
                case LONG:
                    row.set(name, ((Number)record.get(field.pos())).longValue());
                    break;
                case BOOLEAN:
                    row.set(name, record.get(field.pos()));
                    break;
                case FLOAT:
                case DOUBLE:
                    row.set(name, ((Number)record.get(field.pos())).doubleValue());
                    break;
                default:
                    row.set(name, String.valueOf(record.get(field.pos())));
            }
        });
    }

    public static TableSchema getTableSchemaRecord(Schema schema) {
        return new TableSchema().setFields(getFieldsSchema(schema.getFields()));
    }

    static List<TableFieldSchema> getFieldsSchema(List<Schema.Field> fields) {
        return fields.stream().map(field -> {
            TableFieldSchema column = new TableFieldSchema().setName(field.name().replace(".", "_")).setMode("REQUIRED");
            Type type = field.schema().getType();
            //System.out.println(field.name() + " : " + type.getName());
            if(type == Schema.Type.UNION){
            	for (Schema possible : field.schema().getTypes()) {
					if (possible.getType() == Schema.Type.NULL) {
						column.setMode("NULLABLE");
					}else{
						//System.out.println(possible.toString());
						type = possible.getType();
					}
				}
			}
            switch (type) {
            	case ARRAY:
                    column.setType("RECORD");
                    column.setMode("REPEATED");
                    column.setFields(getFieldsSchema(field.schema().getElementType().getFields()));
                    break;
                case RECORD:
                    column.setType("RECORD");
                    column.setFields(getFieldsSchema(field.schema().getFields()));
                    break;
                case INT:
                case LONG:
                    column.setType("INTEGER");
                    break;
                case BOOLEAN:
                    column.setType("BOOLEAN");
                    break;
                case FLOAT:
                case DOUBLE:
                    column.setType("FLOAT");
                    break;
                case BYTES:
                    column.setType("BYTES");
                    break;
                case MAP:
                    column.setType("RECORD");
                    column.setMode("REPEATED");
                    column.setFields(getFieldsSchema(field.schema().getValueType().getFields()));
                    break;
                case NULL:
                    column.setMode("NULLABLE");
                case ENUM:
                case FIXED:
                case STRING:
                default:
                    column.setType("STRING");
            }
            return column;
        }).collect(Collectors.toList());
    }
    
    
    
	private static boolean nullable(Schema schema) {
		if (Schema.Type.NULL == schema.getType()) {
			return true;
		} else if (Schema.Type.UNION == schema.getType()) {
			for (Schema possible : schema.getTypes()) {
				if (nullable(possible)) {
					return true;
				}
			}
		}
		return false;
	}
}