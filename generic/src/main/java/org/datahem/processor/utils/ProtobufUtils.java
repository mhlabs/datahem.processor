package org.datahem.processor.utils;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 - 2019 MatHem Sverige AB
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtobufUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufUtils.class);

	public static String getFieldType(String in) {
		String className = "";
		if (in.toLowerCase().equals("boolean")) {
			className = "BOOLEAN";
		} else if (in.toLowerCase().contains(".string")) {
			className = "STRING";
		} else if (in.toLowerCase().equals("int") || in.toLowerCase().equals("int32")
				|| in.toLowerCase().equals("int64")) {
			className = "INTEGER";
		} else if (in.toLowerCase().equals("float") || in.toLowerCase().equals("double")) {
			className = "FLOAT";
		}
		return className;
	}

	public static TableSchema makeTableSchema(Descriptor d) {
		TableSchema res = new TableSchema();

		List<FieldDescriptor> fields = d.getFields();

		List<TableFieldSchema> schema_fields = new ArrayList<TableFieldSchema>();

		for (FieldDescriptor f : fields) {
			String type = "STRING";
			String mode = "NULLABLE";

			if (f.isRepeated()) {
				mode = "REPEATED";
			}

			if (f.getType().toString().toUpperCase().contains("BYTES")) {
				type = "BYTES";
			} else if (f.getType().toString().toUpperCase().contains("INT") 
                || f.getType().toString().toUpperCase().contains("ENUM")){
				type = "INTEGER";
			} else if (f.getType().toString().toUpperCase().contains("BOOL")) {
				type = "BOOLEAN";
			} else if (f.getType().toString().toUpperCase().contains("FLOAT")
					|| f.getType().toString().toUpperCase().contains("DOUBLE")) {
				type = "FLOAT";
			} else if (f.getType().toString().toUpperCase().contains("MESSAGE")) {
				type = "RECORD";
				TableSchema ts = makeTableSchema(f.getMessageType());

				schema_fields.add(new TableFieldSchema().setName(f.getName().replace(".", "_")).setType(type)
						.setMode(mode).setFields(ts.getFields()));
			}

			if (!type.equals("RECORD")) {
				schema_fields
						.add(new TableFieldSchema().setName(f.getName().replace(".", "_")).setType(type).setMode(mode));
			}
		}

		res.setFields(schema_fields);

		return res;
	}

	public static TableRow makeTableRow(Message message) {
		TableRow res = new TableRow();
		List<FieldDescriptor> fields = message.getDescriptorForType().getFields();

		for (FieldDescriptor f : fields) {
			String type = "STRING";
			//if (!f.isRepeated() && message.hasField(f)) {
            if (!f.isRepeated() ) {
				if (f.getType().toString().toUpperCase().contains("STRING")) {
					res.set(f.getName().replace(".", "_"), String.valueOf(message.getField(f)));
				} else if (f.getType().toString().toUpperCase().contains("BYTES")) {
					res.set(f.getName().replace(".", "_"), (byte[]) message.getField(f));
				} else if (f.getType().toString().toUpperCase().contains("INT32")) {
					res.set(f.getName().replace(".", "_"), (int) message.getField(f));
				} else if (f.getType().toString().toUpperCase().contains("INT64")) {
					res.set(f.getName().replace(".", "_"), (long) message.getField(f));
				} else if (f.getType().toString().toUpperCase().contains("BOOL")) {
					res.set(f.getName().replace(".", "_"), (boolean) message.getField(f));
				} else if (f.getType().toString().toUpperCase().contains("ENUM")) {
					res.set(f.getName().replace(".", "_"), ((EnumValueDescriptor) message.getField(f)).getNumber());
				} else if (f.getType().toString().toUpperCase().contains("FLOAT")
						|| f.getType().toString().toUpperCase().contains("DOUBLE")) {
					res.set(f.getName().replace(".", "_"), (double) message.getField(f));
				} else if (f.getType().toString().toUpperCase().contains("MESSAGE")) {
					type = "RECORD";
					if (message.getAllFields().containsKey(f)) {
						TableRow tr = makeTableRow((Message) message.getField(f));
						res.set(f.getName().replace(".", "_"), tr);
					}
				}
			} else if (f.isRepeated()) {
				if (f.getType().toString().toUpperCase().contains("STRING")) {
					List<String> values = ((List<Object>) message.getField(f)).stream().map(e -> String.valueOf(e))
							.collect(Collectors.toList());
					res.set(f.getName().replace(".", "_"), values);
				} else if (f.getType().toString().toUpperCase().contains("BYTES")) {
					List<byte[]> values = ((List<Object>) message.getField(f)).stream().map(e -> (byte[]) e)
							.collect(Collectors.toList());
					res.set(f.getName().replace(".", "_"), values);
				} else if (f.getType().toString().toUpperCase().contains("INT32")) {
					List<Integer> values = ((List<Object>) message.getField(f)).stream().map(e -> (int) e)
							.collect(Collectors.toList());
					res.set(f.getName().replace(".", "_"), values);
				} else if (f.getType().toString().toUpperCase().contains("INT64")) {
					List<Long> values = ((List<Object>) message.getField(f)).stream().map(e -> (long) e)
							.collect(Collectors.toList());
					res.set(f.getName().replace(".", "_"), values);
				} else if (f.getType().toString().toUpperCase().contains("BOOL")) {
					List<Boolean> values = ((List<Object>) message.getField(f)).stream().map(e -> (boolean) e)
							.collect(Collectors.toList());
					res.set(f.getName().replace(".", "_"), values);
				} else if (f.getType().toString().toUpperCase().contains("FLOAT")
						|| f.getType().toString().toUpperCase().contains("DOUBLE")) {
					List<Double> values = ((List<Object>) message.getField(f)).stream().map(e -> (double) e)
							.collect(Collectors.toList());
					res.set(f.getName().replace(".", "_"), values);
				} else if (f.getType().toString().toUpperCase().contains("MESSAGE")) {
					List<TableRow> values = ((List<Message>) message.getField(f)).stream().map(m -> makeTableRow(m))
							.collect(Collectors.toList());
					res.set(f.getName().replace(".", "_"), values);
				}
			}
		}
		return res;
	}

	// TODO: check repeated vs nested
	// TODO: check naming conventions
	public static Message makeMessage(TableRow tablerow, Message.Builder message) {
		List<FieldDescriptor> fields = message.getDescriptorForType().getFields();

		for (FieldDescriptor f : fields) {
			// Object currentField =
			// tablerow.get(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,
			// f.getName()));
			Object currentField = tablerow.get(f.getName());
			if (currentField != null) {
				if (f.getType().toString().toUpperCase().contains("STRING")) {
					message.setField(f, String.valueOf(currentField));
				} else if (f.getType().toString().toUpperCase().contains("BYTES")) {
					message.setField(f, (String.valueOf(currentField)).getBytes());
				} else if (f.getType().toString().toUpperCase().contains("INT32")) {
					message.setField(f, Integer.parseInt(String.valueOf(currentField)));
				} else if (f.getType().toString().toUpperCase().contains("INT64")) {
					message.setField(f, Long.parseLong(String.valueOf(currentField)));
				} else if (f.getType().toString().toUpperCase().contains("BOOL")) {
					boolean b = Boolean.parseBoolean(String.valueOf(currentField));
					message.setField(f, b);
				} else if (f.getType().toString().toUpperCase().contains("FLOAT")
						|| f.getType().toString().toUpperCase().contains("DOUBLE")) {
					message.setField(f, Double.parseDouble(String.valueOf(currentField)));
				} else if (f.getType().toString().toUpperCase().contains("MESSAGE")) {
					if (f.isRepeated()) {
						// if
						// (CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,
						// f.getName()) != null
						if (f.getName() != null && ((List<?>) currentField).size() > 0) {
							List<Message> m = new ArrayList<Message>();
							for (Object o : (List<Object>) currentField) {
								if (o.getClass() == TableRow.class) {
									m.add(makeMessage((TableRow) o, message.newBuilderForField(f)));
								} else {
									m.add(makeMessage((parseLinkedHashMapToTableRow((LinkedHashMap<String, Object>) o)),
											message.newBuilderForField(f)));
								}
							}
							message.setField(f, m);
						}
					} else if (tablerow
							// .get(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,
							// f.getName())) != null) {
							.get(f.getName()) != null) {
						if (currentField.getClass() != TableRow.class) {
							message.setField(f,
									makeMessage(
											parseLinkedHashMapToTableRow((LinkedHashMap<String, Object>) currentField),
											message.newBuilderForField(f)));

						} else {
							message.setField(f, makeMessage((TableRow) currentField, message.newBuilderForField(f)));
						}
					}
				}
			}
		}
		return message.build();
	}

	public static Message makeMessage(String csvrow, Message.Builder message) {
		List<FieldDescriptor> fields = message.getDescriptorForType().getFields();

		String[] csvArray = csvrow.split(",");
		for (int i = 0; i < fields.size(); i++) {
			FieldDescriptor f = fields.get(i);
			String currentField = csvArray[i];
			if (currentField != null && currentField.length() > 0) {
				// Convention: if name of protobuffer field starts with
				// "timestamp", the type in Java will be a Joda Instant
				// For now: suppose it's a UNIX timestamp in milliseconds
				if (f.getType().toString().toUpperCase().contains("STRING")) {
					message.setField(f, String.valueOf(currentField));
				} else if (f.getType().toString().toUpperCase().contains("BYTES")) {
					message.setField(f, currentField.getBytes());
				} else if (f.getType().toString().toUpperCase().contains("INT32")) {
					message.setField(f, Integer.parseInt(currentField));
				} else if (f.getType().toString().toUpperCase().contains("INT64")) {
					message.setField(f, Long.parseLong(currentField));
				} else if (f.getType().toString().toUpperCase().contains("BOOL")) {
					message.setField(f, Boolean.parseBoolean(currentField));
				} else if (f.getType().toString().toUpperCase().contains("FLOAT")) {
					message.setField(f, Float.parseFloat(currentField));
				} else if (f.getType().toString().toUpperCase().contains("DOUBLE")) {
					message.setField(f, Double.parseDouble(currentField));
				} else if (f.getType().toString().toUpperCase().contains("MESSAGE")) {
					if (f.getMessageType().getName().toUpperCase().contains("TIMESTAMP")) {
						message.setField(f, Timestamp.newBuilder().setSeconds(Long.parseLong(currentField) / 1000)
								.setNanos((int) ((Long.parseLong(currentField) % 1000) * 1000000)).build());
					}
				}
			}
		}
		return message.build();
	}

	public static TableRow parseLinkedHashMapToTableRow(LinkedHashMap<String, Object> input) {
		TableRow out = new TableRow();
		for (Entry<String, Object> e : input.entrySet()) {
			if (e.getValue().getClass() != LinkedHashMap.class && e.getValue().getClass() != TableRow.class) {
				out.set(e.getKey(), e.getValue());
			} else {
				out.set(e.getKey(), parseLinkedHashMapToTableRow((LinkedHashMap<String, Object>) e.getValue()));
			}
		}
		return out;
	}

	// Rewrite to more general (returning Object instead of List of Objects)
	public static Object getValueFromProtobuffer(String field, Message message) {
		Object res = null;
		String[] fieldHierarchy = field.split("\\.");

		FieldDescriptor q = message.getDescriptorForType().findFieldByName(fieldHierarchy[0]);

		if (q.isRepeated()) {
			if (fieldHierarchy.length > 1) {
				List<Message> m = (List<Message>) message.getField(q);
				res = m.stream().map(i -> getValueFromProtobuffer(field.substring(fieldHierarchy[0].length() + 1), i))
						.collect(Collectors.toList());
			} else {
				res = (List<Object>) message.getField(q);
			}
		} else if (message.hasField(q)) {
			if (fieldHierarchy.length > 1) {
				Message m = (Message) message.getField(q);
				res = getValueFromProtobuffer(field.substring(fieldHierarchy[0].length() + 1), m);
			} else {
				res = message.getField(q);
			}
		}

		return res;
	}

	public static Message.Builder getBuilderForField(String field, Message.Builder message) {
		Message.Builder res = null;
		String[] fieldHierarchy = field.split("\\.");

		FieldDescriptor q = message.getDescriptorForType().findFieldByName(fieldHierarchy[0]);

		if (fieldHierarchy.length > 1) {
			res = getBuilderForField(field.substring(fieldHierarchy[0].length() + 1), message.newBuilderForField(q));
		} else {
			res = message.newBuilderForField(q);
		}

		return res;
	}

	public static FieldDescriptor getDescriptorForField(String field, Message.Builder message) {
		String[] fieldHierarchy = field.split("\\.");

		FieldDescriptor q = message.getDescriptorForType().findFieldByName(fieldHierarchy[0]);

		if (fieldHierarchy.length > 1) {
			return getDescriptorForField(field.substring(fieldHierarchy[0].length() + 1),
					message.newBuilderForField(q));
		} else {
			return q;
		}
	}

	public static Message.Builder setBuilderForField(String field, Object value, Message.Builder message) {
		String[] fieldHierarchy = field.split("\\.");

		FieldDescriptor f = message.getDescriptorForType().findFieldByName(fieldHierarchy[0]);

		if (value != null) {
			if (fieldHierarchy.length > 1) {
				if (f.isRepeated()) {
					message.addRepeatedField(f, setBuilderForField(field.substring(fieldHierarchy[0].length() + 1),
							value, message.newBuilderForField(f)).build());
				} else {
					message.setField(f, setBuilderForField(field.substring(fieldHierarchy[0].length() + 1), value,
							message.newBuilderForField(f)).build());
				}
			} else {
				if (f.isRepeated()) {
					message.addRepeatedField(f, value);
				} else {
					message.setField(f, value);
				}
			}
		}
		return message;
	}

	public static Message.Builder mergeMessage(Message in, Message.Builder out) {
		List<FieldDescriptor> fieldsOut = out.getDescriptorForType().getFields();

		for (FieldDescriptor f : fieldsOut) {
			if (in.getDescriptorForType().getFields().stream().map(m -> m.getName()).collect(Collectors.toList())
					.contains(f.getName())
					&& f.getType().toString()
							.equals(in.getDescriptorForType().findFieldByName(f.getName()).getType().toString())) {
				if (f.getType().toString().toUpperCase().contains("MESSAGE")) {
					if (f.isRepeated()) {
						List<Message> inMessages = (List<Message>) in
								.getField(in.getDescriptorForType().findFieldByName(f.getName()));

						List<Message> r = new ArrayList<Message>();
						for (Message m : inMessages) {
							Message toAdd = mergeMessage(m, out.newBuilderForField(f)).build();
							r.add(toAdd);
						}

						out.setField(f, r);
					} else {
						Message r = mergeMessage(
								(Message) in.getField(in.getDescriptorForType().findFieldByName(f.getName())),
								((Message) out.getField(f)).newBuilderForType()).build();

						out.setField(f, r);
					}
				} else if (getValueFromProtobuffer(f.getName(), in) != null) {
					out.setField(f, getValueFromProtobuffer(f.getName(), in));
				}
			}
		}
		return out;
	}
}
