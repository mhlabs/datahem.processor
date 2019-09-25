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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableFieldSchema.Categories;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageOptions;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.Message;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Timestamp;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ByteString;

import io.anemos.metastore.core.proto.*;

import java.io.InputStream;
import java.io.IOException;

import java.nio.channels.Channels;

import java.lang.StringBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Stream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.Iterator;

import java.math.BigInteger;

import org.datahem.protobuf.options.Options;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtobufUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufUtils.class);


    private static Map<String, FileDescriptorProto> extractProtoMap(
        FileDescriptorSet fileDescriptorSet) {
        HashMap<String, FileDescriptorProto> map = new HashMap<>();
        fileDescriptorSet.getFileList().forEach(fdp -> map.put(fdp.getName(), fdp));
        return map;
    }

    private static FileDescriptor getFileDescriptor(String name, FileDescriptorSet fileDescriptorSet) {
        Map<String, FileDescriptorProto> inMap = extractProtoMap(fileDescriptorSet);
        Map<String, FileDescriptor> outMap = new HashMap<>();
        return convertToFileDescriptorMap(name, inMap, outMap);
    }

    private static FileDescriptor convertToFileDescriptorMap(String name, Map<String, FileDescriptorProto> inMap,
        Map<String, FileDescriptor> outMap) {
        if (outMap.containsKey(name)) {
            return outMap.get(name);
        }
        FileDescriptorProto fileDescriptorProto = inMap.get(name);
        List<FileDescriptor> dependencies = new ArrayList<>();
        if (fileDescriptorProto.getDependencyCount() > 0) {
            LOG.info("more than 0 dependencies: " + fileDescriptorProto.toString());
            fileDescriptorProto
                .getDependencyList()
                .forEach(dependencyName -> dependencies.add(convertToFileDescriptorMap(dependencyName, inMap, outMap)));
        }
        try {
            LOG.info("Number of dependencies: " + Integer.toString(dependencies.size()));
            FileDescriptor fileDescriptor = 
                FileDescriptor.buildFrom(
                    fileDescriptorProto, dependencies.toArray(new FileDescriptor[dependencies.size()]));
            outMap.put(name, fileDescriptor);
            return fileDescriptor;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

public static ProtoDescriptor getProtoDescriptorFromCloudStorage(
        String bucketName, 
        String fileDescriptorName) throws Exception {
            try{
                Storage storage = StorageOptions.getDefaultInstance().getService();
                Blob blob = storage.get(BlobId.of(bucketName, fileDescriptorName));
                ReadChannel reader = blob.reader();
                InputStream inputStream = Channels.newInputStream(reader);
                FileDescriptorSet descriptorSetObject = FileDescriptorSet.parseFrom(inputStream);
                return new ProtoDescriptor(descriptorSetObject);
            }catch (Exception e){
                e.printStackTrace();
                return null;
            }
        }

    public static Descriptor getDescriptorFromCloudStorage(
        String bucketName, 
        String fileDescriptorName, 
        String descriptorFullName) throws Exception {
            try{
                return getProtoDescriptorFromCloudStorage(bucketName, fileDescriptorName).getDescriptorByName(descriptorFullName);
            }catch (Exception e){
                e.printStackTrace();
                return null;
            }
        }


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

    private static String unsignedToString(final long value) {
    if (value >= 0) {
      return Long.toString(value);
    } else {
      // Pull off the most-significant bit so that BigInteger doesn't think
      // the number is negative, then set it again using setBit().
      return BigInteger.valueOf(value & 0x7FFFFFFFFFFFFFFFL).setBit(63).toString();
    }
  }

    private static String getOptionValue(FieldDescriptor descriptor, Object value) {
      switch (descriptor.getType()) {
        case STRING:
          return "\"" + (String) value + "\"";
        case INT32:
          return Integer.toString((Integer) value);
        case INT64:
          return Long.toString((Long) value);
        case ENUM:
          EnumValueDescriptor valueDescriptor = (EnumValueDescriptor) value;
          return valueDescriptor.toString();
      }
      return "";
    }

    private static String getUnknownPrimitiveFieldValue(
        FieldDescriptor fieldDescriptor, 
        Object value) {
      switch (fieldDescriptor.getType()) {
        //case MESSAGE: return null;
        case BOOL:
          return value.equals(1L) ? "true" : "false";
        case ENUM:
        case STRING:
          ByteString byteString = (ByteString) value;
          return byteString.toStringUtf8();
          //return "\"" + byteString.toStringUtf8() + "\"";
        case INT32:
        case INT64:
          return unsignedToString((Long) value);
        case DOUBLE:
          Double d = Double.longBitsToDouble((Long) value);
          return d.toString();
        case FLOAT:
          Float f = Float.intBitsToFloat((Integer) value);
          return f.toString();
      }
      throw new RuntimeException(
          "conversion of unknownfield for type " + fieldDescriptor.getType().toString() + " not implemented");
    }

    private static Multimap<FieldDescriptor, String> getUnknownFieldValue(
        FieldDescriptor fieldDescriptor, 
        UnknownFieldSet.Field field) {
      
        HashMultimap<FieldDescriptor, String> unknownFieldValues = HashMultimap.create();
      
      for (Object value : field.getLengthDelimitedList()) {
        unknownFieldValues.put(
            fieldDescriptor, getUnknownPrimitiveFieldValue(fieldDescriptor, value));
      }
      for (Object value : field.getFixed32List()) {
        unknownFieldValues.put(
            fieldDescriptor, getUnknownPrimitiveFieldValue(fieldDescriptor, value));
      }
      for (Object value : field.getFixed64List()) {
        unknownFieldValues.put(
            fieldDescriptor, getUnknownPrimitiveFieldValue(fieldDescriptor, value));
      }
      for (Object value : field.getVarintList()) {
        unknownFieldValues.put(
            fieldDescriptor, getUnknownPrimitiveFieldValue(fieldDescriptor, value));
      }
      for (Object value : field.getGroupList()) {
        unknownFieldValues.put(
            fieldDescriptor, getUnknownPrimitiveFieldValue(fieldDescriptor, value));
      }
      return unknownFieldValues;
    }

    private static HashMultimap<FieldDescriptor, String> getUnknownFieldValues(
        UnknownFieldSet unknownFieldSet,
        Map<Integer, FieldDescriptor> optionsMap) {
      HashMultimap<FieldDescriptor, String> unknownFieldValues = HashMultimap.create();
      unknownFieldSet
          .asMap()
          .forEach(
              (number, field) -> {
                FieldDescriptor fieldDescriptor = optionsMap.get(number);
                unknownFieldValues.putAll(getUnknownFieldValue(fieldDescriptor, field));
              });
      return unknownFieldValues;
    }

    public static HashMultimap<String, String> getMessageOptions(ProtoDescriptor protoDescriptor, Descriptor descriptor){
        HashMultimap<String, String> messageOptions = HashMultimap.create();
        if (!descriptor.getOptions().getUnknownFields().asMap().isEmpty()) {
            HashMultimap<FieldDescriptor, String> unknownOptionsMap = getUnknownFieldValues(
                descriptor.getOptions().getUnknownFields(),
                protoDescriptor.getMessageOptionMap());
            Set<FieldDescriptor> keys = unknownOptionsMap.keySet();
            for (FieldDescriptor fd : keys) {
                Collection<String> values = unknownOptionsMap.get(fd);
                for (String value : values) {
                    messageOptions.put(fd.getName(), value);
                }
            }
        }
        return messageOptions;
    }

    public static HashMultimap<String, String> getFieldOptions(ProtoDescriptor protoDescriptor, FieldDescriptor fieldDescriptor){
        HashMultimap<String, String> fieldOptions = HashMultimap.create();
        if (!fieldDescriptor.getOptions().getUnknownFields().asMap().isEmpty()) {
                 HashMultimap<FieldDescriptor, String> unknownOptionsMap =
                    getUnknownFieldValues(
                        fieldDescriptor.getOptions().getUnknownFields(),
                        protoDescriptor.getFieldOptionMap());
                Iterator<Map.Entry<FieldDescriptor, String>> unknownIter = unknownOptionsMap.entries().iterator();
                while (unknownIter.hasNext()) {
                    Map.Entry<FieldDescriptor, String> fieldOption = unknownIter.next();
                    FieldDescriptor fd = fieldOption.getKey();
                    String value = fieldOption.getValue();
                    fieldOptions.put(fd.getName(), value);
                }
        }
        return fieldOptions;
    }

    public static TableSchema makeTableSchema(ProtoDescriptor protoDescriptor, Descriptor descriptor) {
        return makeTableSchema(protoDescriptor, descriptor, ".*");
    }

    public static TableSchema makeTableSchema(ProtoDescriptor protoDescriptor, Descriptor descriptor, String taxonomyResourcePattern) {
        LOG.info("Descriptor fullname: " + descriptor.getFullName());
        LOG.info("messageOptions: " + descriptor.getOptions().toString());

        TableSchema res = new TableSchema();

        // Iterate fields
		List<FieldDescriptor> fields = descriptor.getFields();
		List<TableFieldSchema> schema_fields = new ArrayList<TableFieldSchema>();
		for (FieldDescriptor f : fields) {
            HashMultimap<String, String> fieldOptions = getFieldOptions(protoDescriptor, f);
            String description = ((Set<String>) fieldOptions.get("BigQueryFieldDescription")).stream().findFirst().orElse("");
            final Pattern categoryFilter = Pattern.compile(taxonomyResourcePattern);
            List<String> categories = ((Set<String>) fieldOptions.get("BigQueryFieldCategories"))
                .stream()
                .map(categoriesOption -> categoriesOption.split(","))
                .flatMap(categoriesArray -> Arrays.stream(categoriesArray))
                .filter(categoryFilter.asPredicate())
                .collect(Collectors.toList());
            TableFieldSchema.Categories fieldCategories = new TableFieldSchema.Categories();
            fieldCategories.setNames(categories.isEmpty() ? null : categories);
			
            String type = "STRING";
			String mode = "NULLABLE";

			if (f.isRepeated()) {
				mode = "REPEATED";
			}
            String bigQueryFieldType = ((Set<String>) fieldOptions.get("BigQueryFieldType")).stream().findFirst().orElse("");
            String[] standardSqlTypes = {"INT64","NUMERIC","FLOAT64","BOOL","STRING","BYTES","DATE","DATETIME","GEOGRAPHY","TIME","TIMESTAMP"};
            if(Arrays.stream(standardSqlTypes).anyMatch(bigQueryFieldType::equals)){
                type = bigQueryFieldType;
            }
			else if (f.getType().toString().toUpperCase().contains("BYTES")) {
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
				TableSchema ts = makeTableSchema(protoDescriptor, f.getMessageType(), taxonomyResourcePattern);

				schema_fields
                    .add(
                        new TableFieldSchema()
                            .setName(f.getName().replace(".", "_"))
                            .setType(type)
					        .setMode(mode)
                            .setFields(ts.getFields())
                            .setDescription(description)
                            );
			}

			if (!type.equals("RECORD")) {
				schema_fields
						.add(new TableFieldSchema()
                            .setName(f.getName().replace(".", "_"))
                            .setType(type)
                            .setMode(mode)
                            .setDescription(description)
                            .setCategories(fieldCategories));
			}
        }
		res.setFields(schema_fields);
        LOG.info("table schema" + res.toString());
		return res;
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
        return makeTableRow(message, message.getDescriptorForType());
    }

    public static TableRow makeTableRow(Descriptor descriptor) {
        return makeTableRow(DynamicMessage.newBuilder(descriptor).clear().build(), descriptor);
    }

	public static TableRow makeTableRow(Message message, Descriptor descriptor) {
		TableRow res = new TableRow();
        List<FieldDescriptor> fields = descriptor.getFields();

		for (FieldDescriptor f : fields) {
			String type = "STRING";
            
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
                        TableRow tr = makeTableRow((Message) message.getField(f), f.getMessageType());
						res.set(f.getName().replace(".", "_"), tr);
					}else{
                        TableRow tr = makeTableRow(f.getMessageType());
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
                        List<TableRow> values = ((List<Message>) message.getField(f))
                            .stream()
                            .map(m -> makeTableRow(m,  f.getMessageType()))
							.collect(Collectors.toList());
					    res.set(f.getName().replace(".", "_"), values);
				}
			}
		}
		return res;
	}

    public static TableRow makeTableRow(Descriptor descriptor, ProtoDescriptor protoDescriptor) {
        return makeTableRow(DynamicMessage.newBuilder(descriptor).clear().build(), descriptor, protoDescriptor);
    }

    public static TableRow makeTableRow(Message message, Descriptor descriptor, ProtoDescriptor protoDescriptor) {
		TableRow res = new TableRow();
        List<FieldDescriptor> fields = descriptor.getFields();

		for (FieldDescriptor f : fields) {
            HashMultimap<String, String> fieldOptions = getFieldOptions(protoDescriptor, f);
            String bigQueryFieldType = ((Set<String>) fieldOptions.get("BigQueryFieldType")).stream().findFirst().orElse("");
            if (!f.isRepeated() ) {
                if(bigQueryFieldType.isEmpty()){
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
                        if (message.getAllFields().containsKey(f)) {
                            TableRow tr = makeTableRow((Message) message.getField(f), f.getMessageType(), protoDescriptor);
                            if(!tr.isEmpty()){
                                res.set(f.getName().replace(".", "_"), tr);
                            }
                        }
                        /*else{
                            // doesn't contain message, create an empty message
                            TableRow tr = makeTableRow(f.getMessageType(), protoDescriptor);
                            res.set(f.getName().replace(".", "_"), tr);
                        }*/
				    }
                } else if(!bigQueryFieldType.isEmpty()){
                    if (bigQueryFieldType.contains("STRING")) {
                        res.set(f.getName().replace(".", "_"), String.valueOf(message.getField(f)));
                    } else if (bigQueryFieldType.contains("BYTES")) {
                        res.set(f.getName().replace(".", "_"), (byte[]) message.getField(f));
                    } else if (bigQueryFieldType.contains("INT64")) {
                        res.set(f.getName().replace(".", "_"), (long) message.getField(f));
                    } else if (bigQueryFieldType.contains("BOOL")) {
                        res.set(f.getName().replace(".", "_"), (boolean) message.getField(f));
                    } else if (bigQueryFieldType.contains("FLOAT")
                        || bigQueryFieldType.contains("DOUBLE")) {
                        res.set(f.getName().replace(".", "_"), (double) message.getField(f));
                    } else if (bigQueryFieldType.contains("DATE")
                        || bigQueryFieldType.contains("DATETIME")
                        || bigQueryFieldType.contains("TIME")
                        || bigQueryFieldType.contains("TIMESTAMP")) {
                        if (!String.valueOf(message.getField(f)).isEmpty()){
                            res.set(f.getName().replace(".", "_"), String.valueOf(message.getField(f)));
                        }
                    } 
                }
			} else if (f.isRepeated()) {
                if(bigQueryFieldType.isEmpty()){
                    if (f.getType().toString().toUpperCase().contains("STRING")) {
                        List<String> values = ((List<Object>) message.getField(f)).stream().map(e -> String.valueOf(e))
                                .collect(Collectors.toList());
                        if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                    } else if (f.getType().toString().toUpperCase().contains("BYTES")) {
                        List<byte[]> values = ((List<Object>) message.getField(f)).stream().map(e -> (byte[]) e)
                                .collect(Collectors.toList());
                        if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                        //res.set(f.getName().replace(".", "_"), values);
                    } else if (f.getType().toString().toUpperCase().contains("INT32")) {
                        List<Integer> values = ((List<Object>) message.getField(f)).stream().map(e -> (int) e)
                                .collect(Collectors.toList());
                        if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                        //res.set(f.getName().replace(".", "_"), values);
                    } else if (f.getType().toString().toUpperCase().contains("INT64")) {
                        List<Long> values = ((List<Object>) message.getField(f)).stream().map(e -> (long) e)
                                .collect(Collectors.toList());
                        if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                        //res.set(f.getName().replace(".", "_"), values);
                    } else if (f.getType().toString().toUpperCase().contains("BOOL")) {
                        List<Boolean> values = ((List<Object>) message.getField(f)).stream().map(e -> (boolean) e)
                                .collect(Collectors.toList());
                        if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                        //res.set(f.getName().replace(".", "_"), values);
                    } else if (f.getType().toString().toUpperCase().contains("FLOAT")
                            || f.getType().toString().toUpperCase().contains("DOUBLE")) {
                        List<Double> values = ((List<Object>) message.getField(f)).stream().map(e -> (double) e)
                                .collect(Collectors.toList());
                        if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                        //res.set(f.getName().replace(".", "_"), values);
                    } else if (f.getType().toString().toUpperCase().contains("MESSAGE")) {
                        List<TableRow> values = ((List<Message>) message.getField(f))
                            .stream()
                            .map(m -> {
                                TableRow tr = makeTableRow(m,  f.getMessageType(), protoDescriptor);
                                if(!tr.isEmpty()){
                                    return tr;
                                }else{
                                    return null;
                                }
                            })
                            .filter(g -> g != null)
							.collect(Collectors.toList());
					    if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                        //res.set(f.getName().replace(".", "_"), values);
				    }
                } else if(!bigQueryFieldType.isEmpty()){

                    if (bigQueryFieldType.contains("STRING")) {
                        List<String> values = ((List<Object>) message.getField(f)).stream().map(e -> String.valueOf(e))
                                .collect(Collectors.toList());
                        if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                        //res.set(f.getName().replace(".", "_"), values);
                    } else if (bigQueryFieldType.contains("BYTES")) {
                        List<byte[]> values = ((List<Object>) message.getField(f)).stream().map(e -> (byte[]) e)
                                .collect(Collectors.toList());
                        if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                        //res.set(f.getName().replace(".", "_"), values);
                    } else if (bigQueryFieldType.contains("INT64")) {
                        List<Long> values = ((List<Object>) message.getField(f)).stream().map(e -> (long) e)
                                .collect(Collectors.toList());
                        if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                        //res.set(f.getName().replace(".", "_"), values);
                    } else if (bigQueryFieldType.contains("BOOL")) {
                        List<Boolean> values = ((List<Object>) message.getField(f)).stream().map(e -> (boolean) e)
                                .collect(Collectors.toList());
                        if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                        //res.set(f.getName().replace(".", "_"), values);
                    } else if (bigQueryFieldType.contains("FLOAT")
                            || bigQueryFieldType.contains("DOUBLE")) {
                        List<Double> values = ((List<Object>) message.getField(f)).stream().map(e -> (double) e)
                                .collect(Collectors.toList());
                        if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                        //res.set(f.getName().replace(".", "_"), values);
                    } else if (bigQueryFieldType.contains("DATE")
                            || bigQueryFieldType.contains("DATETIME")
                            || bigQueryFieldType.contains("TIME")
                            || bigQueryFieldType.contains("TIMESTAMP") ) {
                        List<String> values = ((List<Object>) message.getField(f)).stream().filter(g -> !String.valueOf(g).isEmpty()).map(e -> String.valueOf(e))
                                .collect(Collectors.toList());
                        if(!values.isEmpty()){
                            res.set(f.getName().replace(".", "_"), values);
                        }
                        //res.set(f.getName().replace(".", "_"), values);
                    } 
                }
			}
		}
		return res;
	}


/*
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
    */
}
