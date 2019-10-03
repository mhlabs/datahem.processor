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

import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

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
import java.util.Optional;

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
            if(!fieldOptionBigQueryHidden(fieldOptions)){
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
                } else if(Arrays.stream(standardSqlTypes).anyMatch(bigQueryFieldType::equals)){
                    type = bigQueryFieldType;
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
        }
		res.setFields(schema_fields);
        LOG.info("table schema" + res.toString());
		return res;
	}   

    public static Optional<String> fieldOptionBigQueryRename(HashMultimap<String, String> fieldOptions){
        return ((Set<String>) fieldOptions.get("BigQueryFieldRename")).stream().findFirst();
    }

    public static boolean fieldOptionBigQueryHidden(HashMultimap<String, String> fieldOptions){
        String hidden = ((Set<String>) fieldOptions.get("BigQueryFieldHidden")).stream().findFirst().orElse("false");
        if(hidden.toLowerCase().equals("true")){
            return true;
        }else{
            return false;
        }
    }

    public static Optional<String> fieldOptionBigQueryType(HashMultimap<String, String> fieldOptions){
        return ((Set<String>) fieldOptions.get("BigQueryFieldType")).stream().findFirst();
    }

    public static String fieldOptionBigQueryAppend(String fieldValue, HashMultimap<String, String> fieldOptions){
        String appendix = ((Set<String>) fieldOptions.get("BigQueryFieldAppend")).stream().findFirst().orElse("");
        return (!fieldValue.isEmpty() ? fieldValue + appendix : fieldValue);
    }

    public static String fieldOptionBigQueryConcatFields(String fieldValue, HashMultimap<String, String> fieldOptions){
        String appendix = ((Set<String>) fieldOptions.get("BigQueryFieldAppend")).stream().findFirst().orElse("");
        
        return (!fieldValue.isEmpty() ? fieldValue + appendix : fieldValue);
    }

    public static String fieldOptionBigQueryRegexExtract(String value, HashMultimap<String, String> fieldOptions){
        String regexExtract = ((Set<String>) fieldOptions.get("BigQueryFieldRegexExtract")).stream().findFirst().orElse("");
        if(!regexExtract.isEmpty()){
            final Pattern pattern = Pattern.compile(regexExtract);
            Matcher matcher = pattern.matcher(value);
            if(matcher.find()){
                //LOG.info("Regex: pattern: " + regexExtract + ", input: " + value + ", output: " + matcher.group(0));
                return matcher.group(0);
            }
        }
        return value;
    }

    public static String fieldOptionBigQueryRegexReplace(String value, HashMultimap<String, String> fieldOptions){
        String regexReplace = ((Set<String>) fieldOptions.get("BigQueryFieldRegexReplace")).stream().findFirst().orElse("");
        if(!regexReplace.isEmpty()){
            //LOG.info("regexreplace: " + regexReplace + ", output: " + value.replaceAll(regexReplace.split(",")[0], regexReplace.split(",")[1]));
            return value.replaceAll(regexReplace.split(",")[0].trim(), regexReplace.split(",")[1].trim());
        }
        return value;
    }

    public static String fieldOptionBigQueryLocalToUtc(String value, HashMultimap<String, String> fieldOptions){
        String timezoneSettings = ((Set<String>) fieldOptions.get("BigQueryFieldLocalToUtc")).stream().findFirst().orElse("");
        if(!timezoneSettings.isEmpty()){
            DateTimeFormatter localFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
            DateTimeFormatter utcFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
            String[] timezoneSettingsArr = timezoneSettings.split(",");
            String localTimezone = timezoneSettingsArr[0].trim();
            
            if (timezoneSettingsArr.length == 3){
                localFormatter = DateTimeFormatter.ofPattern(timezoneSettingsArr[1].trim()).withZone(ZoneId.of(localTimezone)); 
                utcFormatter = DateTimeFormatter.ofPattern(timezoneSettingsArr[2].trim()).withZone(ZoneId.of("Etc/UTC"));
            }

            LocalDateTime localDateTime = LocalDateTime.parse(value, localFormatter);
            ZonedDateTime utcDateTime = localDateTime.atZone(ZoneId.of(localTimezone)).withZoneSameInstant(ZoneId.of("UTC"));
            String utc = utcDateTime.format(utcFormatter);
            //LOG.info("fieldOptionBigQueryLocalToUtc: input (local): " + value + ", output (utc): " + utc);
            return utc;
        }
        return value;
    }

/*
    public static String fieldOptionBigQueryConcatFields(String value, HashMultimap<String, String> fieldOptions){
        String concatSettings = ((Set<String>) fieldOptions.get("BigQueryConcatFields")).stream().findFirst().orElse("");
        if(!concatSettings.isEmpty()){
            String[] concatSettingsArr = concatSettings.split(",");
            String concatenated = "";
            for(String c : concatSettings){
                concatenated +=  String.valueOf(message.getField(descriptor.findFieldByName(c)));
            }
            return concatenated;
        }
        return value;
    }
*/

    public static TableRow getTableRow(Message message, FieldDescriptor f, ProtoDescriptor protoDescriptor, TableRow tableRow){
        String[] bigQueryStandardSqlDateTimeTypes = {"DATE","DATETIME","TIME","TIMESTAMP"};
        HashMultimap<String, String> fieldOptions = getFieldOptions(protoDescriptor, f);
        
        if(!fieldOptionBigQueryHidden(fieldOptions)){
            String fieldName = fieldOptionBigQueryRename(fieldOptions).orElse(f.getName().replace(".", "_"));    
            String fieldType = fieldOptionBigQueryType(fieldOptions).orElse(f.getType().toString().toUpperCase());
            
            if (!f.isRepeated() ) {
                if (fieldType.contains("STRING")) {
                    String fieldValue = String.valueOf(message.getField(f));
                    fieldValue = fieldOptionBigQueryRegexExtract(fieldValue, fieldOptions);
                    fieldValue = fieldOptionBigQueryAppend(fieldValue, fieldOptions);
                    fieldValue = fieldOptionBigQueryRegexReplace(fieldValue, fieldOptions);
                    tableRow.set(fieldName, fieldValue);
                } else if (fieldType.contains("BYTES")) {
                    tableRow.set(fieldName, (byte[]) message.getField(f));
                } else if (fieldType.contains("INT32")) {
                    tableRow.set(fieldName, (int) message.getField(f));
                } else if (fieldType.contains("INT64")) {
                    tableRow.set(fieldName, (long) message.getField(f));
                } else if (fieldType.contains("BOOL")) {
                    tableRow.set(fieldName, (boolean) message.getField(f));
                } else if (fieldType.contains("ENUM")) {
                    tableRow.set(fieldName, ((EnumValueDescriptor) message.getField(f)).getNumber());
                } else if (fieldType.contains("FLOAT") || fieldType.contains("DOUBLE")) {
                    tableRow.set(fieldName, (double) message.getField(f));
                } else if(Arrays.stream(bigQueryStandardSqlDateTimeTypes).anyMatch(fieldType::equals)) {
                    String fieldValue = String.valueOf(message.getField(f));
                    if (!fieldValue.isEmpty()){
                        fieldValue = fieldOptionBigQueryRegexExtract(fieldValue, fieldOptions);
                        fieldValue = fieldOptionBigQueryAppend(fieldValue, fieldOptions);
                        fieldValue = fieldOptionBigQueryRegexReplace(fieldValue, fieldOptions);
                        fieldValue = fieldOptionBigQueryLocalToUtc(fieldValue, fieldOptions);
                        tableRow.set(fieldName, fieldValue);
                    }
                } else if (fieldType.contains("MESSAGE")) {
                    if (message.getAllFields().containsKey(f)) {
                        TableRow tr = makeTableRow((Message) message.getField(f), f.getMessageType(), protoDescriptor);
                        if(!tr.isEmpty()){
                            tableRow.set(fieldName, tr);
                        }
                    }
                }
            } else if (f.isRepeated()) {
                if (fieldType.contains("STRING")) {
                    List<String> values = ((List<Object>) message.getField(f))
                        .stream()
                        .map(e -> {
                            String fieldValue = String.valueOf(e);
                            fieldValue = fieldOptionBigQueryRegexExtract(fieldValue, fieldOptions);
                            fieldValue = fieldOptionBigQueryAppend(fieldValue, fieldOptions);
                            fieldValue = fieldOptionBigQueryRegexReplace(fieldValue, fieldOptions);
                            return fieldValue;
                        })
                        .collect(Collectors.toList());
                    if(!values.isEmpty()){
                        tableRow.set(fieldName, values);
                    }
                } else if (fieldType.contains("BYTES")) {
                    List<byte[]> values = ((List<Object>) message.getField(f)).stream().map(e -> (byte[]) e).collect(Collectors.toList());
                    if(!values.isEmpty()){
                        tableRow.set(fieldName, values);
                    }
                } else if (fieldType.contains("INT32")) {
                    List<Integer> values = ((List<Object>) message.getField(f)).stream().map(e -> (int) e).collect(Collectors.toList());
                    if(!values.isEmpty()){
                        tableRow.set(fieldName, values);
                    }
                } else if (fieldType.contains("INT64")) {
                    List<Long> values = ((List<Object>) message.getField(f)).stream().map(e -> (long) e).collect(Collectors.toList());
                    if(!values.isEmpty()){
                        tableRow.set(fieldName, values);
                    }
                } else if (fieldType.contains("BOOL")) {
                    List<Boolean> values = ((List<Object>) message.getField(f)).stream().map(e -> (boolean) e).collect(Collectors.toList());
                    if(!values.isEmpty()){
                        tableRow.set(fieldName, values);
                    }
                } else if (fieldType.contains("FLOAT") || fieldType.contains("DOUBLE")) {
                    List<Double> values = ((List<Object>) message.getField(f)).stream().map(e -> (double) e).collect(Collectors.toList());
                    if(!values.isEmpty()){
                        tableRow.set(fieldName, values);
                    }
                } else if(Arrays.stream(bigQueryStandardSqlDateTimeTypes).anyMatch(fieldType::equals)) {
                    List<String> values = ((List<Object>) message.getField(f))
                        .stream()
                        .map(e -> {
                            String fieldValue = String.valueOf(e);
                            fieldValue = fieldOptionBigQueryRegexExtract(fieldValue, fieldOptions);
                            fieldValue = fieldOptionBigQueryAppend(fieldValue, fieldOptions);
                            fieldValue = fieldOptionBigQueryRegexReplace(fieldValue, fieldOptions);
                            fieldValue = fieldOptionBigQueryLocalToUtc(fieldValue, fieldOptions);
                            return fieldValue;
                        })
                        .collect(Collectors.toList());
                    if(!values.isEmpty()){
                        tableRow.set(fieldName, values);
                    }
                } else if (fieldType.contains("MESSAGE")) {
                    List<TableRow> values = ((List<Message>) message.getField(f)).stream()
                        .map(m -> {
                            TableRow tr = makeTableRow(m,  f.getMessageType(), protoDescriptor);
                            if(!tr.isEmpty()){
                                return tr;
                            }else{
                                return null;
                            }
                        })
                        .filter(g -> g != null).collect(Collectors.toList());
                    if(!values.isEmpty()){
                        tableRow.set(fieldName, values);
                    }
                }
            }
        }       
        return tableRow;
    }

    public static TableRow makeTableRow(Descriptor descriptor, ProtoDescriptor protoDescriptor) {
        return makeTableRow(DynamicMessage.newBuilder(descriptor).clear().build(), descriptor, protoDescriptor);
    }

     public static TableRow makeTableRow(Message message, Descriptor descriptor, ProtoDescriptor protoDescriptor) {
        TableRow tableRow = new TableRow();
        List<FieldDescriptor> fields = descriptor.getFields();

		for (FieldDescriptor field : fields) {
            tableRow = getTableRow(message, field, protoDescriptor, tableRow);
        }
        return tableRow;
     }
}