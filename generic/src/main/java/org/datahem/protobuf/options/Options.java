// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: datahem/options/options.proto

package org.datahem.protobuf.options;

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

public final class Options {
  private Options() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
    registry.add(org.datahem.protobuf.options.Options.bigQueryTableReference);
    registry.add(org.datahem.protobuf.options.Options.bigQueryTableDescription);
    registry.add(org.datahem.protobuf.options.Options.bigQueryFieldDescription);
    registry.add(org.datahem.protobuf.options.Options.bigQueryFieldCategories);
    registry.add(org.datahem.protobuf.options.Options.bigQueryFieldType);
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public static final int BIGQUERYTABLEREFERENCE_FIELD_NUMBER = 66666667;
  /**
   * <code>extend .google.protobuf.MessageOptions { ... }</code>
   */
  public static final
    com.google.protobuf.GeneratedMessage.GeneratedExtension<
      com.google.protobuf.DescriptorProtos.MessageOptions,
      java.lang.String> bigQueryTableReference = com.google.protobuf.GeneratedMessage
          .newFileScopedGeneratedExtension(
        java.lang.String.class,
        null);
  public static final int BIGQUERYTABLEDESCRIPTION_FIELD_NUMBER = 66666668;
  /**
   * <code>extend .google.protobuf.MessageOptions { ... }</code>
   */
  public static final
    com.google.protobuf.GeneratedMessage.GeneratedExtension<
      com.google.protobuf.DescriptorProtos.MessageOptions,
      java.lang.String> bigQueryTableDescription = com.google.protobuf.GeneratedMessage
          .newFileScopedGeneratedExtension(
        java.lang.String.class,
        null);
  public static final int BIGQUERYFIELDDESCRIPTION_FIELD_NUMBER = 66666667;
  /**
   * <code>extend .google.protobuf.FieldOptions { ... }</code>
   */
  public static final
    com.google.protobuf.GeneratedMessage.GeneratedExtension<
      com.google.protobuf.DescriptorProtos.FieldOptions,
      java.lang.String> bigQueryFieldDescription = com.google.protobuf.GeneratedMessage
          .newFileScopedGeneratedExtension(
        java.lang.String.class,
        null);
  public static final int BIGQUERYFIELDCATEGORIES_FIELD_NUMBER = 66666668;
  /**
   * <code>extend .google.protobuf.FieldOptions { ... }</code>
   */
  public static final
    com.google.protobuf.GeneratedMessage.GeneratedExtension<
      com.google.protobuf.DescriptorProtos.FieldOptions,
      java.lang.String> bigQueryFieldCategories = com.google.protobuf.GeneratedMessage
          .newFileScopedGeneratedExtension(
        java.lang.String.class,
        null);
  public static final int BIGQUERYFIELDTYPE_FIELD_NUMBER = 66666669;
  /**
   * <code>extend .google.protobuf.FieldOptions { ... }</code>
   */
  public static final
    com.google.protobuf.GeneratedMessage.GeneratedExtension<
      com.google.protobuf.DescriptorProtos.FieldOptions,
      java.lang.String> bigQueryFieldType = com.google.protobuf.GeneratedMessage
          .newFileScopedGeneratedExtension(
        java.lang.String.class,
        null);

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\035datahem/options/options.proto\022\017datahem" +
      ".options\032 google/protobuf/descriptor.pro" +
      "to:B\n\026BigQueryTableReference\022\037.google.pr" +
      "otobuf.MessageOptions\030\253\201\345\037 \001(\t:D\n\030BigQue" +
      "ryTableDescription\022\037.google.protobuf.Mes" +
      "sageOptions\030\254\201\345\037 \001(\t:B\n\030BigQueryFieldDes" +
      "cription\022\035.google.protobuf.FieldOptions\030" +
      "\253\201\345\037 \001(\t:A\n\027BigQueryFieldCategories\022\035.go" +
      "ogle.protobuf.FieldOptions\030\254\201\345\037 \001(\t:;\n\021B" +
      "igQueryFieldType\022\035.google.protobuf.Field" +
      "Options\030\255\201\345\037 \001(\tB\036\n\034org.datahem.protobuf" +
      ".optionsb\006proto3"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          com.google.protobuf.DescriptorProtos.getDescriptor(),
        }, assigner);
    bigQueryTableReference.internalInit(descriptor.getExtensions().get(0));
    bigQueryTableDescription.internalInit(descriptor.getExtensions().get(1));
    bigQueryFieldDescription.internalInit(descriptor.getExtensions().get(2));
    bigQueryFieldCategories.internalInit(descriptor.getExtensions().get(3));
    bigQueryFieldType.internalInit(descriptor.getExtensions().get(4));
    com.google.protobuf.DescriptorProtos.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
