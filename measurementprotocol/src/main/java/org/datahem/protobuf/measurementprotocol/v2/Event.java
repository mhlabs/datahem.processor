// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: datahem/protobuf/measurementprotocol/v2/measurement_protocol.proto

package org.datahem.protobuf.measurementprotocol.v2;

/*-
 * ========================LICENSE_START=================================
 * Datahem.processor.measurementprotocol
 * %%
 * Copyright (C) 2018 - 2019 Robert Sahlin
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */

/**
 * Protobuf type {@code datahem.protobuf.measurementprotocol.v2.Event}
 */
public  final class Event extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:datahem.protobuf.measurementprotocol.v2.Event)
    EventOrBuilder {
  // Use Event.newBuilder() to construct.
  private Event(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private Event() {
    category_ = "";
    action_ = "";
    label_ = "";
    value_ = 0;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private Event(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    int mutable_bitField0_ = 0;
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!input.skipField(tag)) {
              done = true;
            }
            break;
          }
          case 10: {
            java.lang.String s = input.readStringRequireUtf8();

            category_ = s;
            break;
          }
          case 18: {
            java.lang.String s = input.readStringRequireUtf8();

            action_ = s;
            break;
          }
          case 26: {
            java.lang.String s = input.readStringRequireUtf8();

            label_ = s;
            break;
          }
          case 32: {

            value_ = input.readInt32();
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return org.datahem.protobuf.measurementprotocol.v2.MeasurementProtocolOuterClass.internal_static_datahem_protobuf_measurementprotocol_v2_Event_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return org.datahem.protobuf.measurementprotocol.v2.MeasurementProtocolOuterClass.internal_static_datahem_protobuf_measurementprotocol_v2_Event_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            org.datahem.protobuf.measurementprotocol.v2.Event.class, org.datahem.protobuf.measurementprotocol.v2.Event.Builder.class);
  }

  public static final int CATEGORY_FIELD_NUMBER = 1;
  private volatile java.lang.Object category_;
  /**
   * <pre>
   *ec.	The event category.
   * </pre>
   *
   * <code>optional string category = 1;</code>
   */
  public java.lang.String getCategory() {
    java.lang.Object ref = category_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      category_ = s;
      return s;
    }
  }
  /**
   * <pre>
   *ec.	The event category.
   * </pre>
   *
   * <code>optional string category = 1;</code>
   */
  public com.google.protobuf.ByteString
      getCategoryBytes() {
    java.lang.Object ref = category_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      category_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int ACTION_FIELD_NUMBER = 2;
  private volatile java.lang.Object action_;
  /**
   * <pre>
   *ea.	The event action.
   * </pre>
   *
   * <code>optional string action = 2;</code>
   */
  public java.lang.String getAction() {
    java.lang.Object ref = action_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      action_ = s;
      return s;
    }
  }
  /**
   * <pre>
   *ea.	The event action.
   * </pre>
   *
   * <code>optional string action = 2;</code>
   */
  public com.google.protobuf.ByteString
      getActionBytes() {
    java.lang.Object ref = action_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      action_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int LABEL_FIELD_NUMBER = 3;
  private volatile java.lang.Object label_;
  /**
   * <pre>
   *el	The event label.
   * </pre>
   *
   * <code>optional string label = 3;</code>
   */
  public java.lang.String getLabel() {
    java.lang.Object ref = label_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      label_ = s;
      return s;
    }
  }
  /**
   * <pre>
   *el	The event label.
   * </pre>
   *
   * <code>optional string label = 3;</code>
   */
  public com.google.protobuf.ByteString
      getLabelBytes() {
    java.lang.Object ref = label_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      label_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int VALUE_FIELD_NUMBER = 4;
  private int value_;
  /**
   * <pre>
   *ev	The event value.        
   * </pre>
   *
   * <code>optional int32 value = 4;</code>
   */
  public int getValue() {
    return value_;
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (!getCategoryBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, category_);
    }
    if (!getActionBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, action_);
    }
    if (!getLabelBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 3, label_);
    }
    if (value_ != 0) {
      output.writeInt32(4, value_);
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!getCategoryBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, category_);
    }
    if (!getActionBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, action_);
    }
    if (!getLabelBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, label_);
    }
    if (value_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(4, value_);
    }
    memoizedSize = size;
    return size;
  }

  private static final long serialVersionUID = 0L;
  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof org.datahem.protobuf.measurementprotocol.v2.Event)) {
      return super.equals(obj);
    }
    org.datahem.protobuf.measurementprotocol.v2.Event other = (org.datahem.protobuf.measurementprotocol.v2.Event) obj;

    boolean result = true;
    result = result && getCategory()
        .equals(other.getCategory());
    result = result && getAction()
        .equals(other.getAction());
    result = result && getLabel()
        .equals(other.getLabel());
    result = result && (getValue()
        == other.getValue());
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptorForType().hashCode();
    hash = (37 * hash) + CATEGORY_FIELD_NUMBER;
    hash = (53 * hash) + getCategory().hashCode();
    hash = (37 * hash) + ACTION_FIELD_NUMBER;
    hash = (53 * hash) + getAction().hashCode();
    hash = (37 * hash) + LABEL_FIELD_NUMBER;
    hash = (53 * hash) + getLabel().hashCode();
    hash = (37 * hash) + VALUE_FIELD_NUMBER;
    hash = (53 * hash) + getValue();
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static org.datahem.protobuf.measurementprotocol.v2.Event parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.datahem.protobuf.measurementprotocol.v2.Event parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.datahem.protobuf.measurementprotocol.v2.Event parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static org.datahem.protobuf.measurementprotocol.v2.Event parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static org.datahem.protobuf.measurementprotocol.v2.Event parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static org.datahem.protobuf.measurementprotocol.v2.Event parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static org.datahem.protobuf.measurementprotocol.v2.Event parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static org.datahem.protobuf.measurementprotocol.v2.Event parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static org.datahem.protobuf.measurementprotocol.v2.Event parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static org.datahem.protobuf.measurementprotocol.v2.Event parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(org.datahem.protobuf.measurementprotocol.v2.Event prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code datahem.protobuf.measurementprotocol.v2.Event}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:datahem.protobuf.measurementprotocol.v2.Event)
      org.datahem.protobuf.measurementprotocol.v2.EventOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.datahem.protobuf.measurementprotocol.v2.MeasurementProtocolOuterClass.internal_static_datahem_protobuf_measurementprotocol_v2_Event_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.datahem.protobuf.measurementprotocol.v2.MeasurementProtocolOuterClass.internal_static_datahem_protobuf_measurementprotocol_v2_Event_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.datahem.protobuf.measurementprotocol.v2.Event.class, org.datahem.protobuf.measurementprotocol.v2.Event.Builder.class);
    }

    // Construct using org.datahem.protobuf.measurementprotocol.v2.Event.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    public Builder clear() {
      super.clear();
      category_ = "";

      action_ = "";

      label_ = "";

      value_ = 0;

      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return org.datahem.protobuf.measurementprotocol.v2.MeasurementProtocolOuterClass.internal_static_datahem_protobuf_measurementprotocol_v2_Event_descriptor;
    }

    public org.datahem.protobuf.measurementprotocol.v2.Event getDefaultInstanceForType() {
      return org.datahem.protobuf.measurementprotocol.v2.Event.getDefaultInstance();
    }

    public org.datahem.protobuf.measurementprotocol.v2.Event build() {
      org.datahem.protobuf.measurementprotocol.v2.Event result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public org.datahem.protobuf.measurementprotocol.v2.Event buildPartial() {
      org.datahem.protobuf.measurementprotocol.v2.Event result = new org.datahem.protobuf.measurementprotocol.v2.Event(this);
      result.category_ = category_;
      result.action_ = action_;
      result.label_ = label_;
      result.value_ = value_;
      onBuilt();
      return result;
    }

    public Builder clone() {
      return (Builder) super.clone();
    }
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return (Builder) super.setField(field, value);
    }
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return (Builder) super.clearField(field);
    }
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return (Builder) super.clearOneof(oneof);
    }
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, Object value) {
      return (Builder) super.setRepeatedField(field, index, value);
    }
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return (Builder) super.addRepeatedField(field, value);
    }
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof org.datahem.protobuf.measurementprotocol.v2.Event) {
        return mergeFrom((org.datahem.protobuf.measurementprotocol.v2.Event)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(org.datahem.protobuf.measurementprotocol.v2.Event other) {
      if (other == org.datahem.protobuf.measurementprotocol.v2.Event.getDefaultInstance()) return this;
      if (!other.getCategory().isEmpty()) {
        category_ = other.category_;
        onChanged();
      }
      if (!other.getAction().isEmpty()) {
        action_ = other.action_;
        onChanged();
      }
      if (!other.getLabel().isEmpty()) {
        label_ = other.label_;
        onChanged();
      }
      if (other.getValue() != 0) {
        setValue(other.getValue());
      }
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      org.datahem.protobuf.measurementprotocol.v2.Event parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (org.datahem.protobuf.measurementprotocol.v2.Event) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private java.lang.Object category_ = "";
    /**
     * <pre>
     *ec.	The event category.
     * </pre>
     *
     * <code>optional string category = 1;</code>
     */
    public java.lang.String getCategory() {
      java.lang.Object ref = category_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        category_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <pre>
     *ec.	The event category.
     * </pre>
     *
     * <code>optional string category = 1;</code>
     */
    public com.google.protobuf.ByteString
        getCategoryBytes() {
      java.lang.Object ref = category_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        category_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <pre>
     *ec.	The event category.
     * </pre>
     *
     * <code>optional string category = 1;</code>
     */
    public Builder setCategory(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      category_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     *ec.	The event category.
     * </pre>
     *
     * <code>optional string category = 1;</code>
     */
    public Builder clearCategory() {
      
      category_ = getDefaultInstance().getCategory();
      onChanged();
      return this;
    }
    /**
     * <pre>
     *ec.	The event category.
     * </pre>
     *
     * <code>optional string category = 1;</code>
     */
    public Builder setCategoryBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      category_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object action_ = "";
    /**
     * <pre>
     *ea.	The event action.
     * </pre>
     *
     * <code>optional string action = 2;</code>
     */
    public java.lang.String getAction() {
      java.lang.Object ref = action_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        action_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <pre>
     *ea.	The event action.
     * </pre>
     *
     * <code>optional string action = 2;</code>
     */
    public com.google.protobuf.ByteString
        getActionBytes() {
      java.lang.Object ref = action_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        action_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <pre>
     *ea.	The event action.
     * </pre>
     *
     * <code>optional string action = 2;</code>
     */
    public Builder setAction(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      action_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     *ea.	The event action.
     * </pre>
     *
     * <code>optional string action = 2;</code>
     */
    public Builder clearAction() {
      
      action_ = getDefaultInstance().getAction();
      onChanged();
      return this;
    }
    /**
     * <pre>
     *ea.	The event action.
     * </pre>
     *
     * <code>optional string action = 2;</code>
     */
    public Builder setActionBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      action_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object label_ = "";
    /**
     * <pre>
     *el	The event label.
     * </pre>
     *
     * <code>optional string label = 3;</code>
     */
    public java.lang.String getLabel() {
      java.lang.Object ref = label_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        label_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <pre>
     *el	The event label.
     * </pre>
     *
     * <code>optional string label = 3;</code>
     */
    public com.google.protobuf.ByteString
        getLabelBytes() {
      java.lang.Object ref = label_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        label_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <pre>
     *el	The event label.
     * </pre>
     *
     * <code>optional string label = 3;</code>
     */
    public Builder setLabel(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      label_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     *el	The event label.
     * </pre>
     *
     * <code>optional string label = 3;</code>
     */
    public Builder clearLabel() {
      
      label_ = getDefaultInstance().getLabel();
      onChanged();
      return this;
    }
    /**
     * <pre>
     *el	The event label.
     * </pre>
     *
     * <code>optional string label = 3;</code>
     */
    public Builder setLabelBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      label_ = value;
      onChanged();
      return this;
    }

    private int value_ ;
    /**
     * <pre>
     *ev	The event value.        
     * </pre>
     *
     * <code>optional int32 value = 4;</code>
     */
    public int getValue() {
      return value_;
    }
    /**
     * <pre>
     *ev	The event value.        
     * </pre>
     *
     * <code>optional int32 value = 4;</code>
     */
    public Builder setValue(int value) {
      
      value_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     *ev	The event value.        
     * </pre>
     *
     * <code>optional int32 value = 4;</code>
     */
    public Builder clearValue() {
      
      value_ = 0;
      onChanged();
      return this;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }


    // @@protoc_insertion_point(builder_scope:datahem.protobuf.measurementprotocol.v2.Event)
  }

  // @@protoc_insertion_point(class_scope:datahem.protobuf.measurementprotocol.v2.Event)
  private static final org.datahem.protobuf.measurementprotocol.v2.Event DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new org.datahem.protobuf.measurementprotocol.v2.Event();
  }

  public static org.datahem.protobuf.measurementprotocol.v2.Event getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<Event>
      PARSER = new com.google.protobuf.AbstractParser<Event>() {
    public Event parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
        return new Event(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<Event> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<Event> getParserForType() {
    return PARSER;
  }

  public org.datahem.protobuf.measurementprotocol.v2.Event getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}
