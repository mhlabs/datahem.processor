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
 * Protobuf type {@code datahem.protobuf.measurementprotocol.v2.Time}
 */
public final class Time extends
        com.google.protobuf.GeneratedMessageV3 implements
        // @@protoc_insertion_point(message_implements:datahem.protobuf.measurementprotocol.v2.Time)
        TimeOrBuilder {
    // Use Time.newBuilder() to construct.
    private Time(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
        super(builder);
    }

    private Time() {
        dateTime_ = "";
        date_ = "";
        time_ = "";
        timeZone_ = "";
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
        return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
    }

    private Time(
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

                        dateTime_ = s;
                        break;
                    }
                    case 18: {
                        java.lang.String s = input.readStringRequireUtf8();

                        date_ = s;
                        break;
                    }
                    case 26: {
                        java.lang.String s = input.readStringRequireUtf8();

                        time_ = s;
                        break;
                    }
                    case 34: {
                        java.lang.String s = input.readStringRequireUtf8();

                        timeZone_ = s;
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
        return org.datahem.protobuf.measurementprotocol.v2.MeasurementProtocolOuterClass.internal_static_datahem_protobuf_measurementprotocol_v2_Time_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
    internalGetFieldAccessorTable() {
        return org.datahem.protobuf.measurementprotocol.v2.MeasurementProtocolOuterClass.internal_static_datahem_protobuf_measurementprotocol_v2_Time_fieldAccessorTable
                .ensureFieldAccessorsInitialized(
                        org.datahem.protobuf.measurementprotocol.v2.Time.class, org.datahem.protobuf.measurementprotocol.v2.Time.Builder.class);
    }

    public static final int DATETIME_FIELD_NUMBER = 1;
    private volatile java.lang.Object dateTime_;

    /**
     * <pre>
     * local datetime YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
     * </pre>
     *
     * <code>optional string dateTime = 1;</code>
     */
    public java.lang.String getDateTime() {
        java.lang.Object ref = dateTime_;
        if (ref instanceof java.lang.String) {
            return (java.lang.String) ref;
        } else {
            com.google.protobuf.ByteString bs =
                    (com.google.protobuf.ByteString) ref;
            java.lang.String s = bs.toStringUtf8();
            dateTime_ = s;
            return s;
        }
    }

    /**
     * <pre>
     * local datetime YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
     * </pre>
     *
     * <code>optional string dateTime = 1;</code>
     */
    public com.google.protobuf.ByteString
    getDateTimeBytes() {
        java.lang.Object ref = dateTime_;
        if (ref instanceof java.lang.String) {
            com.google.protobuf.ByteString b =
                    com.google.protobuf.ByteString.copyFromUtf8(
                            (java.lang.String) ref);
            dateTime_ = b;
            return b;
        } else {
            return (com.google.protobuf.ByteString) ref;
        }
    }

    public static final int DATE_FIELD_NUMBER = 2;
    private volatile java.lang.Object date_;

    /**
     * <pre>
     * local date YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
     * </pre>
     *
     * <code>optional string date = 2;</code>
     */
    public java.lang.String getDate() {
        java.lang.Object ref = date_;
        if (ref instanceof java.lang.String) {
            return (java.lang.String) ref;
        } else {
            com.google.protobuf.ByteString bs =
                    (com.google.protobuf.ByteString) ref;
            java.lang.String s = bs.toStringUtf8();
            date_ = s;
            return s;
        }
    }

    /**
     * <pre>
     * local date YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
     * </pre>
     *
     * <code>optional string date = 2;</code>
     */
    public com.google.protobuf.ByteString
    getDateBytes() {
        java.lang.Object ref = date_;
        if (ref instanceof java.lang.String) {
            com.google.protobuf.ByteString b =
                    com.google.protobuf.ByteString.copyFromUtf8(
                            (java.lang.String) ref);
            date_ = b;
            return b;
        } else {
            return (com.google.protobuf.ByteString) ref;
        }
    }

    public static final int TIME_FIELD_NUMBER = 3;
    private volatile java.lang.Object time_;

    /**
     * <pre>
     * local time [H]H:[M]M:[S]S[.DDDDDD]
     * </pre>
     *
     * <code>optional string time = 3;</code>
     */
    public java.lang.String getTime() {
        java.lang.Object ref = time_;
        if (ref instanceof java.lang.String) {
            return (java.lang.String) ref;
        } else {
            com.google.protobuf.ByteString bs =
                    (com.google.protobuf.ByteString) ref;
            java.lang.String s = bs.toStringUtf8();
            time_ = s;
            return s;
        }
    }

    /**
     * <pre>
     * local time [H]H:[M]M:[S]S[.DDDDDD]
     * </pre>
     *
     * <code>optional string time = 3;</code>
     */
    public com.google.protobuf.ByteString
    getTimeBytes() {
        java.lang.Object ref = time_;
        if (ref instanceof java.lang.String) {
            com.google.protobuf.ByteString b =
                    com.google.protobuf.ByteString.copyFromUtf8(
                            (java.lang.String) ref);
            time_ = b;
            return b;
        } else {
            return (com.google.protobuf.ByteString) ref;
        }
    }

    public static final int TIMEZONE_FIELD_NUMBER = 4;
    private volatile java.lang.Object timeZone_;

    /**
     * <pre>
     * local timeZone continent/[region/]city
     * </pre>
     *
     * <code>optional string timeZone = 4;</code>
     */
    public java.lang.String getTimeZone() {
        java.lang.Object ref = timeZone_;
        if (ref instanceof java.lang.String) {
            return (java.lang.String) ref;
        } else {
            com.google.protobuf.ByteString bs =
                    (com.google.protobuf.ByteString) ref;
            java.lang.String s = bs.toStringUtf8();
            timeZone_ = s;
            return s;
        }
    }

    /**
     * <pre>
     * local timeZone continent/[region/]city
     * </pre>
     *
     * <code>optional string timeZone = 4;</code>
     */
    public com.google.protobuf.ByteString
    getTimeZoneBytes() {
        java.lang.Object ref = timeZone_;
        if (ref instanceof java.lang.String) {
            com.google.protobuf.ByteString b =
                    com.google.protobuf.ByteString.copyFromUtf8(
                            (java.lang.String) ref);
            timeZone_ = b;
            return b;
        } else {
            return (com.google.protobuf.ByteString) ref;
        }
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
        if (!getDateTimeBytes().isEmpty()) {
            com.google.protobuf.GeneratedMessageV3.writeString(output, 1, dateTime_);
        }
        if (!getDateBytes().isEmpty()) {
            com.google.protobuf.GeneratedMessageV3.writeString(output, 2, date_);
        }
        if (!getTimeBytes().isEmpty()) {
            com.google.protobuf.GeneratedMessageV3.writeString(output, 3, time_);
        }
        if (!getTimeZoneBytes().isEmpty()) {
            com.google.protobuf.GeneratedMessageV3.writeString(output, 4, timeZone_);
        }
    }

    public int getSerializedSize() {
        int size = memoizedSize;
        if (size != -1) return size;

        size = 0;
        if (!getDateTimeBytes().isEmpty()) {
            size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, dateTime_);
        }
        if (!getDateBytes().isEmpty()) {
            size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, date_);
        }
        if (!getTimeBytes().isEmpty()) {
            size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, time_);
        }
        if (!getTimeZoneBytes().isEmpty()) {
            size += com.google.protobuf.GeneratedMessageV3.computeStringSize(4, timeZone_);
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
        if (!(obj instanceof org.datahem.protobuf.measurementprotocol.v2.Time)) {
            return super.equals(obj);
        }
        org.datahem.protobuf.measurementprotocol.v2.Time other = (org.datahem.protobuf.measurementprotocol.v2.Time) obj;

        boolean result = true;
        result = result && getDateTime()
                .equals(other.getDateTime());
        result = result && getDate()
                .equals(other.getDate());
        result = result && getTime()
                .equals(other.getTime());
        result = result && getTimeZone()
                .equals(other.getTimeZone());
        return result;
    }

    @java.lang.Override
    public int hashCode() {
        if (memoizedHashCode != 0) {
            return memoizedHashCode;
        }
        int hash = 41;
        hash = (19 * hash) + getDescriptorForType().hashCode();
        hash = (37 * hash) + DATETIME_FIELD_NUMBER;
        hash = (53 * hash) + getDateTime().hashCode();
        hash = (37 * hash) + DATE_FIELD_NUMBER;
        hash = (53 * hash) + getDate().hashCode();
        hash = (37 * hash) + TIME_FIELD_NUMBER;
        hash = (53 * hash) + getTime().hashCode();
        hash = (37 * hash) + TIMEZONE_FIELD_NUMBER;
        hash = (53 * hash) + getTimeZone().hashCode();
        hash = (29 * hash) + unknownFields.hashCode();
        memoizedHashCode = hash;
        return hash;
    }

    public static org.datahem.protobuf.measurementprotocol.v2.Time parseFrom(
            com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data);
    }

    public static org.datahem.protobuf.measurementprotocol.v2.Time parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.datahem.protobuf.measurementprotocol.v2.Time parseFrom(byte[] data)
            throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data);
    }

    public static org.datahem.protobuf.measurementprotocol.v2.Time parseFrom(
            byte[] data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.datahem.protobuf.measurementprotocol.v2.Time parseFrom(java.io.InputStream input)
            throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
                .parseWithIOException(PARSER, input);
    }

    public static org.datahem.protobuf.measurementprotocol.v2.Time parseFrom(
            java.io.InputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
                .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static org.datahem.protobuf.measurementprotocol.v2.Time parseDelimitedFrom(java.io.InputStream input)
            throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
                .parseDelimitedWithIOException(PARSER, input);
    }

    public static org.datahem.protobuf.measurementprotocol.v2.Time parseDelimitedFrom(
            java.io.InputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
                .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }

    public static org.datahem.protobuf.measurementprotocol.v2.Time parseFrom(
            com.google.protobuf.CodedInputStream input)
            throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
                .parseWithIOException(PARSER, input);
    }

    public static org.datahem.protobuf.measurementprotocol.v2.Time parseFrom(
            com.google.protobuf.CodedInputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
                .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public Builder newBuilderForType() {
        return newBuilder();
    }

    public static Builder newBuilder() {
        return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(org.datahem.protobuf.measurementprotocol.v2.Time prototype) {
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
     * Protobuf type {@code datahem.protobuf.measurementprotocol.v2.Time}
     */
    public static final class Builder extends
            com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
            // @@protoc_insertion_point(builder_implements:datahem.protobuf.measurementprotocol.v2.Time)
            org.datahem.protobuf.measurementprotocol.v2.TimeOrBuilder {
        public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
            return org.datahem.protobuf.measurementprotocol.v2.MeasurementProtocolOuterClass.internal_static_datahem_protobuf_measurementprotocol_v2_Time_descriptor;
        }

        protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
            return org.datahem.protobuf.measurementprotocol.v2.MeasurementProtocolOuterClass.internal_static_datahem_protobuf_measurementprotocol_v2_Time_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            org.datahem.protobuf.measurementprotocol.v2.Time.class, org.datahem.protobuf.measurementprotocol.v2.Time.Builder.class);
        }

        // Construct using org.datahem.protobuf.measurementprotocol.v2.Time.newBuilder()
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
            dateTime_ = "";

            date_ = "";

            time_ = "";

            timeZone_ = "";

            return this;
        }

        public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
            return org.datahem.protobuf.measurementprotocol.v2.MeasurementProtocolOuterClass.internal_static_datahem_protobuf_measurementprotocol_v2_Time_descriptor;
        }

        public org.datahem.protobuf.measurementprotocol.v2.Time getDefaultInstanceForType() {
            return org.datahem.protobuf.measurementprotocol.v2.Time.getDefaultInstance();
        }

        public org.datahem.protobuf.measurementprotocol.v2.Time build() {
            org.datahem.protobuf.measurementprotocol.v2.Time result = buildPartial();
            if (!result.isInitialized()) {
                throw newUninitializedMessageException(result);
            }
            return result;
        }

        public org.datahem.protobuf.measurementprotocol.v2.Time buildPartial() {
            org.datahem.protobuf.measurementprotocol.v2.Time result = new org.datahem.protobuf.measurementprotocol.v2.Time(this);
            result.dateTime_ = dateTime_;
            result.date_ = date_;
            result.time_ = time_;
            result.timeZone_ = timeZone_;
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
            if (other instanceof org.datahem.protobuf.measurementprotocol.v2.Time) {
                return mergeFrom((org.datahem.protobuf.measurementprotocol.v2.Time) other);
            } else {
                super.mergeFrom(other);
                return this;
            }
        }

        public Builder mergeFrom(org.datahem.protobuf.measurementprotocol.v2.Time other) {
            if (other == org.datahem.protobuf.measurementprotocol.v2.Time.getDefaultInstance()) return this;
            if (!other.getDateTime().isEmpty()) {
                dateTime_ = other.dateTime_;
                onChanged();
            }
            if (!other.getDate().isEmpty()) {
                date_ = other.date_;
                onChanged();
            }
            if (!other.getTime().isEmpty()) {
                time_ = other.time_;
                onChanged();
            }
            if (!other.getTimeZone().isEmpty()) {
                timeZone_ = other.timeZone_;
                onChanged();
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
            org.datahem.protobuf.measurementprotocol.v2.Time parsedMessage = null;
            try {
                parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                parsedMessage = (org.datahem.protobuf.measurementprotocol.v2.Time) e.getUnfinishedMessage();
                throw e.unwrapIOException();
            } finally {
                if (parsedMessage != null) {
                    mergeFrom(parsedMessage);
                }
            }
            return this;
        }

        private java.lang.Object dateTime_ = "";

        /**
         * <pre>
         * local datetime YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
         * </pre>
         *
         * <code>optional string dateTime = 1;</code>
         */
        public java.lang.String getDateTime() {
            java.lang.Object ref = dateTime_;
            if (!(ref instanceof java.lang.String)) {
                com.google.protobuf.ByteString bs =
                        (com.google.protobuf.ByteString) ref;
                java.lang.String s = bs.toStringUtf8();
                dateTime_ = s;
                return s;
            } else {
                return (java.lang.String) ref;
            }
        }

        /**
         * <pre>
         * local datetime YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
         * </pre>
         *
         * <code>optional string dateTime = 1;</code>
         */
        public com.google.protobuf.ByteString
        getDateTimeBytes() {
            java.lang.Object ref = dateTime_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8(
                                (java.lang.String) ref);
                dateTime_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        /**
         * <pre>
         * local datetime YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
         * </pre>
         *
         * <code>optional string dateTime = 1;</code>
         */
        public Builder setDateTime(
                java.lang.String value) {
            if (value == null) {
                throw new NullPointerException();
            }

            dateTime_ = value;
            onChanged();
            return this;
        }

        /**
         * <pre>
         * local datetime YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
         * </pre>
         *
         * <code>optional string dateTime = 1;</code>
         */
        public Builder clearDateTime() {

            dateTime_ = getDefaultInstance().getDateTime();
            onChanged();
            return this;
        }

        /**
         * <pre>
         * local datetime YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
         * </pre>
         *
         * <code>optional string dateTime = 1;</code>
         */
        public Builder setDateTimeBytes(
                com.google.protobuf.ByteString value) {
            if (value == null) {
                throw new NullPointerException();
            }
            checkByteStringIsUtf8(value);

            dateTime_ = value;
            onChanged();
            return this;
        }

        private java.lang.Object date_ = "";

        /**
         * <pre>
         * local date YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
         * </pre>
         *
         * <code>optional string date = 2;</code>
         */
        public java.lang.String getDate() {
            java.lang.Object ref = date_;
            if (!(ref instanceof java.lang.String)) {
                com.google.protobuf.ByteString bs =
                        (com.google.protobuf.ByteString) ref;
                java.lang.String s = bs.toStringUtf8();
                date_ = s;
                return s;
            } else {
                return (java.lang.String) ref;
            }
        }

        /**
         * <pre>
         * local date YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
         * </pre>
         *
         * <code>optional string date = 2;</code>
         */
        public com.google.protobuf.ByteString
        getDateBytes() {
            java.lang.Object ref = date_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8(
                                (java.lang.String) ref);
                date_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        /**
         * <pre>
         * local date YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
         * </pre>
         *
         * <code>optional string date = 2;</code>
         */
        public Builder setDate(
                java.lang.String value) {
            if (value == null) {
                throw new NullPointerException();
            }

            date_ = value;
            onChanged();
            return this;
        }

        /**
         * <pre>
         * local date YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
         * </pre>
         *
         * <code>optional string date = 2;</code>
         */
        public Builder clearDate() {

            date_ = getDefaultInstance().getDate();
            onChanged();
            return this;
        }

        /**
         * <pre>
         * local date YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
         * </pre>
         *
         * <code>optional string date = 2;</code>
         */
        public Builder setDateBytes(
                com.google.protobuf.ByteString value) {
            if (value == null) {
                throw new NullPointerException();
            }
            checkByteStringIsUtf8(value);

            date_ = value;
            onChanged();
            return this;
        }

        private java.lang.Object time_ = "";

        /**
         * <pre>
         * local time [H]H:[M]M:[S]S[.DDDDDD]
         * </pre>
         *
         * <code>optional string time = 3;</code>
         */
        public java.lang.String getTime() {
            java.lang.Object ref = time_;
            if (!(ref instanceof java.lang.String)) {
                com.google.protobuf.ByteString bs =
                        (com.google.protobuf.ByteString) ref;
                java.lang.String s = bs.toStringUtf8();
                time_ = s;
                return s;
            } else {
                return (java.lang.String) ref;
            }
        }

        /**
         * <pre>
         * local time [H]H:[M]M:[S]S[.DDDDDD]
         * </pre>
         *
         * <code>optional string time = 3;</code>
         */
        public com.google.protobuf.ByteString
        getTimeBytes() {
            java.lang.Object ref = time_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8(
                                (java.lang.String) ref);
                time_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        /**
         * <pre>
         * local time [H]H:[M]M:[S]S[.DDDDDD]
         * </pre>
         *
         * <code>optional string time = 3;</code>
         */
        public Builder setTime(
                java.lang.String value) {
            if (value == null) {
                throw new NullPointerException();
            }

            time_ = value;
            onChanged();
            return this;
        }

        /**
         * <pre>
         * local time [H]H:[M]M:[S]S[.DDDDDD]
         * </pre>
         *
         * <code>optional string time = 3;</code>
         */
        public Builder clearTime() {

            time_ = getDefaultInstance().getTime();
            onChanged();
            return this;
        }

        /**
         * <pre>
         * local time [H]H:[M]M:[S]S[.DDDDDD]
         * </pre>
         *
         * <code>optional string time = 3;</code>
         */
        public Builder setTimeBytes(
                com.google.protobuf.ByteString value) {
            if (value == null) {
                throw new NullPointerException();
            }
            checkByteStringIsUtf8(value);

            time_ = value;
            onChanged();
            return this;
        }

        private java.lang.Object timeZone_ = "";

        /**
         * <pre>
         * local timeZone continent/[region/]city
         * </pre>
         *
         * <code>optional string timeZone = 4;</code>
         */
        public java.lang.String getTimeZone() {
            java.lang.Object ref = timeZone_;
            if (!(ref instanceof java.lang.String)) {
                com.google.protobuf.ByteString bs =
                        (com.google.protobuf.ByteString) ref;
                java.lang.String s = bs.toStringUtf8();
                timeZone_ = s;
                return s;
            } else {
                return (java.lang.String) ref;
            }
        }

        /**
         * <pre>
         * local timeZone continent/[region/]city
         * </pre>
         *
         * <code>optional string timeZone = 4;</code>
         */
        public com.google.protobuf.ByteString
        getTimeZoneBytes() {
            java.lang.Object ref = timeZone_;
            if (ref instanceof String) {
                com.google.protobuf.ByteString b =
                        com.google.protobuf.ByteString.copyFromUtf8(
                                (java.lang.String) ref);
                timeZone_ = b;
                return b;
            } else {
                return (com.google.protobuf.ByteString) ref;
            }
        }

        /**
         * <pre>
         * local timeZone continent/[region/]city
         * </pre>
         *
         * <code>optional string timeZone = 4;</code>
         */
        public Builder setTimeZone(
                java.lang.String value) {
            if (value == null) {
                throw new NullPointerException();
            }

            timeZone_ = value;
            onChanged();
            return this;
        }

        /**
         * <pre>
         * local timeZone continent/[region/]city
         * </pre>
         *
         * <code>optional string timeZone = 4;</code>
         */
        public Builder clearTimeZone() {

            timeZone_ = getDefaultInstance().getTimeZone();
            onChanged();
            return this;
        }

        /**
         * <pre>
         * local timeZone continent/[region/]city
         * </pre>
         *
         * <code>optional string timeZone = 4;</code>
         */
        public Builder setTimeZoneBytes(
                com.google.protobuf.ByteString value) {
            if (value == null) {
                throw new NullPointerException();
            }
            checkByteStringIsUtf8(value);

            timeZone_ = value;
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


        // @@protoc_insertion_point(builder_scope:datahem.protobuf.measurementprotocol.v2.Time)
    }

    // @@protoc_insertion_point(class_scope:datahem.protobuf.measurementprotocol.v2.Time)
    private static final org.datahem.protobuf.measurementprotocol.v2.Time DEFAULT_INSTANCE;

    static {
        DEFAULT_INSTANCE = new org.datahem.protobuf.measurementprotocol.v2.Time();
    }

    public static org.datahem.protobuf.measurementprotocol.v2.Time getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<Time>
            PARSER = new com.google.protobuf.AbstractParser<Time>() {
        public Time parsePartialFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return new Time(input, extensionRegistry);
        }
    };

    public static com.google.protobuf.Parser<Time> parser() {
        return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<Time> getParserForType() {
        return PARSER;
    }

    public org.datahem.protobuf.measurementprotocol.v2.Time getDefaultInstanceForType() {
        return DEFAULT_INSTANCE;
    }

}

