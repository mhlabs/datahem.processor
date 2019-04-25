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

public interface TimeOrBuilder extends
    // @@protoc_insertion_point(interface_extends:datahem.protobuf.measurementprotocol.v2.Time)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   * local datetime YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
   * </pre>
   *
   * <code>optional string dateTime = 1;</code>
   */
  java.lang.String getDateTime();
  /**
   * <pre>
   * local datetime YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
   * </pre>
   *
   * <code>optional string dateTime = 1;</code>
   */
  com.google.protobuf.ByteString
      getDateTimeBytes();

  /**
   * <pre>
   * local date YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
   * </pre>
   *
   * <code>optional string date = 2;</code>
   */
  java.lang.String getDate();
  /**
   * <pre>
   * local date YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
   * </pre>
   *
   * <code>optional string date = 2;</code>
   */
  com.google.protobuf.ByteString
      getDateBytes();

  /**
   * <pre>
   * local time [H]H:[M]M:[S]S[.DDDDDD]
   * </pre>
   *
   * <code>optional string time = 3;</code>
   */
  java.lang.String getTime();
  /**
   * <pre>
   * local time [H]H:[M]M:[S]S[.DDDDDD]
   * </pre>
   *
   * <code>optional string time = 3;</code>
   */
  com.google.protobuf.ByteString
      getTimeBytes();

  /**
   * <pre>
   * local timeZone continent/[region/]city
   * </pre>
   *
   * <code>optional string timeZone = 4;</code>
   */
  java.lang.String getTimeZone();
  /**
   * <pre>
   * local timeZone continent/[region/]city
   * </pre>
   *
   * <code>optional string timeZone = 4;</code>
   */
  com.google.protobuf.ByteString
      getTimeZoneBytes();
}