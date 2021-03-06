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

public interface PropertyOrBuilder extends
    // @@protoc_insertion_point(interface_extends:datahem.protobuf.measurementprotocol.v2.Property)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   *ds
   * </pre>
   *
   * <code>optional string dataSource = 1;</code>
   */
  java.lang.String getDataSource();
  /**
   * <pre>
   *ds
   * </pre>
   *
   * <code>optional string dataSource = 1;</code>
   */
  com.google.protobuf.ByteString
      getDataSourceBytes();

  /**
   * <pre>
   *gtm
   * </pre>
   *
   * <code>optional string gtmContainerId = 2;</code>
   */
  java.lang.String getGtmContainerId();
  /**
   * <pre>
   *gtm
   * </pre>
   *
   * <code>optional string gtmContainerId = 2;</code>
   */
  com.google.protobuf.ByteString
      getGtmContainerIdBytes();

  /**
   * <pre>
   *tid
   * </pre>
   *
   * <code>optional string trackingId = 3;</code>
   */
  java.lang.String getTrackingId();
  /**
   * <pre>
   *tid
   * </pre>
   *
   * <code>optional string trackingId = 3;</code>
   */
  com.google.protobuf.ByteString
      getTrackingIdBytes();
}
