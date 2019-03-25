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

public interface CustomDimensionOrBuilder extends
    // @@protoc_insertion_point(interface_extends:datahem.protobuf.measurementprotocol.v2.CustomDimension)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   *cd([0-9]{1,3}).The index of the custom dimension.
   * </pre>
   *
   * <code>optional int64 index = 1;</code>
   */
  long getIndex();

  /**
   * <pre>
   *cd[0-9]{1,3}).The value of the custom dimension.
   * </pre>
   *
   * <code>optional string value = 2;</code>
   */
  java.lang.String getValue();
  /**
   * <pre>
   *cd[0-9]{1,3}).The value of the custom dimension.
   * </pre>
   *
   * <code>optional string value = 2;</code>
   */
  com.google.protobuf.ByteString
      getValueBytes();
}
