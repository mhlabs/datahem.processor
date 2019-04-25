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

public interface GeoOrBuilder extends
    // @@protoc_insertion_point(interface_extends:datahem.protobuf.measurementprotocol.v2.Geo)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   *X-AppEngine-Country. The country from which sessions originated, based on IP address.
   * </pre>
   *
   * <code>optional string country = 1;</code>
   */
  java.lang.String getCountry();
  /**
   * <pre>
   *X-AppEngine-Country. The country from which sessions originated, based on IP address.
   * </pre>
   *
   * <code>optional string country = 1;</code>
   */
  com.google.protobuf.ByteString
      getCountryBytes();

  /**
   * <pre>
   *X-AppEngine-Region. The region from which sessions originate, derived from IP addresses. In the U.S., a region is a state, such as New York.
   * </pre>
   *
   * <code>optional string region = 2;</code>
   */
  java.lang.String getRegion();
  /**
   * <pre>
   *X-AppEngine-Region. The region from which sessions originate, derived from IP addresses. In the U.S., a region is a state, such as New York.
   * </pre>
   *
   * <code>optional string region = 2;</code>
   */
  com.google.protobuf.ByteString
      getRegionBytes();

  /**
   * <pre>
   *X-AppEngine-City. Users' city, derived from their IP addresses or Geographical IDs.
   * </pre>
   *
   * <code>optional string city = 3;</code>
   */
  java.lang.String getCity();
  /**
   * <pre>
   *X-AppEngine-City. Users' city, derived from their IP addresses or Geographical IDs.
   * </pre>
   *
   * <code>optional string city = 3;</code>
   */
  com.google.protobuf.ByteString
      getCityBytes();

  /**
   * <pre>
   *X-AppEngine-CityLatLong. The approximate latitude and longitude of users' city, derived from their IP addresses or Geographical IDs. Locations north of the equator have positive latitudes and locations south of the equator have negative latitudes.
   * </pre>
   *
   * <code>optional string cityLatLong = 4;</code>
   */
  java.lang.String getCityLatLong();
  /**
   * <pre>
   *X-AppEngine-CityLatLong. The approximate latitude and longitude of users' city, derived from their IP addresses or Geographical IDs. Locations north of the equator have positive latitudes and locations south of the equator have negative latitudes.
   * </pre>
   *
   * <code>optional string cityLatLong = 4;</code>
   */
  com.google.protobuf.ByteString
      getCityLatLongBytes();
}