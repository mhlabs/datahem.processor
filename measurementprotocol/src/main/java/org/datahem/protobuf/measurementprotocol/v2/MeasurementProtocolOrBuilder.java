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

public interface MeasurementProtocolOrBuilder extends
    // @@protoc_insertion_point(interface_extends:datahem.protobuf.measurementprotocol.v2.MeasurementProtocol)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   *t
   * </pre>
   *
   * <code>optional string hitType = 1;</code>
   */
  java.lang.String getHitType();
  /**
   * <pre>
   *t
   * </pre>
   *
   * <code>optional string hitType = 1;</code>
   */
  com.google.protobuf.ByteString
      getHitTypeBytes();

  /**
   * <pre>
   *cid
   * </pre>
   *
   * <code>optional string clientId = 2;</code>
   */
  java.lang.String getClientId();
  /**
   * <pre>
   *cid
   * </pre>
   *
   * <code>optional string clientId = 2;</code>
   */
  com.google.protobuf.ByteString
      getClientIdBytes();

  /**
   * <pre>
   *uid
   * </pre>
   *
   * <code>optional string userId = 3;</code>
   */
  java.lang.String getUserId();
  /**
   * <pre>
   *uid
   * </pre>
   *
   * <code>optional string userId = 3;</code>
   */
  com.google.protobuf.ByteString
      getUserIdBytes();

  /**
   * <code>optional string hitId = 4;</code>
   */
  java.lang.String getHitId();
  /**
   * <code>optional string hitId = 4;</code>
   */
  com.google.protobuf.ByteString
      getHitIdBytes();

  /**
   * <pre>
   *local date as partition field.
   * </pre>
   *
   * <code>optional string date = 5;</code>
   */
  java.lang.String getDate();
  /**
   * <pre>
   *local date as partition field.
   * </pre>
   *
   * <code>optional string date = 5;</code>
   */
  com.google.protobuf.ByteString
      getDateBytes();

  /**
   * <pre>
   *ni	If this hit was a non-interaction hit, this is true.
   * </pre>
   *
   * <code>optional bool nonInteraction = 6;</code>
   */
  boolean getNonInteraction();

  /**
   * <pre>
   *v
   * </pre>
   *
   * <code>optional string version = 7;</code>
   */
  java.lang.String getVersion();
  /**
   * <pre>
   *v
   * </pre>
   *
   * <code>optional string version = 7;</code>
   */
  com.google.protobuf.ByteString
      getVersionBytes();

  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.CustomDimension customDimensions = 8;</code>
   */
  java.util.List<org.datahem.protobuf.measurementprotocol.v2.CustomDimension> 
      getCustomDimensionsList();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.CustomDimension customDimensions = 8;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.CustomDimension getCustomDimensions(int index);
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.CustomDimension customDimensions = 8;</code>
   */
  int getCustomDimensionsCount();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.CustomDimension customDimensions = 8;</code>
   */
  java.util.List<? extends org.datahem.protobuf.measurementprotocol.v2.CustomDimensionOrBuilder> 
      getCustomDimensionsOrBuilderList();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.CustomDimension customDimensions = 8;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.CustomDimensionOrBuilder getCustomDimensionsOrBuilder(
      int index);

  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.CustomMetric customMetrics = 9;</code>
   */
  java.util.List<org.datahem.protobuf.measurementprotocol.v2.CustomMetric> 
      getCustomMetricsList();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.CustomMetric customMetrics = 9;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.CustomMetric getCustomMetrics(int index);
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.CustomMetric customMetrics = 9;</code>
   */
  int getCustomMetricsCount();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.CustomMetric customMetrics = 9;</code>
   */
  java.util.List<? extends org.datahem.protobuf.measurementprotocol.v2.CustomMetricOrBuilder> 
      getCustomMetricsOrBuilderList();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.CustomMetric customMetrics = 9;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.CustomMetricOrBuilder getCustomMetricsOrBuilder(
      int index);

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Device device = 10;</code>
   */
  boolean hasDevice();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Device device = 10;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Device getDevice();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Device device = 10;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.DeviceOrBuilder getDeviceOrBuilder();

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Ecommerce ecommerce = 11;</code>
   */
  boolean hasEcommerce();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Ecommerce ecommerce = 11;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Ecommerce getEcommerce();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Ecommerce ecommerce = 11;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.EcommerceOrBuilder getEcommerceOrBuilder();

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Event event = 12;</code>
   */
  boolean hasEvent();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Event event = 12;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Event getEvent();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Event event = 12;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.EventOrBuilder getEventOrBuilder();

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Exception exception = 13;</code>
   */
  boolean hasException();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Exception exception = 13;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Exception getException();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Exception exception = 13;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.ExceptionOrBuilder getExceptionOrBuilder();

  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Experiment experiment = 14;</code>
   */
  java.util.List<org.datahem.protobuf.measurementprotocol.v2.Experiment> 
      getExperimentList();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Experiment experiment = 14;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Experiment getExperiment(int index);
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Experiment experiment = 14;</code>
   */
  int getExperimentCount();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Experiment experiment = 14;</code>
   */
  java.util.List<? extends org.datahem.protobuf.measurementprotocol.v2.ExperimentOrBuilder> 
      getExperimentOrBuilderList();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Experiment experiment = 14;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.ExperimentOrBuilder getExperimentOrBuilder(
      int index);

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Geo geo = 15;</code>
   */
  boolean hasGeo();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Geo geo = 15;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Geo getGeo();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Geo geo = 15;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.GeoOrBuilder getGeoOrBuilder();

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Latency latency = 16;</code>
   */
  boolean hasLatency();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Latency latency = 16;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Latency getLatency();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Latency latency = 16;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.LatencyOrBuilder getLatencyOrBuilder();

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Page page = 17;</code>
   */
  boolean hasPage();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Page page = 17;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Page getPage();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Page page = 17;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.PageOrBuilder getPageOrBuilder();

  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Product products = 18;</code>
   */
  java.util.List<org.datahem.protobuf.measurementprotocol.v2.Product> 
      getProductsList();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Product products = 18;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Product getProducts(int index);
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Product products = 18;</code>
   */
  int getProductsCount();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Product products = 18;</code>
   */
  java.util.List<? extends org.datahem.protobuf.measurementprotocol.v2.ProductOrBuilder> 
      getProductsOrBuilderList();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Product products = 18;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.ProductOrBuilder getProductsOrBuilder(
      int index);

  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Promotion promitions = 19;</code>
   */
  java.util.List<org.datahem.protobuf.measurementprotocol.v2.Promotion> 
      getPromitionsList();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Promotion promitions = 19;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Promotion getPromitions(int index);
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Promotion promitions = 19;</code>
   */
  int getPromitionsCount();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Promotion promitions = 19;</code>
   */
  java.util.List<? extends org.datahem.protobuf.measurementprotocol.v2.PromotionOrBuilder> 
      getPromitionsOrBuilderList();
  /**
   * <code>repeated .datahem.protobuf.measurementprotocol.v2.Promotion promitions = 19;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.PromotionOrBuilder getPromitionsOrBuilder(
      int index);

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Property property = 20;</code>
   */
  boolean hasProperty();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Property property = 20;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Property getProperty();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Property property = 20;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.PropertyOrBuilder getPropertyOrBuilder();

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Refund refund = 21;</code>
   */
  boolean hasRefund();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Refund refund = 21;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Refund getRefund();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Refund refund = 21;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.RefundOrBuilder getRefundOrBuilder();

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Social social = 22;</code>
   */
  boolean hasSocial();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Social social = 22;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Social getSocial();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Social social = 22;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.SocialOrBuilder getSocialOrBuilder();

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Time time = 23;</code>
   */
  boolean hasTime();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Time time = 23;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Time getTime();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Time time = 23;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.TimeOrBuilder getTimeOrBuilder();

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.TrafficSource trafficSource = 24;</code>
   */
  boolean hasTrafficSource();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.TrafficSource trafficSource = 24;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.TrafficSource getTrafficSource();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.TrafficSource trafficSource = 24;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.TrafficSourceOrBuilder getTrafficSourceOrBuilder();

  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Transaction transaction = 25;</code>
   */
  boolean hasTransaction();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Transaction transaction = 25;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.Transaction getTransaction();
  /**
   * <code>optional .datahem.protobuf.measurementprotocol.v2.Transaction transaction = 25;</code>
   */
  org.datahem.protobuf.measurementprotocol.v2.TransactionOrBuilder getTransactionOrBuilder();
}
