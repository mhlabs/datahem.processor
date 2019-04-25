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

public interface LatencyOrBuilder extends
    // @@protoc_insertion_point(interface_extends:datahem.protobuf.measurementprotocol.v2.Latency)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   *dns	The total time (in milliseconds) all samples spent in DNS lookup for this page.
   * </pre>
   *
   * <code>optional int32 domainLookupTime = 1;</code>
   */
  int getDomainLookupTime();

  /**
   * <pre>
   *clt	The time (in milliseconds), including the network time from users' locations to the site's server, the browser takes to parse the document and execute deferred and parser-inserted scripts (DOMContentLoaded).
   * </pre>
   *
   * <code>optional int32 domContentLoadedTime = 2;</code>
   */
  int getDomContentLoadedTime();

  /**
   * <pre>
   *dit	The time (in milliseconds), including the network time from users' locations to the site's server, the browser takes to parse the document (DOMInteractive).
   * </pre>
   *
   * <code>optional int32 domInteractiveTime = 3;</code>
   */
  int getDomInteractiveTime();

  /**
   * <pre>
   *pdt	INTEGER	The total time (in milliseconds) to download this page among all samples.
   * </pre>
   *
   * <code>optional int32 pageDownloadTime = 4;</code>
   */
  int getPageDownloadTime();

  /**
   * <pre>
   *plt	Total time (in milliseconds), from pageview initiation (e.g., a click on a page link) to page load completion in the browser, the pages in the sample set take to load.
   * </pre>
   *
   * <code>optional int32 pageLoadTime = 5;</code>
   */
  int getPageLoadTime();

  /**
   * <pre>
   *rrt	The total time (in milliseconds) all samples spent in redirects before fetching this page. If there are no redirects, this is 0.
   * </pre>
   *
   * <code>optional int32 redirectionTime = 6;</code>
   */
  int getRedirectionTime();

  /**
   * <pre>
   *tcp	Total time (in milliseconds) all samples spent in establishing a TCP connection to this page.
   * </pre>
   *
   * <code>optional int32 serverConnectionTime = 7;</code>
   */
  int getServerConnectionTime();

  /**
   * <pre>
   *srt	The total time (in milliseconds) the site's server takes to respond to users' requests among all samples; this includes the network time from users' locations to the server.
   * </pre>
   *
   * <code>optional int32 serverResponseTime = 8;</code>
   */
  int getServerResponseTime();

  /**
   * <pre>
   *utc	For easier reporting purposes, this is used to categorize all user timing variables into logical groups.
   * </pre>
   *
   * <code>optional string userTimingCategory = 9;</code>
   */
  java.lang.String getUserTimingCategory();
  /**
   * <pre>
   *utc	For easier reporting purposes, this is used to categorize all user timing variables into logical groups.
   * </pre>
   *
   * <code>optional string userTimingCategory = 9;</code>
   */
  com.google.protobuf.ByteString
      getUserTimingCategoryBytes();

  /**
   * <pre>
   *utl	The name of the resource's action being tracked.
   * </pre>
   *
   * <code>optional string userTimingLabel = 10;</code>
   */
  java.lang.String getUserTimingLabel();
  /**
   * <pre>
   *utl	The name of the resource's action being tracked.
   * </pre>
   *
   * <code>optional string userTimingLabel = 10;</code>
   */
  com.google.protobuf.ByteString
      getUserTimingLabelBytes();

  /**
   * <pre>
   *utt 	Total number of milliseconds for user timing.
   * </pre>
   *
   * <code>optional int32 userTimingValue = 11;</code>
   */
  int getUserTimingValue();

  /**
   * <pre>
   *utv	Variable used to add flexibility to visualize user timings in the reports.
   * </pre>
   *
   * <code>optional string userTimingVariable = 12;</code>
   */
  java.lang.String getUserTimingVariable();
  /**
   * <pre>
   *utv	Variable used to add flexibility to visualize user timings in the reports.
   * </pre>
   *
   * <code>optional string userTimingVariable = 12;</code>
   */
  com.google.protobuf.ByteString
      getUserTimingVariableBytes();
}