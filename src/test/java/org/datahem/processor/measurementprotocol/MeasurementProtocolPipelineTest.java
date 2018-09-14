package org.datahem.processor.measurementprotocol;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 Robert Sahlin and MatHem Sverige AB
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * =========================LICENSE_END==================================
 */

import com.google.api.services.bigquery.model.TableRow;

import com.google.gson.Gson;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;
import org.datahem.processor.utils.FieldMapper;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;

import org.datahem.processor.measurementprotocol.utils.*;

import org.datahem.protobuf.collector.v1.CollectorPayloadEntityProto.CollectorPayloadEntity;
//import org.datahem.processor.measurementprotocol.utils.PayloadToMPEntityFn;
//import org.datahem.processor.measurementprotocol.utils.MPEntityToTableRowFn;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class MeasurementProtocolPipelineTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(MeasurementProtocolPipelineTest.class);
	
	@Rule public transient TestPipeline p = TestPipeline.create();

	private static TableRow parameterToTR(Parameter parameter){
		String s = "";
		switch(parameter.getValueType()){
			case "Integer":	s = "intValue";
				break;
			case "String":	s= "stringValue";
				break;
			case "Boolean":	s= "intValue";
				break;
			case "Double":	s= "floatValue";
		}
		return new TableRow()
			.set("key",parameter.getExampleParameterName())
			.set("value", new TableRow()
				.set(s, parameter.getExampleValue()));
	}

	/*
	 * User headers
	 */
	private static Map<String,String> user = new HashMap<String, String>(){
		{
			put("X-AppEngine-Country","SE");
			put("X-AppEngine-Region","ab");
			put("X-AppEngine-City","stockholm");
			put("X-AppEngine-CityLatLong","59.422571,17.833131");
			put("User-Agent","Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14");
		}
	};
	
	/*
	 * Bot headers
	 */
	private static Map<String,String> bot = new HashMap<String, String>(){
		{
			put("X-AppEngine-Country","SE");
			put("X-AppEngine-City","stockholm");
			put("User-Agent","Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)");
		}
	};
	
	/*
	 * Base entity
	 */

	private static BaseEntity baseEntity = new BaseEntity();
	
	private static TableRow baseTR = new TableRow()
		.set("type","pageview")
		.set("clientId","35009a79-1a05-49d7-b876-2b884d0f825b")
		.set("userId","as8eknlll")
		.set("utcTimestamp","2018-03-02 07:50:53")
		.set("epochMillis",1519977053236L)
		.set("date","2018-03-02");
	
	private static String basePayload = 
		baseEntity
		.getParameters()
		.stream()
		.map(p -> p.getExampleParameter() + "=" + FieldMapper.encode(p.getExampleValue()))
		.collect(Collectors.joining("&"));

	/*
	 * Pageview entity
	 */

	private static PageviewEntity pageviewEntity = new PageviewEntity();
	
	private static TableRow pageviewTR = baseTR.clone()
		.set("params", 
			Stream.concat(baseEntity.getParameters().stream(), pageviewEntity.getParameters().stream())
			.sorted(Comparator.comparing(Parameter::getExampleParameterName))
			.map(p -> parameterToTR(p))
			.collect(Collectors.toList()));	

	private static String pageviewPayload =
		Stream.concat(
			pageviewEntity
				.getParameters()
				.stream(), 
			baseEntity
				.getParameters()
				.stream()
		)
		.map(p -> p.getExampleParameter() + "=" + FieldMapper.encode(p.getExampleValue()))
		.collect(Collectors.joining("&"));


	/*
	 * Event entity
	 */

	private static EventEntity eventEntity = new EventEntity();
	
	private static List<Parameter> entityType = Arrays.asList(
		new Parameter("t", "String", null, 50, "hitType", true, "event"),
		new Parameter("et", "String", null, 50, "entityType", true, "event")
	);
	private static TableRow eventTR = baseTR.clone()
		.set("type","event")
		.set("params", 
			Stream.concat(baseEntity.getParameters().stream(), eventEntity.getParameters().stream())
			.sorted(Comparator.comparing(Parameter::getExampleParameterName))
			.map(o -> o.getExampleParameterName() == "hitType" ? new Parameter("t", "String", null, 50, "hitType", true, "event") : o)
			.map(o -> o.getExampleParameterName() == "entityType" ? new Parameter("et", "String", null, 50, "entityType", true, "event") : o)
			.map(p -> parameterToTR(p))
			.collect(Collectors.toList()));	

	private static String eventPayload =
		Stream.concat(
			eventEntity
				.getParameters()
				.stream(), 
			baseEntity
				.getParameters()
				.stream()
				.map(o -> o.getExampleParameterName() == "entityType" ? new Parameter("et", "String", null, 50, "entityType", true, "exception") : o)
				.map(o -> o.getExampleParameterName() == "hitType" ? new Parameter("t", "String", null, 50, "hitType", true, "event") : o)
		)
		.map(p -> p.getExampleParameter() + "=" + FieldMapper.encode(p.getExampleValue()))
		.collect(Collectors.joining("&"));

	/*
	 * Exception entity
	 */

	private static ExceptionEntity exceptionEntity = new ExceptionEntity();
	
	private static TableRow exceptionTR = baseTR.clone()
		.set("type","exception")
		.set("params", 
			Stream.concat(baseEntity.getParameters().stream(), exceptionEntity.getParameters().stream())
			.sorted(Comparator.comparing(Parameter::getExampleParameterName))
			.map(o -> o.getExampleParameterName() == "entityType" ? new Parameter("et", "String", null, 50, "entityType", true, "exception") : o)
			.map(o -> o.getExampleParameterName() == "hitType" ? new Parameter("t", "String", null, 50, "hitType", true, "exception") : o)
			.map(p -> parameterToTR(p))
			.collect(Collectors.toList()));	

		
	private static String exceptionPayload =
		Stream.concat(
			exceptionEntity
				.getParameters()
				.stream()
			, 
			baseEntity
				.getParameters()
				.stream()
				.map(o -> o.getExampleParameterName() == "entityType" ? new Parameter("et", "String", null, 50, "entityType", true, "exception") : o)
				.map(o -> o.getExampleParameterName() == "hitType" ? new Parameter("t", "String", null, 50, "hitType", true, "exception") : o)
		)
		.map(p -> p.getExampleParameter() + "=" + FieldMapper.encode(p.getExampleValue()))
		.collect(Collectors.joining("&"));


	/*
	 * SiteSearch entity
	 */

	private static SiteSearchEntity siteSearchEntity = new SiteSearchEntity();
	
	private static TableRow siteSearchTR = baseTR.clone()
		.set("type","siteSearch")
		.set("params", 
			Stream.concat(baseEntity.getParameters().stream(), siteSearchEntity.getParameters().stream())
			.sorted(Comparator.comparing(Parameter::getExampleParameterName))
			.map(o -> o.getExampleParameterName() == "entityType" ? new Parameter("et", "String", null, 50, "entityType", true, "siteSearch") : o)
			.map(o -> o.getExampleParameterName() == "url" ? new Parameter("dl", "String", null, 100, "url", false, "http://foo.com/home?q=creme%20fraiche") : o)
			.map(p -> parameterToTR(p))
			.collect(Collectors.toList()));
	
	private static TableRow siteSearchPageviewTR = baseTR.clone()
		.set("params", 
			Stream.concat(baseEntity.getParameters().stream(), pageviewEntity.getParameters().stream())
			.sorted(Comparator.comparing(Parameter::getExampleParameterName))
			.map(o -> o.getExampleParameterName() == "url" ? new Parameter("dl", "String", null, 100, "url", false, "http://foo.com/home?q=creme%20fraiche") : o)
			.map(p -> parameterToTR(p))
			.collect(Collectors.toList()));	

		
	private static String siteSearchPayload =
		Stream.concat(
			pageviewEntity
				.getParameters()
				.stream()
			, 
			baseEntity
				.getParameters()
				.stream()
				.map(o -> o.getExampleParameterName() == "entityType" ? new Parameter("et", "String", null, 50, "entityType", true, "siteSearch") : o)
				.map(o -> o.getExampleParameterName() == "url" ? new Parameter("dl", "String", null, 100, "url", false, "http://foo.com/home?q=creme%20fraiche") : o)
		)
		.map(p -> p.getExampleParameter() + "=" + FieldMapper.encode(p.getExampleValue()))
		.collect(Collectors.joining("&"));


	/*
	 * Social entity
	 */

	private static SocialEntity socialEntity = new SocialEntity();
	
	private static TableRow socialTR = baseTR.clone()
		.set("type","social")
		.set("params", 
			Stream.concat(baseEntity.getParameters().stream(), socialEntity.getParameters().stream())
			.sorted(Comparator.comparing(Parameter::getExampleParameterName))
			.map(o -> o.getExampleParameterName() == "entityType" ? new Parameter("et", "String", null, 50, "entityType", true, "social") : o)
			.map(o -> o.getExampleParameterName() == "hitType" ? new Parameter("t", "String", null, 50, "hitType", true, "social") : o)
			.map(p -> parameterToTR(p))
			.collect(Collectors.toList()));	

		
	private static String socialPayload =
		Stream.concat(
			socialEntity
				.getParameters()
				.stream()
			, 
			baseEntity
				.getParameters()
				.stream()
				.map(o -> o.getExampleParameterName() == "entityType" ? new Parameter("et", "String", null, 50, "entityType", true, "social") : o)
				.map(o -> o.getExampleParameterName() == "hitType" ? new Parameter("t", "String", null, 50, "hitType", true, "social") : o)
		)
		.map(p -> p.getExampleParameter() + "=" + FieldMapper.encode(p.getExampleValue()))
		.collect(Collectors.joining("&"));

	/*
	 * Social entity
	 */

	private static TimingEntity timingEntity = new TimingEntity();
	
	private static TableRow timingTR = baseTR.clone()
		.set("type","timing")
		.set("params", 
			Stream.concat(baseEntity.getParameters().stream(), timingEntity.getParameters().stream())
			.sorted(Comparator.comparing(Parameter::getExampleParameterName))
			.map(o -> o.getExampleParameterName() == "entityType" ? new Parameter("et", "String", null, 50, "entityType", true, "timing") : o)
			.map(o -> o.getExampleParameterName() == "hitType" ? new Parameter("t", "String", null, 50, "hitType", true, "timing") : o)
			.map(p -> parameterToTR(p))
			.collect(Collectors.toList()));	

		
	private static String timingPayload =
		Stream.concat(
			timingEntity
				.getParameters()
				.stream()
			, 
			baseEntity
				.getParameters()
				.stream()
				.map(o -> o.getExampleParameterName() == "entityType" ? new Parameter("et", "String", null, 50, "entityType", true, "timing") : o)
				.map(o -> o.getExampleParameterName() == "hitType" ? new Parameter("t", "String", null, 50, "hitType", true, "timing") : o)
		)
		.map(p -> p.getExampleParameter() + "=" + FieldMapper.encode(p.getExampleValue()))
		.collect(Collectors.joining("&"));

	/*
	 * Transaction entity
	 */

	private static TransactionEntity transactionEntity = new TransactionEntity();
	
	private static TableRow transactionTR = baseTR.clone()
		.set("type","transaction")
		.set("params", 
			Stream.concat(
				baseEntity.getParameters().stream(), 
				transactionEntity.getParameters().stream())
			.sorted(Comparator.comparing(Parameter::getExampleParameterName))
			.map(o -> o.getExampleParameterName() == "entityType" ? new Parameter("et", "String", null, 50, "entityType", true, "transaction") : o)
			.map(p -> parameterToTR(p))
			.collect(Collectors.toList()));
			
			
		
	private static String transactionPayload =
		Stream.concat(
			Stream.concat(
				pageviewEntity
					.getParameters()
					.stream(),
				transactionEntity
					.getParameters()
					.stream())
			,
			Stream.concat(
				baseEntity
					.getParameters()
					.stream()
					.filter(o -> o.getExampleParameterName() != "entityType"),
				Arrays.asList(new Parameter("pa", "String", null, 50, "productAction", true,"purchase")).stream())
		)
		.map(p -> p.getExampleParameter() + "=" + FieldMapper.encode(p.getExampleValue()))
		.collect(Collectors.joining("&"));


/*
 * **************************************
 */


	private static CollectorPayloadEntity cpeBuilder(Map headers, String payload){
			return CollectorPayloadEntity.newBuilder()
				.setPayload(payload)
				.putAllHeaders(headers)
				.setEpochMillis("1519977053236")
				.setUuid("5bd43e1a-8217-4020-826f-3c7f0b763c32")
				.build();
	}


	@Test
	public void userPageviewTest() throws Exception {
		PCollection<TableRow> output = p
			.apply(Create.of(Arrays.asList(cpeBuilder(user, pageviewPayload))))
			.apply(ParDo.of(new PayloadToMPEntityFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"),
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),
				StaticValueProvider.of("Europe/Stockholm"))))
			.apply(ParDo.of(new MPEntityToTableRowFn()));
		PAssert.that(output).containsInAnyOrder(pageviewTR);
		p.run();
	}
	
	@Test
	public void botPageviewTest() throws Exception {
		PCollection<TableRow> output = p
			.apply(Create.of(Arrays.asList(cpeBuilder(bot, pageviewPayload))))
			.apply(ParDo.of(new PayloadToMPEntityFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"),
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),
				StaticValueProvider.of("Europe/Stockholm"))))
			.apply(ParDo.of(new MPEntityToTableRowFn()));
		PAssert.that(output).containsInAnyOrder();
		p.run();
	}

	@Test
	public void userEventTest() throws Exception {
		PCollection<TableRow> output = p
			.apply(Create.of(Arrays.asList(cpeBuilder(user, eventPayload))))
			.apply(ParDo.of(new PayloadToMPEntityFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"),
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),
				StaticValueProvider.of("Europe/Stockholm"))))
			.apply(ParDo.of(new MPEntityToTableRowFn()));
		PAssert.that(output).containsInAnyOrder(eventTR);
		p.run();
	}


	@Test
	public void userExceptionTest() throws Exception {
		PCollection<TableRow> output = p
			.apply(Create.of(Arrays.asList(cpeBuilder(user, exceptionPayload))))
			.apply(ParDo.of(new PayloadToMPEntityFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"),
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),
				StaticValueProvider.of("Europe/Stockholm"))))
			.apply(ParDo.of(new MPEntityToTableRowFn()));
		PAssert.that(output).containsInAnyOrder(exceptionTR);
		p.run();
	}
	
	@Test
	public void userSiteSearchTest() throws Exception {
		PCollection<TableRow> output = p
			.apply(Create.of(Arrays.asList(cpeBuilder(user, siteSearchPayload))))
			.apply(ParDo.of(new PayloadToMPEntityFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"),
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),
				StaticValueProvider.of("Europe/Stockholm"))))
			.apply(ParDo.of(new MPEntityToTableRowFn()));
		PAssert.that(output).containsInAnyOrder(siteSearchTR, siteSearchPageviewTR);
		p.run();
	}

	@Test
	public void userSocialTest() throws Exception {
		PCollection<TableRow> output = p
			.apply(Create.of(Arrays.asList(cpeBuilder(user, socialPayload))))
			.apply(ParDo.of(new PayloadToMPEntityFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"),
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),
				StaticValueProvider.of("Europe/Stockholm"))))
			.apply(ParDo.of(new MPEntityToTableRowFn()));
		PAssert.that(output).containsInAnyOrder(socialTR);
		p.run();
	}


	@Test
	public void userTimingTest() throws Exception {
		PCollection<TableRow> output = p
			.apply(Create.of(Arrays.asList(cpeBuilder(user, timingPayload))))
			.apply(ParDo.of(new PayloadToMPEntityFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"),
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),
				StaticValueProvider.of("Europe/Stockholm"))))
			.apply(ParDo.of(new MPEntityToTableRowFn()));
		PAssert.that(output).containsInAnyOrder(timingTR);
		p.run();
	}
	
	@Test
	public void userTransactionTest() throws Exception {
		LOG.info(Integer.toString(transactionTR.hashCode())+" : "+transactionTR.toPrettyString());	
		PCollection<TableRow> output = p
			.apply(Create.of(Arrays.asList(cpeBuilder(user, transactionPayload))))
			.apply(ParDo.of(new PayloadToMPEntityFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"),
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),
				StaticValueProvider.of("Europe/Stockholm"))))
			.apply(ParDo.of(new MPEntityToTableRowFn()));
		PAssert.that(output).containsInAnyOrder(transactionTR, pageviewTR);
		p.run();
	}
}
