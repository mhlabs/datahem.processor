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

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;

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

@RunWith(JUnit4.class)
public class MeasurementProtocolPipelineTest {
	
	@Rule public transient TestPipeline p = TestPipeline.create();

	//Helper function to create TableRows
	private static TableRow paramToTR(Param param){
		return new TableRow()
			.set("key",param.getKey())
			.set("value", new TableRow()
				.set(param.getValueType(), param.getValue()));
	}

	private static TableRow parameterToTR(Parameter parameter){
		return new TableRow()
			.set("key",parameter.getParameterName())
			.set("value", new TableRow()
				.set(parameter.getValueType(), parameter.getExampleValue()));
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
			put("User-Agent","Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36");
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
		.set("clientId","1062063169.1517835391")
		.set("userId","947563")
		.set("utcTimestamp","2018-03-02 07:50:53")
		.set("epochMillis",1519977053236L)
		.set("date","2018-03-02");
	private static String basePayload = "v=1&_v=j66&a=1140262547&t=pageview&_s=1&dl=https%3A%2F%2Fwww.datahem.org%2Fvaror%2Fkott-o-chark&dp=%2Fvaror%2Fkott-o-chark&dt=Frukt%20%26%20Gr%C3%B6nt%20%7C%20Mathem&cid=1062063169.1517835391&uid=947563&tid=UA-1234567-89&jid=&gtm=G7rP2BRHCI&cd1=gold&cd2=family&cm1=25";

	/*
	 * Pageview entity
	 */

	private static PageviewEntity pageviewEntity = new PageviewEntity();
	private static TableRow pageviewTR = baseTR.clone().set("params", Stream.concat(baseEntity.getParameters().stream(), pageviewEntity.getParameters().stream()).sorted(Comparator.comparing(Parameter::getParameterName)).map(p -> parameterToTR(p)).collect(Collectors.toList()));	
	private static String pageviewPayload = "ul=sv&de=UTF-8&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&fl=10%201%20r103";
	
	/*
	 * Event entity
	 */


	private static List<Param> eventParams = Arrays.asList(
		new Param("eventCategory", "stringValue", "/varor/kott-o-chark"), 
		new Param("eventAction", "stringValue", "www.datahem.org"),
		new Param("eventLabel", "stringValue", "947563"),
		new Param("eventValue", "intValue", 25)
	);


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
		String payload = basePayload + "&" + pageviewPayload;//"v=1&_v=j66&a=1140262547&t=pageview&_s=1&dl=https%3A%2F%2Fwww.datahem.org%2Fvaror%2Fkott-o-chark&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=Frukt%20%26%20Gr%C3%B6nt%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&cid=1062063169.1517835391&uid=947563&tid=UA-1234567-89&jid=&gtm=G7rP2BRHCI&cd1=gold&cd2=family&cm1=25";
		PCollection<TableRow> output = p
		.apply(Create.of(Arrays.asList(cpeBuilder(user, payload))))
		.apply(ParDo.of(new PayloadToMPEntityFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),
				StaticValueProvider.of(".*(beta.datahem.org|www.datahem.org).*"),
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),
				StaticValueProvider.of(".*(beta.datahem.org|www.datahem.org).*"),
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"),
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),
				StaticValueProvider.of("Europe/Stockholm"))))
		.apply(ParDo.of(new MPEntityToTableRowFn()));
		PAssert.that(output).containsInAnyOrder(Arrays.asList(pageviewTR));
		p.run();
	}
	
	/*
	@Test
	public void botPageviewTest() throws Exception {
		String payload = "v=1&_v=j66&a=1140262547&t=pageview&_s=1&dl=https%3A%2F%2Fwww.datahem.org%2Fvaror%2Fkott-o-chark&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=Frukt%20%26%20Gr%C3%B6nt%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&cid=1062063169.1517835391&uid=947563&tid=UA-1234567-89&jid=&gtm=G7rP2BRHCI&cd1=gold&cd2=family&cm1=25";
		PCollection<TableRow> output = p
		.apply(Create.of(Arrays.asList(cpeBuilder(bot, payload))))
		.apply(ParDo.of(new PayloadToMPEntityFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),
				StaticValueProvider.of(".*(beta.datahem.org|www.datahem.org).*"),
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),
				StaticValueProvider.of(".*(beta.datahem.org|www.datahem.org).*"),
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"),
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),
				StaticValueProvider.of("Europe/Stockholm"))))
		.apply(ParDo.of(new MPEntityToTableRowFn()));
		PAssert.that(output).containsInAnyOrder();
		p.run();
	}
	
			@Test
	public void userEventTest() throws Exception {
		String payload = "v=1&_v=j66&a=1140262547&t=pageview&_s=1&dl=https%3A%2F%2Fwww.datahem.org%2Fvaror%2Fkott-o-chark&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=Frukt%20%26%20Gr%C3%B6nt%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&cid=1062063169.1517835391&uid=947563&tid=UA-1234567-89&jid=&gtm=G7rP2BRHCI&cd1=gold&cd2=family&cm1=25";
		PCollection<TableRow> output = p
		.apply(Create.of(Arrays.asList(cpeBuilder(bot, payload))))
		.apply(ParDo.of(new PayloadToMPEntityFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),
				StaticValueProvider.of(".*(beta.datahem.org|www.datahem.org).*"),
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),
				StaticValueProvider.of(".*(beta.datahem.org|www.datahem.org).*"),
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"),
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),
				StaticValueProvider.of("Europe/Stockholm"))))
		.apply(ParDo.of(new MPEntityToTableRowFn()));
		PAssert.that(output).containsInAnyOrder(Arrays.asList(eventTR));
		p.run();
	}*/
}
