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

import org.datahem.protobuf.collector.v1.CollectorPayloadEntityProto.CollectorPayloadEntity;
import org.datahem.processor.measurementprotocol.utils.PayloadToMPEntityFn;
import org.datahem.processor.measurementprotocol.utils.MPEntityToTableRowFn;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MeasurementProtocolPipelineTest {

	@Rule public transient TestPipeline p = TestPipeline.create();

	private static String siteSearchPayload = "v=1&_v=j66&a=1140262547&t=pageview&_s=1&dl=https%3A%2F%2Fbeta.datahem.org%2Fsok%3Fq%3Dpasta%20knyten%26page%3D1%26pageSize%3D25&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=Frukt%20%26%20Gr%C3%B6nt%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&_u=aCDAAEAL~&jid=&gjid=&cid=1062063169.1517835391&uid=947563&tid=UA-7391864-18&_gid=616449507.1520411256&gtm=G2rP9BRHCJ&z=631938637&cd1=gold&cd2=family&cm1=25";
	private static TableRow siteSearchTR = new TableRow().set("timestamp", 0).set("contributor_username", "user1");

	private static CollectorPayloadEntity test(Map headers, String payload){
			return CollectorPayloadEntity.newBuilder()
				.setPayload(payload)
				.putAllHeaders(headers)
				.setEpochMillis("1519977053236")
				.setUuid("5bd43e1a-8217-4020-826f-3c7f0b763c32")
				.build();
	}

	@Test
	public void testMeasurementProtocolPipeline() throws Exception {
		//@Rule public transient TestPipeline p = TestPipeline.create();

		Map<String,String> headers = new HashMap<String, String>();
		headers.put("X-AppEngine-Country","SE");
		headers.put("X-AppEngine-City","stockholm");
		headers.put("User-Agent","Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36");

		CollectorPayloadEntity cpe = test(headers, siteSearchPayload);
		
		PCollection<CollectorPayloadEntity> input = p.apply(
				Create.of(
					Arrays.asList(
						cpe)));
		

		PCollection<MPEntity> step1 =
			input
			.apply(ParDo.of(new PayloadToMPEntityFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),//options.getSearchEnginesPattern(),
				StaticValueProvider.of(".*(beta.datahem.org|www.datahem.org).*"),//options.getIgnoredReferersPattern(), 
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),//options.getSocialNetworksPattern(),
				StaticValueProvider.of(".*(beta.datahem.org|www.datahem.org).*"), // match all hostnames
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"), // match no user agent
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),//options.getSiteSearchPattern(),
				StaticValueProvider.of("Europe/Stockholm")//options.getTimeZone()
				)));
		
		PCollection<TableRow> output =
			step1
				.apply(ParDo.of(new MPEntityToTableRowFn()));

      
       PAssert.that(output)
        .containsInAnyOrder(
            Arrays.asList(siteSearchTR));

    p.run();

    }
}
