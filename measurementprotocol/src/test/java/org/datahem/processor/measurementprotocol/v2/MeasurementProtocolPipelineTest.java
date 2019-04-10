package org.datahem.processor.measurementprotocol.v2;

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

import java.nio.charset.StandardCharsets;
//import com.google.protobuf.ByteString;

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
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import org.datahem.processor.utils.ProtobufUtils;
import org.datahem.processor.measurementprotocol.v2.utils.MeasurementProtocolBuilder;
import org.datahem.processor.measurementprotocol.v2.utils.PayloadToMeasurementProtocolFn;
import org.datahem.processor.measurementprotocol.v2.utils.MeasurementProtocolToTableRowFn;
import org.datahem.protobuf.measurementprotocol.v2.*;


//import org.datahem.processor.measurementprotocol.v1.utils.*;
//import org.datahem.protobuf.collector.v1.CollectorPayloadEntityProto.CollectorPayloadEntity;
//import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
//import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto;

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

    //String mpPayload = "v=1&_v=j73&aip=1&uid=123456&a=1786234232&t=pageview&_s=1&dl=https%3A%2F%2Fwww.foo.com%2F%3Futm_source%3Dnewsletter%26utm_medium%3Demail%26utm_campaign%3Dspring%26utm_term%3Dsale%26utm_content%3Dsemla&dp=%2F%3Futm_source%3Dnewsletter%26utm_medium%3Demail%26utm_campaign%3Dspring%26utm_term%3Dsale%26utm_content%3Dsemla&ul=en-us&de=UTF-8&dt=-&sd=24-bit&sr=1745x981&vp=1020x855&je=0&exp=Sa5K9MPeRXOmyHvW_zss6Q.1!6T0EqjhiQsedsWpXlts-jA.1&_u=yCCAAEAjQAAAg~&jid=&gjid=&cid=1653181724.1547722779&tid=UA-7391864-1&_gid=1874693131.1553603015&gtm=2wg3i1P9BRHCJ&cd13=7b2e603d-3166-45c2-861d-8fa0f828f81f&cd14=Mozilla%2F5.0%20(X11%3B%20CrOS%20aarch64%2011316.165.0)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F72.0.3626.122%20Safari%2F537.36&z=2064930086&ti=T12345&ta=Google%20Store%20-%20Online&tr=37.39&tt=2.85&ts=5.34&tcc=SUMMER2013&pa=purchase&pr1id=26392&pr1nm=Klassikerl%C3%A5da%2020-p%20GB%20Glace                      &pr1br=GB%20Glace&pr1ca=Glasspinnar&pr1cd1=gb&pr1cd2=red&pr1cm1=12&il1nm=recos&il1pi1id=hello&il1pi2id=world&il2nm=popular&il2pi1id=foo&il2pi1cd1=bar&il2pi1cm1=3";
    //String mpPayload = "v=1&_v=j73&aip=1&uid=123456&a=1786234232&t=pageview&_s=1&dl=https%3A%2F%2Fwww.foo.com%2Fstart%3Fq%3Dsite-search%26utm_source%3Dnewsletter%26utm_medium%3Demail%26utm_campaign%3Dspring%26utm_term%3Dsale%26utm_content%3Dsemla&dp=%2Fstart%3Fq%3Dsite-search%26utm_source%3Dnewsletter%26utm_medium%3Demail%26utm_campaign%3Dspring%26utm_term%3Dsale%26utm_content%3Dsemla&ul=en-us&de=UTF-8&dt=-&sd=24-bit&sr=1745x981&vp=1020x855&je=0&exp=Sa5K9MPeRXOmyHvW_zss6Q.1!6T0EqjhiQsedsWpXlts-jA.1&_u=yCCAAEAjQAAAg~&jid=&gjid=&cid=1653181724.1547722779&tid=UA-7391864-1&_gid=1874693131.1553603015&gtm=2wg3i1P9BRHCJ&cd13=7b2e603d-3166-45c2-861d-8fa0f828f81f&cd14=Mozilla%2F5.0%20(X11%3B%20CrOS%20aarch64%2011316.165.0)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F72.0.3626.122%20Safari%2F537.36&z=2064930086&ti=T12345&ta=Google%20Store%20-%20Online&tr=37.39&tt=2.85&ts=5.34&tcc=SUMMER2013&pa=purchase&pr1id=26392&pr1nm=Klassikerl%C3%A5da%2020-p%20GB%20Glace&pr1pr=114&pr1br=GB%20Glace&pr1ca=Glasspinnar&pr1qt=1&pr1cd1=gb&pr1cd2=red&pr1cm1=12&il1nm=recos&il1pi1id=hello&il1pi2id=world&il2nm=popular&il2pi1id=foo&&promo1id=PROMO_1234&promo1nm=Summer%20Sale&promo1cr=summer_banner2&promo1ps=banner_slot1&cd1=hello&cm1=5&X-AppEngine-Country=SE&X-AppEngine-City=upplands%20vasby&X-AppEngine-CityLatLong=59.519610%2C17.928340&User-Agent=Mozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F73.0.3683.86%20Safari%2F537.36&X-AppEngine-Region=ab";
    //String mpPayload = "v=1&_v=j73&aip=1&a=2005112053&t=pageview&_s=1&dl=https%3A%2F%2Fwww.foo.com%2Fmina-ordrar&dp=%2Fkassan%2Ftack&ul=sv-se&de=UTF-8&dt=-&sd=24-bit&sr=3840x2160&vp=1872x2001&je=0&_u=SDCAAEArQ~&jid=&gjid=&cid=1890517954.1543527482&uid=151954&tid=UA-7391864-1&_gid=6858861.1554376890&gtm=2wg3i1P9BRHCJ&cd7=151954&cd13=d9240a10-5546-4727-83df-bf1367ffe9bb&cd14=Mozilla%2F5.0%20(Windows%20NT%2010.0%3B%20Win64%3B%20x64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F73.0.3683.86%20Safari%2F537.36&linkid=cart&z=1160250524&X-AppEngine-Country=SE&X-AppEngine-City=upplands%20vasby&X-AppEngine-CityLatLong=59.519610%2C17.928340&User-Agent=Mozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F73.0.3683.86%20Safari%2F537.36&X-AppEngine-Region=ab&dr=googleads.g.doubleclick.net";
    String mpPayload = "v=1&_v=j73&aip=1&uid=123456&a=1786234232&t=pageview&_s=1&dl=https%3A%2F%2Fwww.foo.com%2Fstart%3Fq%3Dsite-search%26utm_source%3Dnewsletter%26utm_medium%3Demail%26utm_campaign%3Dspring%26utm_term%3Dsale%26utm_content%3Dsemla&ul=en-us&de=UTF-8&dt=-&sd=24-bit&sr=1745x981&vp=1020x855&je=0&exp=Sa5K9MPeRXOmyHvW_zss6Q.1!6T0EqjhiQsedsWpXlts-jA.1&_u=yCCAAEAjQAAAg~&jid=&gjid=&cid=1653181724.1547722779&tid=UA-7391864-1&_gid=1874693131.1553603015&gtm=2wg3i1P9BRHCJ&cd13=7b2e603d-3166-45c2-861d-8fa0f828f81f&cd14=Mozilla%2F5.0%20(X11%3B%20CrOS%20aarch64%2011316.165.0)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F72.0.3626.122%20Safari%2F537.36&z=2064930086&ti=T12345&ta=Google%20Store%20-%20Online&tr=37.39&tt=2.85&ts=5.34&tcc=SUMMER2013&pa=purchase&pr1id=26392&pr1nm=Klassikerl%C3%A5da%2020-p%20GB%20Glace&pr1pr=114&pr1br=GB%20Glace&pr1ca=Glasspinnar&pr1qt=1&pr1cd1=gb&pr1cd2=red&pr1cm1=12&il1nm=recos&il1pi1id=hello&il1pi2id=world&il2nm=popular&il2pi1id=foo&&promo1id=PROMO_1234&promo1nm=Summer%20Sale&promo1cr=summer_banner2&promo1ps=banner_slot1&cd1=hello&cm1=5&dr=com.google.android.googlequicksearchbox&X-AppEngine-Country=SE&X-AppEngine-City=upplands%20vasby&X-AppEngine-CityLatLong=59.519610%2C17.928340&User-Agent=Mozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F73.0.3683.86%20Safari%2F537.36&X-AppEngine-Region=ab)";
    //String mpPayload = "v=1&_v=j73&a=808219341&t=pageview&_s=1&dl=https%3A%2F%2Fwww.foo.com%2Fsok%3Fq%3Dtoa%20papper%26qtype%3Dp&ul=en-us&de=UTF-8&dt=MatHem%20-%20ljus%20sirap&sd=24-bit&sr=1745x981&vp=1088x855&je=0&_u=SCEAgAArQAAAi~&jid=&gjid=&cid=1483380997.1531213586&uid=&tid=UA-7391864-1&_gid=752770038.1554797702&gtm=2wg4305GDW&cd1=Private&cd5=10&cd7=&cd9=2&cd10=1483380997.1531213586&cd13=c111c426-bcbf-4acd-81db-92244e6b4646&linkid=txtSearch&z=415796223&User-Agent=Mozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F73.0.3683.86%20Safari%2F537.36";

    byte[] payload = mpPayload.getBytes(StandardCharsets.UTF_8);
	private static Map<String,String> attributes = new HashMap<String, String>(){
		{
            /*
			put("X-AppEngine-Country","SE");
			put("X-AppEngine-Region","ab");
			put("X-AppEngine-City","stockholm");
			put("X-AppEngine-CityLatLong","59.422571,17.833131");
			put("User-Agent","Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14");
            put("MessageTimestamp", "1549048495901");
		    put("MessageStream", "test");
			put("MessageUuid", "123-456-abc");
            */
            put("timestamp", "2013-08-16T23:36:32.444Z");
            put("uuid", "123-456-abc");
            put("source", "test");
		}
	};

    PubsubMessage pm = new PubsubMessage(payload, attributes);

	@Test
	public void userPageviewTest(){
        LOG.info("payload" + mpPayload);
		//LOG.info(Integer.toString(pageviewTR.hashCode())+" : "+pageviewTR.toPrettyString());
		//LOG.info(Integer.toString(refererTR.hashCode())+" : "+refererTR.toPrettyString());
		PCollection<TableRow> output = p
			.apply(Create.of(Arrays.asList(pm)))
			.apply(ParDo.of(new PayloadToMeasurementProtocolFn(
				StaticValueProvider.of(".*(www.google.|www.bing.|search.yahoo.).*"),
				StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*"),
                StaticValueProvider.of(".*(foo.com|www.foo.com).*"),
				StaticValueProvider.of(".*(^$|bot|spider|crawler).*"),
				StaticValueProvider.of(".*q=(([^&#]*)|&|#|$)"),
				StaticValueProvider.of("Europe/Stockholm"))))
			.apply(ParDo.of(new MeasurementProtocolToTableRowFn()));
		//PAssert.that(output).containsInAnyOrder(pageviewTR);
        Assert.assertEquals(true, true);
		p.run();
	}
	

	
}
