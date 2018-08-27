package org.datahem.processor.measurementprotocol.utils;

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

import org.datahem.processor.utils.FieldMapper;
import org.datahem.processor.measurementprotocol.utils.PageviewEntity;
import org.datahem.processor.measurementprotocol.utils.EventEntity;
import org.datahem.processor.measurementprotocol.utils.ExceptionEntity;
import org.datahem.processor.measurementprotocol.utils.TimingEntity;
import org.datahem.processor.measurementprotocol.utils.ProductEntity;
import org.datahem.processor.measurementprotocol.utils.TransactionEntity;
import org.datahem.processor.measurementprotocol.utils.SocialEntity;
import org.datahem.processor.measurementprotocol.utils.TrafficEntity;
import org.datahem.processor.measurementprotocol.utils.PromotionEntity;
import org.datahem.processor.measurementprotocol.utils.ProductImpressionEntity;
import org.datahem.processor.measurementprotocol.utils.SiteSearchEntity;

import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.datahem.protobuf.collector.v1.CollectorPayloadEntityProto.CollectorPayloadEntity;
import com.google.protobuf.TextFormat;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.net.URL;
import java.net.MalformedURLException;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.Instant;
import org.joda.time.DateTimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeasurementProtocolBuilder{
	
	private static final Logger LOG = LoggerFactory.getLogger(MeasurementProtocolBuilder.class);
	
	private Map<String, String> paramMap;
	private List<MPEntity> events = new ArrayList<>();
	private PageviewEntity pageviewEntity = new PageviewEntity();
	private EventEntity eventEntity = new EventEntity();
	private ExceptionEntity exceptionEntity = new ExceptionEntity();
	private ProductEntity productEntity = new ProductEntity();
	private TransactionEntity transactionEntity = new TransactionEntity();
	private SocialEntity socialEntity = new SocialEntity();
	private TimingEntity timingEntity = new TimingEntity();	
	private TrafficEntity trafficEntity = new TrafficEntity();
	private PromotionEntity promotionEntity = new PromotionEntity();
	private ProductImpressionEntity productImpressionEntity = new ProductImpressionEntity();
	private SiteSearchEntity siteSearchEntity = new SiteSearchEntity();
    private static String excludedBotsPattern = ".*(^$|bot|spider|crawler).*";
    //private static String includedHostnamesPattern = ".*";
    private static String includedHostnamesPattern = ".*(beta.datahem.org|www.datahem.org).*";
    private static String timeZone = "Etc/UTC";
    
    public MeasurementProtocolBuilder(){
	}
	
  	public String getSearchEnginesPattern(){
    	return this.trafficEntity.getSearchEnginesPattern();
  	}

	public void setSearchEnginesPattern(String pattern){
    	this.trafficEntity.setSearchEnginesPattern(pattern);
  	}
  	
  	public String getSocialNetworksPattern(){
    	return this.trafficEntity.getSocialNetworksPattern();
  	}

	public void setSocialNetworksPattern(String pattern){
    	this.trafficEntity.setSocialNetworksPattern(pattern);
  	}
  	
  	public String getIgnoredReferersPattern(){
    	return this.trafficEntity.getIgnoredReferersPattern();
  	}

	public void setIgnoredReferersPattern(String pattern){
    	this.trafficEntity.setIgnoredReferersPattern(pattern);
  	}
  	
  	public String getExcludedBotsPattern(){
    	return this.excludedBotsPattern;
  	}

	public void setExcludedBotsPattern(String pattern){
    	this.excludedBotsPattern = pattern;
  	}
  	
  	public String getIncludedHostnamesPattern(){
    	return this.includedHostnamesPattern;
  	}

	public void setIncludedHostnamesPattern(String pattern){
    	this.includedHostnamesPattern = pattern;
  	}
  	
  	public String getSiteSearchPattern(){
    	return this.siteSearchEntity.getSiteSearchPattern();
  	}

	public void setSiteSearchPattern(String pattern){
    	this.siteSearchEntity.setSiteSearchPattern(pattern);
  	}
  	
  	public String getTimeZone(){
    	return this.timeZone;
  	}

	public void setTimeZone(String tz){
    	this.timeZone = tz;
  	}

	public List<MPEntity> mpEntitiesFromCollectorPayload(CollectorPayloadEntity cp){
		try{
	        //Check if post body contains payload and add parameters in a map
	        if (!"".equals(cp.getPayload())) {
	            //Add header parameters to paramMap
	            paramMap = FieldMapper.fieldMapFromQuery(cp.getPayload());
	            paramMap.putAll(cp.getHeadersMap());
	            
	            //Exclude bots, spiders and crawlers
				if(paramMap.get("User-Agent") == null){
					paramMap.put("User-Agent", "");
					LOG.info("User-Agent = null");
				}
	
	        	if(!paramMap.get("User-Agent").matches(getExcludedBotsPattern()) && paramMap.get("dl").matches(getIncludedHostnamesPattern())){
	                //Add epochMillis and timestamp to paramMap       
		            Instant payloadTimeStamp = new Instant(Long.parseLong(cp.getEpochMillis()));
					DateTimeFormatter utc_timestamp = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZoneUTC();
		            paramMap.put("cpts", payloadTimeStamp.toString(utc_timestamp));
		            paramMap.put("cpem", cp.getEpochMillis());
	
					//Set local timezone for use as partition field
					DateTimeFormatter partition = DateTimeFormat.forPattern("YYYY-MM-dd").withZone(DateTimeZone.forID(getTimeZone()));
					paramMap.put("cpd", payloadTimeStamp.toString(partition));
					
					addAllIfNotNull(events, pageviewEntity.build(paramMap));			
					addAllIfNotNull(events, eventEntity.build(paramMap));
					addAllIfNotNull(events, exceptionEntity.build(paramMap));
					addAllIfNotNull(events, productEntity.build(paramMap));
					addAllIfNotNull(events, transactionEntity.build(paramMap));
					addAllIfNotNull(events, socialEntity.build(paramMap));
					addAllIfNotNull(events, timingEntity.build(paramMap));
					addAllIfNotNull(events, trafficEntity.build(paramMap));
					addAllIfNotNull(events, promotionEntity.build(paramMap));
					addAllIfNotNull(events, productImpressionEntity.build(paramMap));
					addAllIfNotNull(events, siteSearchEntity.build(paramMap));
				}
	        }
        }
        catch (Exception e) {
				LOG.error(e.toString());
		}
    	return events;   
    }
        
    public static void addAllIfNotNull(List<MPEntity> list, List<MPEntity> c) {
    	if (c != null) {
        	list.addAll(c);
    	}
	}
	
	public static boolean regexMatcherFind(String expression,String input) {
    	if(!"".equals(input)) return false;
    	Pattern p = Pattern.compile(expression);
        Matcher m = p.matcher(input);
        return m.find();
	}

//mvn compile exec:java -Dexec.mainClass="org.datahem.processor.measurementprotocol.utils.MeasurementProtocolBuilder"
public static void main(String[] args) {
		
	String click = "v=1&_v=j66&a=1140262547&t=event&ni=0&_s=1&dl=https%3A%2F%2Fwww.datahem.org%2Fvaror%2Fkott-o-chark&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=K%C3%B6tt%20%26%20Chark%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=992x1096&je=0&ec=Ecommerce&ea=Product%20Click&_u=aCDAAEAL~&jid=145378208&gjid=1242227089&cid=1062063169.1517835391&uid=947563&tid=UA-7391864-18&_gid=616449507.1520411256&_r=1&gtm=G2rP9BRHCJ&pa=click&pr1id=25258&pr1nm=Blodpudding&pr1pr=10.95&pr1br=GEAS&pr1ca=Blodpudding&pal=%2Fvaror%2Fkott-o-chark&z=28686755";
	String purchase = "v=1&tid=UA-XXXXX-Y&cid=555&t=pageview&dl=https%3A%2F%2Fwww.datahem.org&dp=/receipt&dt=Receipt%20Page&ti=T12345&ta=Google%20Store%20-%20Online&cd1=test1&cd2=test2&cm1=1&cm2=2&tr=37.39&tt=2.85&ts=5.34&tcc=SUMMER2013&pa=purchase&pr1id=P12345&pr1nm=Android%20Warhol%20T-Shirt&pr1ca=Apparel&pr1br=Google&pr1va=Black&pr1ps=1&pr1cd1=test1&pr1cd2=test2&pr1cm1=1&pr1cm2=2";
	String detail = "v=1&tid=UA-XXXXX-Y&cid=555&t=pageview&pa=detail&pr1id=P12345&pr1nm=Android%20Warhol%20T-Shirt&pr1ca=Apparel&pr1br=Google&pr1va=Black&pr1ps=1&pr2id=P54321&pr2nm=iOS%20Warhol%20T-Shirt&pr2ca=Apparel&pr2br=Apple&pr2va=White&pr2ps=2";
	String event = "v=1&tid=UA-XXXXX-Y&cid=555&t=event&ec=UX&ea=click&el=Results&ev=50&dl=https%3A%2F%2Fwww.tele2.se%2Fhandla%2Faktuella-kampanjer%3Futm_source%3DtestSource%26utm_medium%3DtestMedium%26utm_campaign%3DtestName%26utm_term%3DtestTerm%26utm_content%3DtestContent%26gclid%3D54321";
	String pageview = "v=1&_v=j66&a=1140262547&t=pageview&_s=1&dl=https%3A%2F%2Fwww.datahem.org%2Fvaror%2Fkott-o-chark&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=Frukt%20%26%20Gr%C3%B6nt%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&_u=aCDAAEAL~&jid=&gjid=&cid=1062063169.1517835391&uid=947563&tid=UA-7391864-18&_gid=616449507.1520411256&gtm=G2rP9BRHCJ&z=631938637&cd1=gold&cd2=family&cm1=25";
	String add = "v=1&_v=j66&a=1140262547&t=event&ni=0&cu=SEK&_s=1&dl=https%3A%2F%2Fwww.datahem.org%2Fvaror%2Fkott-o-chark&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=K%C3%B6tt%20%26%20Chark%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&ec=Ecommerce&ea=Add%20To%20Cart&_u=aCDAAEAL~&jid=&gjid=&cid=1062063169.1517835391&uid=947563&tid=UA-7391864-18&_gid=616449507.1520411256&gtm=G2rP9BRHCJ&pa=add&pr1id=22534&pr1nm=Kalkon%20R%C3%B6kt%20Skivad&pr1pr=39.95&pr1br=P%C3%A4rsons&pr1ca=Kalkon%20P%C3%A5l%C3%A4gg&pr1qt=1&z=2064466511";
	String remove = "v=1&_v=j66&a=1140262547&t=event&ni=0&cu=SEK&_s=1&dl=https%3A%2F%2Fwww.datahem.org%2Fvaror%2Fkott-o-chark&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=K%C3%B6tt%20%26%20Chark%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&ec=Ecommerce&ea=Add%20To%20Cart&_u=aCDAAEAL~&jid=&gjid=&cid=1062063169.1517835391&uid=947563&tid=UA-7391864-18&_gid=616449507.1520411256&gtm=G2rP9BRHCJ&pa=remove&pr1id=22534&pr1nm=Kalkon%20R%C3%B6kt%20Skivad&pr1pr=39.95&pr1br=P%C3%A4rsons&pr1ca=Kalkon%20P%C3%A5l%C3%A4gg&pr1qt=1&z=2064466511";
	String checkoutStepOption = "v=1&_v=j66&a=1140262547&t=event&ni=0&cu=SEK&_s=1&dl=https%3A%2F%2Fwww.datahem.org%2Fvaror%2Fkott-o-chark&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=K%C3%B6tt%20%26%20Chark%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&ec=Ecommerce&ea=Add%20To%20Cart&_u=aCDAAEAL~&jid=&gjid=&cid=1062063169.1517835391&uid=947563&tid=UA-7391864-18&_gid=616449507.1520411256&gtm=G2rP9BRHCJ&pa=checkout&pr1id=22534&pr1nm=Kalkon%20R%C3%B6kt%20Skivad&pr1pr=39.95&pr1br=P%C3%A4rsons&pr1ca=Kalkon%20P%C3%A5l%C3%A4gg&pr1qt=1&cos=1&col=visa&z=2064466511";
	String refundProduct = "v=1&_v=j66&a=1140262547&t=event&ni=1&cu=SEK&_s=1&dl=https%3A%2F%2Fwww.datahem.org%2Fvaror%2Fkott-o-chark&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=K%C3%B6tt%20%26%20Chark%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&ec=Ecommerce&ea=Refund&_u=aCDAAEAL~&jid=&gjid=&cid=1062063169.1517835391&uid=947563&tid=UA-7391864-18&_gid=616449507.1520411256&gtm=G2rP9BRHCJ&pa=refund&ti=12345&pr1id=22534&pr1nm=Kalkon%20R%C3%B6kt%20Skivad&pr1pr=39.95&pr1br=P%C3%A4rsons&pr1ca=Kalkon%20P%C3%A5l%C3%A4gg&pr1qt=1&z=2064466511";
	//refundTransaction
	String productImpression = "v=1&_v=j66&a=1140262547&t=pageview&_s=1&dl=https%3A%2F%2Fwww.datahem.org%2Fvaror%2Fkott-o-chark&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=Frukt%20%26%20Gr%C3%B6nt%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&_u=aCDAAEAL~&jid=&gjid=&cid=1062063169.1517835391&uid=947563&tid=UA-7391864-18&_gid=616449507.1520411256&gtm=G2rP9BRHCJ&z=631938637&cd1=gold&cd2=family&cm1=25&il1nm=Search%20Results&il1pi2id=P67890&il1pi2nm=Android%20T-Shirt&il1pi2br=Google&il1pi2ca=Apparel&il1pi2va=Black&il1pi2ps=2&il1pi2pr=29.20&il1pi2cd3=Member&il1pi2cm3=28";
	String promotion = "v=1&_v=j66&a=1140262547&t=pageview&_s=1&dl=https%3A%2F%2Fwww.datahem.org%2Fvaror%2Fkott-o-chark&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=Frukt%20%26%20Gr%C3%B6nt%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&_u=aCDAAEAL~&jid=&gjid=&cid=1062063169.1517835391&uid=947563&tid=UA-7391864-18&_gid=616449507.1520411256&gtm=G2rP9BRHCJ&z=631938637&cd1=gold&cd2=family&cm1=25&promoa=view&promo1id=SHIP&promo1nm=Free%20Shipping&promo1cr=Shipping%20Banner&promo2id=SHIPPED&promo2nm=Expensive%20Shipping&promo2cr=Shipping%20Banner";
	//social
	//timing
	//trafficPaidSearch
	//trafficDoubleClidck
	//trafficCampaign
	//exception
	String siteSearch = "v=1&_v=j66&a=1140262547&t=pageview&_s=1&dl=https%3A%2F%2Fbeta.datahem.org%2Fsok%3Fq%3Dpasta%20knyten%26page%3D1%26pageSize%3D25&dp=%2Fvaror%2Fkott-o-chark&ul=sv&de=UTF-8&dt=Frukt%20%26%20Gr%C3%B6nt%20%7C%20Mathem&sd=24-bit&sr=1920x1200&vp=1292x1096&je=0&_u=aCDAAEAL~&jid=&gjid=&cid=1062063169.1517835391&uid=947563&tid=UA-7391864-18&_gid=616449507.1520411256&gtm=G2rP9BRHCJ&z=631938637&cd1=gold&cd2=family&cm1=25";
	
	MeasurementProtocolBuilder mpb = new MeasurementProtocolBuilder();
	mpb.setExcludedBotsPattern(".*(^$|bot|spider|crawler).*");
	
	List<String> payloads = Stream.of(siteSearch).collect(Collectors.toList());
	payloads
		.stream()
		.forEach(payload -> test(mpb, payload));
}

private static void test(MeasurementProtocolBuilder mpb, String payload){
	//LOG.info("Inside test!");
	Map<String,String> headers = new HashMap<String, String>();
	headers.put("X-AppEngine-Country","SE");
	headers.put("X-AppEngine-City","stockholm");
	headers.put("User-Agent","Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36"); //Normal user
	//headers.put("User-Agent","Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"); //bot
	//headers.put("dr","https://www.tele2.se/mobiltelefoner/samsung/samsung-galaxy-s7"); //referal
	//headers.put("dr","https://www.google.se/"); //organic search
	//headers.put("dr","http://m.facebook.com/fdafda?foo=bar&q=hej"); //social
	//headers.put("dr","https://www.datahem.org/varor/kott-o-chark"); //ignored referer
	
	CollectorPayloadEntity cp = CollectorPayloadEntity.newBuilder()
			//.setSchemaName("measurementprotocol")
			.setPayload(payload)
			.putAllHeaders(headers)
			.setEpochMillis("1519977053236")
			.setUuid("5bd43e1a-8217-4020-826f-3c7f0b763c32")
			.build();
	List<MPEntity> mpEntities = mpb.mpEntitiesFromCollectorPayload(cp);
	mpEntities.forEach(mpEntity -> {LOG.info(TextFormat.printToString(mpEntity));});
}

}
