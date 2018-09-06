package org.datahem.processor.measurementprotocol.utils;

import org.datahem.protobuf.collector.v1.CollectorPayloadEntityProto.*;
import org.datahem.protobuf.collector.v1.CollectorPayloadEntityProto;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.options.ValueProvider;
import java.util.List;

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

public class PayloadToMPEntityFn extends DoFn<CollectorPayloadEntity, MPEntity> {
		ValueProvider<String> searchEnginesPattern;
		ValueProvider<String> ignoredReferersPattern;
		ValueProvider<String> socialNetworksPattern;
		ValueProvider<String> includedHostnamesPattern;
		ValueProvider<String> excludedBotsPattern;
		ValueProvider<String> siteSearchPattern;
		ValueProvider<String> timeZone;
		
	  	public PayloadToMPEntityFn(
	  		ValueProvider<String> searchEnginesPattern, 
	  		ValueProvider<String> ignoredReferersPattern, 
	  		ValueProvider<String> socialNetworksPattern, 
	  		ValueProvider<String> includedHostnamesPattern, 
	  		ValueProvider<String> excludedBotsPattern, 
	  		ValueProvider<String> siteSearchPattern,
	  		ValueProvider<String> timeZone) {
		     	this.searchEnginesPattern = searchEnginesPattern;
		     	this.ignoredReferersPattern = ignoredReferersPattern;
		     	this.socialNetworksPattern = socialNetworksPattern;
		     	this.includedHostnamesPattern = includedHostnamesPattern;
		     	this.excludedBotsPattern = excludedBotsPattern;
		     	this.siteSearchPattern = siteSearchPattern;
		     	this.timeZone = timeZone;
	   	}

      	@ProcessElement      
      	public void processElement(ProcessContext c)  {
	      	
	      	CollectorPayloadEntity cp = c.element();
	        MeasurementProtocolBuilder mpb = new MeasurementProtocolBuilder();
	/*        mpb.setSearchEnginesPattern(searchEnginesPattern.get());
	        mpb.setIgnoredReferersPattern(ignoredReferersPattern.get());
	        mpb.setSocialNetworksPattern(socialNetworksPattern.get());*/
	        mpb.setIncludedHostnamesPattern(includedHostnamesPattern.get());
	        mpb.setExcludedBotsPattern(excludedBotsPattern.get());
	//        mpb.setSiteSearchPattern(siteSearchPattern.get());
	        mpb.setTimeZone(timeZone.get());
	        
	        List<MPEntity> mpEntities = mpb.mpEntitiesFromCollectorPayload(cp);
	        mpEntities.forEach(mpEntity -> {
	        	//LOG.info("events timestamp: " + ev.getTimestamp() + ", EpochMillis: " + ev.getEpochMillis());
	        	c.output(mpEntity);
        	});
	    	return;
	    	
		}
      
  }
