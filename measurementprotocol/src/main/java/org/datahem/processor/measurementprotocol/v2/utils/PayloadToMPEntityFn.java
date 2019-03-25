package org.datahem.processor.measurementprotocol.v2.utils;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.options.ValueProvider;

import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto;

import java.util.List;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 Robert Sahlin and MatHem Sverige AB
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */

public class PayloadToMPEntityFn extends DoFn<PubsubMessage, MPEntity> {
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
	      	
	      	PubsubMessage received = c.element();
	        MeasurementProtocolBuilder mpb = new MeasurementProtocolBuilder();
	        mpb.setSearchEnginesPattern(searchEnginesPattern.get());
	        mpb.setIgnoredReferersPattern(ignoredReferersPattern.get());
	        mpb.setSocialNetworksPattern(socialNetworksPattern.get());
	        mpb.setIncludedHostnamesPattern(includedHostnamesPattern.get());
	        mpb.setExcludedBotsPattern(excludedBotsPattern.get());
	        mpb.setSiteSearchPattern(siteSearchPattern.get());
	        mpb.setTimeZone(timeZone.get());
	        
	        List<MPEntity> mpEntities = mpb.mpEntitiesFromPayload(received);
	        mpEntities.forEach(mpEntity -> {
	        	c.output(mpEntity);
        	});	
	    	return;	
		}
  }
