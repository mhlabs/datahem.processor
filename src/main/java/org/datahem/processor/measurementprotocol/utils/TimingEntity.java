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

import org.datahem.processor.measurementprotocol.utils.BaseEntity;
import org.datahem.processor.measurementprotocol.utils.Parameter;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimingEntity extends BaseEntity{
	private Map<String, Parameter> parameters;
	private static final Logger LOG = LoggerFactory.getLogger(TimingEntity.class);

	public TimingEntity(){
		super();
		parameters = new HashMap<String, Parameter>();
		parameters.put("USER_TIMING_CATEGORY", new Parameter("utc", "String", null, 150, "userTimingCategory", true));
		parameters.put("USER_TIMING_VARIABLE_NAME", new Parameter("utv", "String", null, 500, "userTimingVariableName", true));
		parameters.put("USER_TIMING_TIME", new Parameter("utt", "Integer", null, 500, "userTimingTime", true));
		parameters.put("USER_TIMING_LABEL", new Parameter("utl", "String", null, 500, "userTimingLabel", false));
		parameters.put("PAGE_LOAD_TIME", new Parameter("plt", "Integer", null, 500, "pageLoadTime", false));
		parameters.put("DNS_TIME", new Parameter("dns", "Integer", null, 500, "dnsTime", false));
		parameters.put("PAGE_DOWNLOAD_TIME", new Parameter("pdt", "Integer", null, 500, "pageDownloadTime", false));
		parameters.put("REDIRECT_RESPONSE_TIME", new Parameter("rrt", "Integer", null, 500, "redirectResponseTime", false));
		parameters.put("TCP_CONNECT_TIME", new Parameter("tcp", "Integer", null, 500, "tcpConnectTime", false));
		parameters.put("SERVER_RESPONSE_TIME", new Parameter("srt", "Integer", null, 500, "serverResponseTime", false));
		parameters.put("DOM_INTERACTIVE_TIME", new Parameter("dit", "Integer", null, 500, "domInteractiveTime", false));
		parameters.put("CONTENT_LOAD_TIME", new Parameter("clt", "Integer", null, 500, "contentLoadTime", false));
	}
	
	private boolean trigger(Map<String, String> paramMap){
		return (null != paramMap.get("utc") && null != paramMap.get("utv") && null != paramMap.get("utt"));
	}
	
	public List<MPEntity> build(Map<String, String> paramMap){
		List<MPEntity> eventList = new ArrayList<>();
		if(trigger(paramMap)){
    		try{
				paramMap.put("ht", "timing");
				eventList.add(builder(paramMap).build());
				return eventList;
			}
			catch(IllegalArgumentException e){
				LOG.error(e.toString());
				return null;
			}
		}
		else{
			return null;
		}
	}
	
	public MPEntity.Builder builder(Map<String, String> paramMap) throws IllegalArgumentException{
		return builder(paramMap, super.builder(paramMap));
	}
	
	public MPEntity.Builder builder(Map<String, String> paramMap, MPEntity.Builder eventBuilder) throws IllegalArgumentException{
		return super.builder(paramMap, eventBuilder, this.parameters);
	}	
}
