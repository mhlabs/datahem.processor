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

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.ValueProvider;

public interface MeasurementProtocolOptions extends PipelineOptions {

    @Description("Search Engine domain regex pattern, java syntax, keyword as submatch i.e. .*360\\.cn.*q=(([^&#]*)|&|#|$)")
    @Default.String(".*www\\.google\\..*|.*www\\.bing\\..*|.*search\\.yahoo\\..*")
    ValueProvider<String> getSearchEnginesPattern();
    void setSearchEnginesPattern(ValueProvider<String> value);
    
    @Description("Ignored referer domain regex pattern, java syntax, i.e. .*mathem\\.se.*")
    //@Default.String(".*")
    ValueProvider<String> getIgnoredReferersPattern();
    void setIgnoredReferersPattern(ValueProvider<String> value);
    
    @Description("Social Networks domain regex pattern, java syntax, i.e. .*facebook\\..*|.*instagram\\..*|.*pinterest\\..*|.*youtube\\..*|.*linkedin\\..*|.*twitter\\..*")
    @Default.String(".*facebook\\..*|.*instagram\\..*|.*pinterest\\..*|.*youtube\\..*|.*linkedin\\..*|.*twitter\\..*")
    ValueProvider<String> getSocialNetworksPattern();
    void setSocialNetworksPattern(ValueProvider<String> value);
    
    @Description("Included hostnames regex pattern, java syntax, i.e. .*mathem\\.se.*")
    @Default.String(".*")
    ValueProvider<String> getIncludedHostnamesPattern();
    void setIncludedHostnamesPattern(ValueProvider<String> value);
    
    @Description("Exclude bot traffic with user agent matching regex pattern, java syntax, i.e. .*bot.*|.*spider.*|.*crawler.*")
    @Default.String(".*bot.*|.*spider.*|.*crawler.*")
    ValueProvider<String> getExcludedBotsPattern();
    void setExcludedBotsPattern(ValueProvider<String> value);

	@Description("Extract search term from site search with URL regex pattern, java syntax, i.e. .*q=(([^&#]*)|&|#|$)")
    @Default.String(".*q=(([^&#]*)|&|#|$)")
    ValueProvider<String> getSiteSearchPattern();
    void setSiteSearchPattern(ValueProvider<String> value);
    
    @Description("Time Zone to use for the date field")
    @Default.String("Etc/UTC")
    ValueProvider<String> getTimeZone();
    void setTimeZone(ValueProvider<String> value);
	
  }
