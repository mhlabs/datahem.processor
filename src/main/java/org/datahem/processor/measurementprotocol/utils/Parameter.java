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

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Parameter {	
	
	private final String parameter;
	private final String valueType;
	private final String defaultValue;
	private final int maxLength;
	private final String parameterName;
	private final boolean required;
	private final String parameterNameSuffix;
	
	public Parameter(String parameter, String valueType, String defaultValue, int maxLength, String parameterName, boolean required){
		/*this.parameter = parameter;
		this.valueType = valueType;
		this.defaultValue = defaultValue;
		this.maxLength = maxLength;
		this.parameterName = parameterName;
		this.required = required;
		this.parameterNameSuffix = null;*/
		this(parameter, valueType, defaultValue, maxLength, parameterName, required, null);
	}

	public Parameter(String parameter, String valueType, String defaultValue, int maxLength, String parameterName, boolean required, String parameterNameSuffix){
		this.parameter = parameter;
		this.valueType = valueType;
		this.defaultValue = defaultValue;
		this.maxLength = maxLength;
		this.parameterName = parameterName;
		this.required = required;
		this.parameterNameSuffix = parameterNameSuffix;
	}
	
	public String getParameter(){return parameter;}
	public String getValueType(){return valueType;}
	public String getDefaultValue(){return defaultValue;}
	public int getMaxLength(){return maxLength;}
	public String getParameterName(){return parameterName;}
	public boolean getRequired(){return required;}
	public String getParameterNameSuffix(){return parameterNameSuffix;}
	
	public String getParameterNameWithSuffix(String param){
		if(null == parameterNameSuffix){
			return parameterName;
		}
		else{
			Pattern pattern = Pattern.compile(parameterNameSuffix);
			Matcher matcher = pattern.matcher(param);
			if(matcher.find() && matcher.group(1) != null){
				return parameterName + matcher.group(1);
			}
			else {
				return parameterName;
			}
		}
	}
}
