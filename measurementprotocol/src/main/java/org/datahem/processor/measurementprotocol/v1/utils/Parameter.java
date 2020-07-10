package org.datahem.processor.measurementprotocol.v1.utils;

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


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parameter {

    private final String parameter;
    private final String valueType;
    private final String defaultValue;
    private final int maxLength;
    private final String parameterName;
    private final boolean required;
    private final String parameterNameSuffix;
    private final String exampleParameter;
    private final Object exampleValue;
    private final String exampleParameterName;

    public Parameter(String parameter, String valueType, String defaultValue, int maxLength, String parameterName, boolean required) {
        this(parameter, valueType, defaultValue, maxLength, parameterName, required, null);
    }

    public Parameter(String parameter, String valueType, String defaultValue, int maxLength, String parameterName, boolean required, Object exampleValue) {
        this(parameter, valueType, defaultValue, maxLength, parameterName, required, null, parameter, exampleValue, parameterName);
    }

    public Parameter(String parameter, String valueType, String defaultValue, int maxLength, String parameterName, boolean required, String exampleParameter, Object exampleValue) {
        this(parameter, valueType, defaultValue, maxLength, parameterName, required, null, exampleParameter, exampleValue, parameterName);
    }

    public Parameter(String parameter, String valueType, String defaultValue, int maxLength, String parameterName, boolean required, String parameterNameSuffix, String exampleParameter, Object exampleValue, String exampleParameterName) {
        this.parameter = parameter;
        this.valueType = valueType;
        this.defaultValue = defaultValue;
        this.maxLength = maxLength;
        this.parameterName = parameterName;
        this.required = required;
        this.parameterNameSuffix = parameterNameSuffix;
        this.exampleParameter = exampleParameter;
        this.exampleValue = exampleValue;
        this.exampleParameterName = exampleParameterName;
    }

    public String getParameter() {
        return parameter;
    }

    public String getValueType() {
        return valueType;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public String getParameterName() {
        return parameterName;
    }

    public boolean getRequired() {
        return required;
    }

    public String getParameterNameSuffix() {
        return parameterNameSuffix;
    }

    public String getParameterNameWithSuffix(String param) {
        if (null == parameterNameSuffix) {
            return parameterName;
        } else {
            Pattern pattern = Pattern.compile(parameterNameSuffix);
            Matcher matcher = pattern.matcher(param);
            if (matcher.find() && matcher.group(1) != null) {
                return parameterName + matcher.group(1);
            } else {
                return parameterName;
            }
        }
    }

    public String getExampleParameter() {
        return exampleParameter;
    }

    public Object getExampleValue() {
        return exampleValue;
    }

    public String getExampleParameterName() {
        return exampleParameterName;
    }
}
