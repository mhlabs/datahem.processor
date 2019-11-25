package org.datahem.processor.utils;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 - 2019 MatHem Sverige AB
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.options.ValueProvider;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TablePartition implements SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination> {

    private static final Logger LOG = LoggerFactory.getLogger(TablePartition.class);
    private final String tableSpec;
    private final String tableDescription;
    
    public TablePartition(ValueProvider<String> tableSpec, String tableDescription) {
        this.tableSpec = tableSpec.get(); 
        this.tableDescription = tableDescription;
    }

    @Override
    public TableDestination apply(ValueInSingleWindow<TableRow> input) {
        DateTimeFormatter partition = DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC();
        TableReference reference = BigQueryHelpers.parseTableSpec(tableSpec + "$" + input.getWindow().maxTimestamp().toString(partition));
        return new TableDestination(reference, tableDescription, new TimePartitioning());
    }
}
