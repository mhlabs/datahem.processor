package org.datahem.processor.utils;

/*-
 * #%L
 * DataHem
 * %%
 * Copyright (C) 2018 - 2019 MatHem Sverige AB
 * %%
 * DataHem is a serverless real-time data platform built for reporting, analytics and data/ML-products.
 * Copyright (C) 2019 Robert Sahlin
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * #L%
 */

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

public class JsonToTableRowFn extends DoFn<String, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonToTableRowFn.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        try {
            String json = (String) c.element();
            Gson gson = new Gson();
            TableRow outputRow = gson.fromJson(json, TableRow.class);
            LOG.info("tablerow: " + outputRow.toString());
            c.output(outputRow);
        } catch (Exception e) {
            //LOG.error(ExceptionUtils.getStackTrace(e));
            LOG.error(e.toString());
        }
        return;
    }
}
