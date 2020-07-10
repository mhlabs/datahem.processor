package org.datahem.processor.measurementprotocol.v1.utils;


import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.datahem.processor.utils.ProtobufUtils;
import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.MPEntity;
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

public class MPEntityToTableRowFn extends DoFn<MPEntity, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(MPEntityToTableRowFn.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        MPEntity mpEntity = c.element();
        TableRow tRow = ProtobufUtils.makeTableRow(mpEntity);
        c.output(tRow);
        return;
    }
}
