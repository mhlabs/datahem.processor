package org.datahem.processor.measurementprotocol.v2.utils;



import org.apache.beam.sdk.transforms.DoFn;
import com.google.api.services.bigquery.model.TableRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto.*;
//import org.datahem.protobuf.measurementprotocol.v1.MPEntityProto;
import org.datahem.protobuf.measurementprotocol.v2.*;

import org.datahem.processor.utils.ProtobufUtils;

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

public class MeasurementProtocolToTableRowFn extends DoFn<MeasurementProtocol,TableRow> {
	
	private static final Logger LOG = LoggerFactory.getLogger(MeasurementProtocolToTableRowFn.class);

      	@ProcessElement      
      	public void processElement(ProcessContext c)  {
      		MeasurementProtocol measurementProtocol = c.element();
	        TableRow tRow = ProtobufUtils.makeTableRow(measurementProtocol);
	        c.output(tRow);
	     	return;
	    }
}
