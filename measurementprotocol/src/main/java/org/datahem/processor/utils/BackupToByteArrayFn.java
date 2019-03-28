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

import org.apache.beam.sdk.transforms.DoFn;
import com.google.api.services.bigquery.model.TableRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import java.io.Serializable;
import java.io.IOException;
import java.util.Base64;

//import org.apache.beam.sdk.options.ValueProvider;
//import java.util.List;

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

public class BackupToByteArrayFn extends DoFn<TableRow, byte[]> {
	
	private static final Logger LOG = LoggerFactory.getLogger(BackupToByteArrayFn.class);

      	@ProcessElement      
      	public void processElement(ProcessContext c)  {
	      	TableRow row = c.element();
			String b = (String) row.get("data");
	      	byte[] decoded = Base64.getDecoder().decode(b.getBytes());
	      	c.output(decoded);
	     	return;
	    }
  }
