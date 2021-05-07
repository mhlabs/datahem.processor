package org.datahem.processor.dynamodb;

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

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;

/*
_CONFIG='{
      "fileDescriptorBucket":"mathem-ml-datahem-test-descriptor",
      "fileDescriptorName":"schemas.desc",
      "descriptorFullName":"mathem.distribution.tms_truck_temperature.truck_temperature.v1.TruckTemperature",
      "taxonomyResourcePattern":".*590903188537942776.*",
      "pubsubSubscription":"",
      "bigQueryTableSpec":""
}'
*/

public class Config {
    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    static public class JobConfig {
        public String fileDescriptorBucket;
        public String fileDescriptorName;
        public String descriptorFullName;
        public String taxonomyResourcePattern;
        public String pubsubSubscription;
        public String bigQueryTableSpec;
    }

    public static JobConfig read(String config) {
        LOG.info("config:" + config);
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new StringReader(config));
        reader.setLenient(true);
        try {
            JobConfig JobConfig = gson.fromJson(reader, JobConfig.class);
            return JobConfig;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
