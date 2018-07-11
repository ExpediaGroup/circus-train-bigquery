/**
 * Copyright (C) 2018 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.bdp.circustrain.bigquery.extraction;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.Table;
import com.google.common.annotations.VisibleForTesting;

public class BigQueryDataExtractionManager {

  private static final Logger log = LoggerFactory.getLogger(BigQueryDataExtractionManager.class);

  private final BigQueryDataExtractionService service;
  private final Map<Table, BigQueryExtractionData> locationMap;

  public BigQueryDataExtractionManager(BigQueryDataExtractionService service) {
    this.service = service;
    this.locationMap = new HashMap<>();
  }

  @VisibleForTesting
  BigQueryDataExtractionManager(
      BigQueryDataExtractionService service,
      Map<Table, BigQueryExtractionData> locationMap) {
    this.service = service;
    this.locationMap = locationMap;
  }

  public void register(Table table) {
    locationMap.put(table, new BigQueryExtractionData());
  }

  public void extractAll() {
    for (Map.Entry<Table, BigQueryExtractionData> entry : locationMap.entrySet()) {
      Table table = entry.getKey();
      BigQueryExtractionData extractionData = entry.getValue();
      log.info("Extracting table: {}.{}", table.getTableId().getDataset(), table.getTableId().getTable());
      service.extract(table, extractionData);
    }
  }

  public void cleanupAll() {
    for (Map.Entry<Table, BigQueryExtractionData> entry : locationMap.entrySet()) {
      Table table = entry.getKey();
      BigQueryExtractionData extractionData = entry.getValue();
      log.info("Cleaning data from table:  {}.{}", table.getTableId().getDataset(), table.getTableId().getTable());
      service.cleanup(extractionData);
    }
  }

  public String getDataLocation(Table table) {
    if (!locationMap.containsKey(table)) {
      return null;
    }
    return "gs://" + locationMap.get(table).getDataBucket() + "/";
  }
}
