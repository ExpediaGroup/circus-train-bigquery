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


import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.Table;
import com.google.common.annotations.VisibleForTesting;

public class BigQueryDataExtractionManager {

  private static final Logger log = LoggerFactory.getLogger(BigQueryDataExtractionManager.class);

  private final BigQueryDataExtractionService service;
  private final Map<Table, Pair<BigQueryExtractionData, Boolean>> locationMap;
  private final Set<BigQueryExtractionData> extracted = new HashSet<>();

  public BigQueryDataExtractionManager(BigQueryDataExtractionService service) {
    this.service = service;
    this.locationMap = new LinkedHashMap<>();
  }

  @VisibleForTesting
  BigQueryDataExtractionManager(
      BigQueryDataExtractionService service,
      Map<Table, Pair<BigQueryExtractionData, Boolean>> locationMap) {
    this.service = service;
    this.locationMap = locationMap;
  }

  public void register(Table table) {
    this.register(table, new BigQueryExtractionData(), false);
  }

  public void register(Table table, boolean deleteTable) {
    this.register(table, new BigQueryExtractionData(), deleteTable);
  }

  public void register(Table table, BigQueryExtractionData extractionData) {
    Pair<BigQueryExtractionData, Boolean> deleteInfo = Pair.of(extractionData, false);
    locationMap.put(table, deleteInfo);
  }

  public void register(Table table, BigQueryExtractionData extractionData, boolean deleteTable) {
    Pair<BigQueryExtractionData, Boolean> deleteInfo = Pair.of(extractionData, deleteTable);
    locationMap.put(table, deleteInfo);
  }

  public void extractAll() {
    for (Map.Entry<Table, Pair<BigQueryExtractionData, Boolean>> entry : locationMap.entrySet()) {
      Table table = entry.getKey();
      BigQueryExtractionData extractionData = entry.getValue().getKey();
      if (!extracted.contains(extractionData)) {
        log.info("Extracting table: {}.{}", table.getTableId().getDataset(), table.getTableId().getTable());
        service.extract(table, extractionData);
        extracted.add(extractionData);
      }
    }
  }

  public void cleanupAll() {
    for (Map.Entry<Table, Pair<BigQueryExtractionData, Boolean>> entry : locationMap.entrySet()) {
      Table table = entry.getKey();
      Pair<BigQueryExtractionData, Boolean> deleteInfo = entry.getValue();
      BigQueryExtractionData extractionData = entry.getValue().getKey();
      Boolean deleteTable = deleteInfo.getValue();
      log.info("Cleaning data from table:  {}.{}", table.getTableId().getDataset(), table.getTableId().getTable());
      service.cleanup(extractionData);
      if (deleteTable) {
        log.info("Deleting table {}.{}", table.getTableId().getDataset(), table.getTableId().getTable());
        table.delete();
      }
    }
  }

  public String getDataLocation(Table table) {
    if (!locationMap.containsKey(table)) {
      return null;
    }
    return "gs://" + locationMap.get(table).getKey().getDataBucket() + "/";
  }
}
