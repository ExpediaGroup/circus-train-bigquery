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

public class BigQueryDataExtractionManager {

  private static final Logger log = LoggerFactory.getLogger(BigQueryDataExtractionManager.class);

  private final BigQueryDataExtractionService service;
  private final Map<Table, BigQueryExtractionData> cache = new HashMap<>();

  public BigQueryDataExtractionManager(BigQueryDataExtractionService service) {
    this.service = service;
  }

  public void register(Table... tables) {
    for (Table table : tables) {
      cacheRead(table);
    }
  }

  public void extract() {
    for (Table table : cache.keySet()) {
      extract(table);
    }
  }

  public void cleanup() {
    for (Table table : cache.keySet()) {
      cleanup(table);
    }
  }

  private BigQueryExtractionData cacheRead(Table table) {
    BigQueryExtractionData data = cache.get(table);
    if (data == null) {
      log.info("Adding table: {}.{} to cache", table.getTableId().getDataset(), table.getTableId().getTable());
      data = new BigQueryExtractionData(table);
      cache.put(table, data);
    }
    return data;
  }

  public void extract(Table table) {
    log.info("Extracting table: {}.{}", table.getTableId().getDataset(), table.getTableId().getTable());
    BigQueryExtractionData data = cacheRead(table);
    service.extract(data);
  }

  public void cleanup(Table table) {
    log.info("Cleaning data from table:  {}.{}", table.getTableId().getDataset(), table.getTableId().getTable());
    BigQueryExtractionData data = cache.get(table);
    if (data == null) {
      log.warn("Attempting to cleanup table data for  {}.{} which has not been extracted",
          table.getTableId().getDataset(), table.getTableId().getTable());
      return;
    }
    service.cleanup(data);
  }

  public String location(Table table) {
    BigQueryExtractionData data = cacheRead(table);
    return "gs://" + data.getDataBucket() + "/";
  }
}
