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

import static com.hotels.bdp.circustrain.bigquery.extraction.BigQueryDataExtractionKey.makeKey;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.Table;
import com.google.common.annotations.VisibleForTesting;

public class BigQueryDataExtractionManager {

  @VisibleForTesting
  static class TableWrapper {

    private final Table table;

    TableWrapper(Table table) {
      this.table = table;
    }

    private Table getTable() {
      return this.table;
    }

    @Override
    public String toString() {
      return table.getTableId().getDataset() + "." + table.getTableId().getTable();
    }

    @Override
    public int hashCode() {
      return (makeKey(table.getTableId().getDataset(), table.getTableId().getTable())).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TableWrapper other = (TableWrapper) obj;
      Table otherTable = other.getTable();
      String thisDatasetName = this.table.getTableId().getDataset();
      String otherDatasetName = otherTable.getTableId().getDataset();
      if (!thisDatasetName.equals(otherDatasetName)) {
        return false;
      }

      String thisTableName = this.table.getTableId().getTable();
      String otherTableName = otherTable.getTableId().getTable();
      if (!thisTableName.equals(otherTableName)) {
        return false;
      }

      return true;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(BigQueryDataExtractionManager.class);

  private final BigQueryDataExtractionService service;

  private Map<TableWrapper, BigQueryExtractionData> cache = new HashMap<>();

  public BigQueryDataExtractionManager(BigQueryDataExtractionService service) {
    this.service = service;
  }

  public void register(Table... tables) {
    for (Table table : tables) {
      cacheRead(new TableWrapper(table));
    }
  }

  public void extract() {
    for (TableWrapper wrapper : cache.keySet()) {
      extract(wrapper);
    }
  }

  public void cleanup() {
    for (TableWrapper table : cache.keySet()) {
      cleanup(table);
    }
  }

  private BigQueryExtractionData cacheRead(TableWrapper wrapper) {
    BigQueryExtractionData data = cache.get(wrapper);
    if (data == null) {
      log.info("Adding table: {} to cache", wrapper);
      data = new BigQueryExtractionData(wrapper.getTable());
      cache.put(wrapper, data);
    }
    return data;
  }

  public boolean extract(Table table) {
    return extract(new TableWrapper(table));
  }

  private boolean extract(TableWrapper wrapper) {
    log.info("Extracting table: {}", wrapper);
    BigQueryExtractionData data = cacheRead(wrapper);
    return service.extract(data);
  }

  public boolean cleanup(Table table) {
    return cleanup(new TableWrapper(table));
  }

  private boolean cleanup(TableWrapper wrapper) {
    log.info("Cleaning data from table: {}", wrapper);
    BigQueryExtractionData data = cache.get(wrapper);
    if (data == null) {
      log.warn("Attempting to cleanup table data {} which has not been extracted", wrapper);
      return false;
    }
    return service.cleanup(data);
  }

  public String location(Table table) {
    return location(new TableWrapper(table));
  }

  private String location(TableWrapper wrapper) {
    BigQueryExtractionData data = cache.get(wrapper);
    if (data == null) {
      return null;
    }
    return "gs://" + data.getDataBucket() + "/";
  }
}
