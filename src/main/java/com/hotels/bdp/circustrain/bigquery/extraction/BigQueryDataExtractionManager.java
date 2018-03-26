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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.cloud.bigquery.Table;

import com.google.common.collect.ImmutableList;
import com.hotels.bdp.circustrain.api.CircusTrainException;

public class BigQueryDataExtractionManager {

  private final List<Table> registered = new ArrayList<>();
  private final Map<String, BigQueryExtractionData> cache = new HashMap<>();
  private final BigQueryDataExtractionService service;

  public BigQueryDataExtractionManager(BigQueryDataExtractionService service) {
    this.service = service;
  }

  private String getKey(Table table) {
    String databaseName = table.getTableId().getDataset();
    String tableName = table.getTableId().getTable();
    String key = makeKey(databaseName, tableName);
    return key;
  }

  public void register(Table... table) {
    registered.addAll(Arrays.asList(table));
  }

  public void extract() {
    for (Table table : registered) {
      extract(table);
    }
  }

  public void cleanup() {
    for (Table table : registered) {
      cleanup(table);
    }
    registered.clear();
  }

  public ImmutableList<Table> getRegistered() {
    return ImmutableList.copyOf(registered);
  }

  public boolean extract(Table table) {
    String key = getKey(table);
    if (cache.get(key) != null) {
      return false;
    }
    BigQueryExtractionData data = new BigQueryExtractionData(table);
    cache.put(key, data);
    service.extract(data);
    return true;
  }

  public void cleanup(Table table) {
    String key = getKey(table);
    BigQueryExtractionData data = cache.get(key);
    if (data == null) {
      throw new CircusTrainException("Attempting to cleanup " + table + " this table was not extracted");
    }
    service.cleanup(data);
  }

  public String location(Table table) {
    String key = getKey(table);
    BigQueryExtractionData data = cache.get(key);
    if (data == null) {
      return null;
    }
    return "gs://" + data.getDataBucket() + "/";
  }
}
