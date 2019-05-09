/**
 * Copyright (C) 2018-2019 Expedia, Inc.
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
package com.hotels.bdp.circustrain.bigquery.client;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.bigquery.util.TableNameFactory;

public class HiveTableCache {

  private static final Logger log = LoggerFactory.getLogger(HiveTableCache.class);

  private final Map<String, Table> tableCache = new HashMap<>();

  public boolean contains(String databaseName, String tableName) {
    String key = TableNameFactory.newInstance(databaseName, tableName);
    return tableCache.containsKey(key);
  }

  public Table get(String databaseName, String tableName) {
    String key = TableNameFactory.newInstance(databaseName, tableName);
    log.debug("Getting table {} from cache", key);
    return tableCache.get(key);
  }

  public void put(Table table) {
    String key = TableNameFactory.newInstance(table);
    log.debug("Adding table {} to cache", key);
    tableCache.put(key, table);
  }
}
