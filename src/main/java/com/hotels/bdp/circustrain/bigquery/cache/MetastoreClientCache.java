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
package com.hotels.bdp.circustrain.bigquery.cache;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public class MetastoreClientCache {

  private final TableCache tableCache = new TableCache();
  private final PartitionCache partitionCache = new PartitionCache();

  public void cachePartition(Partition partition) {
    partitionCache.add(partition);
  }

  public boolean containsPartitions(String key) {
    return partitionCache.contains(key);
  }

  public List<Partition> getPartitions(String key) {
    return partitionCache.get(key);
  }

  public boolean containsTable(String key) {
    return tableCache.contains(key);
  }

  public Table getTable(String key) {
    return tableCache.get(key);
  }

  public void cacheTable(Table table) {
    tableCache.add(table);
  }

  public void clear() {
    partitionCache.clear();
    tableCache.clear();
  }

}
