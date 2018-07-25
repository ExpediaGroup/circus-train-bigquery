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

import static com.hotels.bdp.circustrain.bigquery.util.BigQueryKey.makeKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Partition;

class PartitionCache {

  private final Map<String, List<Partition>> partitionCache = new HashMap<>();

  void add(Partition partition) {
    String partitionKey = makeKey(partition.getDbName(), partition.getTableName());

    if (partitionCache.containsKey(partitionKey)) {
      partitionCache.get(partitionKey).add(partition);
    } else {
      List<Partition> partitions = new ArrayList<>();
      partitions.add(partition);
      partitionCache.put(partitionKey, partitions);
    }
  }

  boolean contains(String key) {
    return partitionCache.containsKey(key);
  }

  List<Partition> get(String key) {
    return partitionCache.get(key);
  }

  void clear() {
    partitionCache.clear();
  }
}
