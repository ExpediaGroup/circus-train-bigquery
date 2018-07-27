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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionCache {

  private static final Logger log = LoggerFactory.getLogger(PartitionCache.class);

  // TODO: Test whether this can just be list of Partitions when running with multiple replications
  private final Map<String, List<Partition>> partitionCache = new HashMap<>();

  void add(Partition partition) {
    String partitionKey = makeKey(partition.getDbName(), partition.getTableName());
    log.debug("Adding partition with key {} to cache", partitionKey);

    if (partitionCache.containsKey(partitionKey)) {
      log.debug("Cache contains key {}. appending to list of Partitions", partitionKey);
      partitionCache.get(partitionKey).add(partition);
    } else {
      log.debug("Cache does not contain key {}. creating new list of Partitions", partitionKey);

      List<Partition> partitions = new ArrayList<>();
      partitions.add(partition);
      partitionCache.put(partitionKey, partitions);
    }
  }

  boolean contains(String key) {
    return partitionCache.containsKey(key);
  }

  List<Partition> get(String key) {
    log.info("Getting partitions for key {}", key);
    return partitionCache.get(key);
  }

  void clear() {
    partitionCache.clear();
  }
}
