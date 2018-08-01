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
package com.hotels.bdp.circustrain.bigquery.conf;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import static com.hotels.bdp.circustrain.bigquery.CircusTrainBigQueryConstants.PARTITION_BY;
import static com.hotels.bdp.circustrain.bigquery.CircusTrainBigQueryConstants.PARTITION_FILTER;
import static com.hotels.bdp.circustrain.bigquery.util.CircusTrainBigQueryKey.makeKey;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.conf.TableReplications;

@Component
public class PartitioningConfiguration {

  private static final Logger log = LoggerFactory.getLogger(PartitioningConfiguration.class);

  private final HashMap<String, Map<String, Object>> replicationConfigMap = new HashMap<>();

  @Autowired
  PartitioningConfiguration(TableReplications tableReplications) {
    for (TableReplication tableReplication : tableReplications.getTableReplications()) {
      SourceTable sourceTable = tableReplication.getSourceTable();
      String key = makeKey(sourceTable.getDatabaseName().trim().toLowerCase(),
          sourceTable.getTableName().trim().toLowerCase());
      if (replicationConfigMap.containsKey(key)) {
        throw new CircusTrainException(
            "Partitioning can not be carried out when there are duplicate source tables with partitioning configured");
      }
      log.info("Loading BigQuery partitioning configuration for table {}", key);
      replicationConfigMap.put(key, tableReplication.getCopierOptions());
    }
  }

  public String getPartitionFilter(Table table) {
    String key = makeKey(table);
    log.info("Loading 'partition-filter' for table {}", key);
    if (replicationConfigMap.containsKey(key)) {
      Object o = replicationConfigMap.get(key).get(PARTITION_FILTER);
      if (o == null) {
        return null;
      }
      return o.toString();
    }
    return null;
  }

  public String getPartitionBy(Table table) {
    String key = makeKey(table);
    log.info("Loading 'partition-by' for table {}", key);
    if (replicationConfigMap.containsKey(key)) {
      Object o = replicationConfigMap.get(key).get(PARTITION_BY);
      if (o == null) {
        log.info("Object is null");
        return null;
      }
      return o.toString();
    }
    return null;
  }

  public boolean partitioningConfigured(Table table) {
    return isNotBlank(getPartitionBy(table));
  }
}
