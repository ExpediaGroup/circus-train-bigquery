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
package com.hotels.bdp.circustrain.bigquery.partition;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import static com.hotels.bdp.circustrain.bigquery.partition.PartitionGenerationUtils.getPartitionBy;
import static com.hotels.bdp.circustrain.bigquery.partition.PartitionGenerationUtils.getPartitionFilter;
import static com.hotels.bdp.circustrain.bigquery.partition.PartitionGenerationUtils.randomTableName;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;

import com.hotels.bdp.circustrain.bigquery.context.CircusTrainBigQueryConfiguration;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;

public class TableServiceFactory {

  private final CircusTrainBigQueryConfiguration configuration;
  private final BigQueryMetastore bigQueryMetastore;
  private final ExtractionService service;

  private final Map<Table, TableService> cache = new HashMap<>();

  public TableServiceFactory(
      CircusTrainBigQueryConfiguration configuration,
      BigQueryMetastore bigQueryMetastore,
      ExtractionService service) {
    this.configuration = configuration;
    this.bigQueryMetastore = bigQueryMetastore;
    this.service = service;
  }

  public TableService newInstance(Table hiveTable) {
    if (cache.containsKey(hiveTable)) {
      return cache.get(hiveTable);
    }

    final String sqlFilterQuery = getSelectStatment(hiveTable);
    final String datasetName = hiveTable.getDbName();
    final String tableName = randomTableName();

    BigQueryTableFilterer filterer = new BigQueryTableFilterer(bigQueryMetastore, service, datasetName, tableName, sqlFilterQuery);
    HiveParitionKeyAdder adder = new HiveParitionKeyAdder(hiveTable);
    HivePartitionGenerator hivePartitionGenerator = new HivePartitionGenerator(hiveTable, bigQueryMetastore, service);
    TableService service = new TableService(configuration, filterer, adder, hivePartitionGenerator);
    cache.put(hiveTable, service);
    return service;
  }

  private String getSelectStatment(Table hiveTable) {
    final String partitionBy = getPartitionBy(configuration);
    final String partitionFilter = getPartitionFilter(configuration);
    if (isNotBlank(partitionBy) && isNotBlank(partitionFilter)) {
      return String.format("select * from %s.%s where %s", hiveTable.getDbName(), hiveTable.getTableName(),
          getPartitionFilter(configuration));
    } else if (isNotBlank(partitionBy) && isBlank(partitionFilter)) {
      return String.format("select %s from %s.%s group by %s order by %s", partitionBy, hiveTable.getDbName(),
          hiveTable.getTableName(), partitionBy, partitionBy);
    } else {
      throw new IllegalStateException();
    }
  }
}
