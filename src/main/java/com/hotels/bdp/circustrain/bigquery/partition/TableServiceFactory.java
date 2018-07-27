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

import static com.hotels.bdp.circustrain.bigquery.partition.PartitionGenerationUtils.randomTableName;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.bigquery.context.CircusTrainBigQueryConfiguration;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;

public class TableServiceFactory {

  private final CircusTrainBigQueryConfiguration configuration;
  private final BigQueryMetastore bigQueryMetastore;
  private final ExtractionService service;
  private final Map<Table, TableService> cache;
  private final PartitionQueryFactory partitionQueryFactory;

  public TableServiceFactory(
      CircusTrainBigQueryConfiguration configuration,
      BigQueryMetastore bigQueryMetastore,
      ExtractionService service) {
    this(configuration, bigQueryMetastore, service, new HashMap<Table, TableService>());
  }

  @VisibleForTesting
  TableServiceFactory(
      CircusTrainBigQueryConfiguration configuration,
      BigQueryMetastore bigQueryMetastore,
      ExtractionService service,
      Map<Table, TableService> cache) {
    this(configuration, bigQueryMetastore, service, cache, new PartitionQueryFactory(configuration));
  }

  @VisibleForTesting
  TableServiceFactory(
      CircusTrainBigQueryConfiguration configuration,
      BigQueryMetastore bigQueryMetastore,
      ExtractionService service,
      Map<Table, TableService> cache,
      PartitionQueryFactory partitionQueryFactory) {
    this.configuration = configuration;
    this.bigQueryMetastore = bigQueryMetastore;
    this.service = service;
    this.cache = cache;
    this.partitionQueryFactory = partitionQueryFactory;
  }

  public TableService newInstance(Table hiveTable) {
    if (cache.containsKey(hiveTable)) {
      return cache.get(hiveTable);
    }

    final String sqlFilterQuery = partitionQueryFactory.get(hiveTable);
    final String datasetName = hiveTable.getDbName();
    final String tableName = randomTableName();

    BigQueryTableFilterer filterer = new BigQueryTableFilterer(bigQueryMetastore, service, datasetName, tableName,
        sqlFilterQuery);
    HiveParitionKeyAdder adder = new HiveParitionKeyAdder(hiveTable);
    HivePartitionService hivePartitionService = new HivePartitionService(hiveTable, bigQueryMetastore, service);
    TableService service = new TableService(configuration, filterer, adder, hivePartitionService);
    cache.put(hiveTable, service);
    return service;
  }

}
