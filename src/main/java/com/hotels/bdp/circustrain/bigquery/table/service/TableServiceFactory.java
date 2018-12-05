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
package com.hotels.bdp.circustrain.bigquery.table.service;

import static com.hotels.bdp.circustrain.bigquery.util.RandomStringGenerationUtils.randomTableName;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.bigquery.api.TableService;
import com.hotels.bdp.circustrain.bigquery.conf.PartitioningConfiguration;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.partition.BigQueryTableFilterer;
import com.hotels.bdp.circustrain.bigquery.partition.HivePartitionGenerator;
import com.hotels.bdp.circustrain.bigquery.partition.HivePartitionKeyAdder;
import com.hotels.bdp.circustrain.bigquery.partition.PartitionQueryFactory;
import com.hotels.bdp.circustrain.bigquery.table.service.partitioned.PartitionedTableService;
import com.hotels.bdp.circustrain.bigquery.table.service.unpartitioned.UnpartitionedTableService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;
import com.hotels.bdp.circustrain.bigquery.util.SchemaExtractor;

@Profile({ Modules.REPLICATION })
@Component
public class TableServiceFactory {

  private final BigQueryMetastore bigQueryMetastore;
  private final ExtractionService extractionService;
  private final Map<Table, TableService> cache;
  private final PartitionQueryFactory partitionQueryFactory;
  private final PartitioningConfiguration configuration;
  private final SchemaExtractor schemaExtractor;

  @Autowired
  public TableServiceFactory(
      BigQueryMetastore bigQueryMetastore,
      ExtractionService extractionService,
      PartitionQueryFactory partitionQueryFactory,
      PartitioningConfiguration configuration,
      SchemaExtractor schemaExtractor) {
    this(bigQueryMetastore, extractionService, new HashMap<Table, TableService>(), partitionQueryFactory, configuration,
        schemaExtractor);
  }

  @VisibleForTesting
  TableServiceFactory(
      BigQueryMetastore bigQueryMetastore,
      ExtractionService extractionService,
      Map<Table, TableService> cache,
      PartitionQueryFactory partitionQueryFactory,
      PartitioningConfiguration configuration,
      SchemaExtractor schemaExtractor) {
    this.bigQueryMetastore = bigQueryMetastore;
    this.extractionService = extractionService;
    this.cache = cache;
    this.partitionQueryFactory = partitionQueryFactory;
    this.configuration = configuration;
    this.schemaExtractor = schemaExtractor;
  }

  public TableService newInstance(Table hiveTable) {
    if (cache.containsKey(hiveTable)) {
      return cache.get(hiveTable);
    }

    TableService tableService = null;

    if (configuration.isPartitioningConfigured(hiveTable)) {
      final String partitionBy = configuration.getPartitionByFor(hiveTable);
      final String partitionFilter = configuration.getPartitionFilterFor(hiveTable);
      final String sqlFilterQuery = partitionQueryFactory.newInstance(hiveTable, partitionBy, partitionFilter);
      final String datasetName = hiveTable.getDbName();
      final String tableName = randomTableName();

      BigQueryTableFilterer filterer = new BigQueryTableFilterer(bigQueryMetastore, extractionService, datasetName,
          tableName, sqlFilterQuery);
      HivePartitionKeyAdder adder = new HivePartitionKeyAdder(hiveTable);
      HivePartitionGenerator hivePartitionGenerator = new HivePartitionGenerator(hiveTable, bigQueryMetastore,
          extractionService, schemaExtractor);
      tableService = new PartitionedTableService(partitionBy, filterer, adder, hivePartitionGenerator);
    } else {
      tableService = new UnpartitionedTableService(hiveTable);
    }
    cache.put(hiveTable, tableService);
    return tableService;
  }
}
