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

import static com.hotels.bdp.circustrain.bigquery.partition.PartitionGenerationUtils.getPartitionBy;

import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.bigquery.context.CircusTrainBigQueryConfiguration;

public class TableService {

  private final CircusTrainBigQueryConfiguration configuration;
  private final HiveParitionKeyAdder adder;
  private final HivePartitionService factory;
  private final TableResult result;
  private final com.google.cloud.bigquery.Table filteredTable;

  TableService(
      CircusTrainBigQueryConfiguration configuration,
      BigQueryTableFilterer filterer,
      HiveParitionKeyAdder adder,
      HivePartitionService factory) {
    this(configuration, adder, factory, filterer.filterTable(), filterer.getFilteredTable());
  }

  @VisibleForTesting
  TableService(
      CircusTrainBigQueryConfiguration configuration,
      HiveParitionKeyAdder adder,
      HivePartitionService factory,
      TableResult result,
      com.google.cloud.bigquery.Table filteredTable) {
    this.configuration = configuration;
    this.adder = adder;
    this.factory = factory;
    this.result = result;
    this.filteredTable = filteredTable;
  }

  public Table getTable() {
    return adder.add(Objects.requireNonNull(getPartitionBy(configuration)).toLowerCase(),
        filteredTable.getDefinition().getSchema());
  }

  public List<Partition> getPartitions() {
    return factory.generate(getPartitionKey(filteredTable), result.iterateAll());
  }

  private String getPartitionKey(com.google.cloud.bigquery.Table table) {
    Schema schema = table.getDefinition().getSchema();
    if (schema == null) {
      return "";
    }

    return sanitisePartitionKey(schema);
  }

  private String sanitisePartitionKey(Schema schema) {
    // Case sensitive in Google Cloud
    String partitionKey = getPartitionBy(configuration);
    for (Field field : schema.getFields()) {
      if (field.getName().toLowerCase().equals(partitionKey.toLowerCase())) {
        partitionKey = field.getName();
        break;
      }
    }
    return partitionKey;
  }
}
