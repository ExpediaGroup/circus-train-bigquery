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
package com.hotels.bdp.circustrain.bigquery.table.service.partitioned;

import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.bigquery.api.TableService;
import com.hotels.bdp.circustrain.bigquery.partition.BigQueryTableFilterer;
import com.hotels.bdp.circustrain.bigquery.partition.HivePartitionGenerator;
import com.hotels.bdp.circustrain.bigquery.partition.HivePartitionKeyAdder;

public class PartitionedTableService implements TableService {
  private static final Logger log = LoggerFactory.getLogger(PartitionedTableService.class);

  private final String partitionedBy;
  private final HivePartitionKeyAdder adder;
  private final HivePartitionGenerator factory;
  private final TableResult result;
  private final com.google.cloud.bigquery.Table filteredTable;

  public PartitionedTableService(
      String partitionedBy,
      BigQueryTableFilterer filterer,
      HivePartitionKeyAdder adder,
      HivePartitionGenerator factory) {
    this(partitionedBy, adder, factory, filterer.filterTable(), filterer.getFilteredTable());
  }

  @VisibleForTesting
  PartitionedTableService(
      String partitionedBy,
      HivePartitionKeyAdder adder,
      HivePartitionGenerator factory,
      TableResult result,
      com.google.cloud.bigquery.Table filteredTable) {
    this.partitionedBy = partitionedBy;
    this.adder = adder;
    this.factory = factory;
    this.result = result;
    this.filteredTable = filteredTable;

    try {
      log.info("result : {}", result.iterateAll().iterator().next().get(0).toString());
    } catch (Exception e) {
      log.info("could not get the result wanted: results.iterator().next().get(0).toString()");
    }
  }

  @Override
  public Table getTable() {
    return adder.add(Objects.requireNonNull(partitionedBy).toLowerCase(), filteredTable.getDefinition().getSchema());
  }

  @Override
  public List<Partition> getPartitions() {
    return factory.generate(getPartitionKey(), getPartitionKeyType(), result.iterateAll());
  }

  private String getPartitionKey() {
    Schema schema = filteredTable.getDefinition().getSchema();
    if (schema == null) {
      return "";
    }

    return sanitisePartitionKey(schema);
  }

  private String getPartitionKeyType() {
    if (tableContainsField()) {
      int position = filteredTable.getDefinition().getSchema().getFields().getIndex(partitionedBy);
      return filteredTable.getDefinition().getSchema().getFields().get(position).getType().name();
    } else {
      return null;
    }
  }

  private boolean tableContainsField() {
    return filteredTable.getDefinition().getSchema().getFields().size() > 0;
  }

  private String sanitisePartitionKey(Schema schema) {
    // Case sensitive in Google Cloud
    String partitionKey = partitionedBy;
    for (Field field : schema.getFields()) {
      String trimmedFieldName = field.getName().toLowerCase().trim();
      String trimmedPartitionKey = partitionKey.toLowerCase().trim();

      if (trimmedFieldName.equals(trimmedPartitionKey)) {
        partitionKey = field.getName();
        break;
      }
    }
    return partitionKey;
  }
}
