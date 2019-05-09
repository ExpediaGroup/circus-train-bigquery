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
package com.hotels.bdp.circustrain.bigquery.table.service.partitioned;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.bigquery.api.TableService;
import com.hotels.bdp.circustrain.bigquery.partition.BigQueryTableFilterer;
import com.hotels.bdp.circustrain.bigquery.partition.HivePartitionGenerator;
import com.hotels.bdp.circustrain.bigquery.partition.HivePartitionKeyAdder;

public class PartitionedTableService implements TableService {

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
  }

  @Override
  public Table getTable() {
    return adder
        .add(Objects.requireNonNull(partitionedBy).toLowerCase(Locale.ROOT), filteredTable.getDefinition().getSchema());
  }

  @Override
  public List<Partition> getPartitions() {
    Field partitionField = getPartitionField(filteredTable.getDefinition().getSchema());
    return factory.generate(partitionField.getName(), partitionField.getType().name(), result.iterateAll());
  }

  private Field getPartitionField(Schema schema) {
    if (filteredTable.getDefinition().getSchema() == null) {
      throw new IllegalStateException(
          "No schema defined for " + filteredTable.getFriendlyName() + "; cannot generate partitions");
    } else {
      return filteredTable.getDefinition().getSchema().getFields().get(partitionedBy);
    }
  }

}
