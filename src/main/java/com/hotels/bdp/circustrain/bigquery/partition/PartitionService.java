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
import static com.hotels.bdp.circustrain.bigquery.partition.PartitionGenerationUtils.shouldPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;

import com.hotels.bdp.circustrain.bigquery.context.CircusTrainBigQueryConfiguration;

//TODO: Refactor for test
public class PartitionService {

  private static final Logger log = LoggerFactory.getLogger(PartitionService.class);

  private final CircusTrainBigQueryConfiguration configuration;
  private final BigQueryTableFilterer filterer;
  private final HiveParitionKeyAdder adder;
  private final HivePartitionGenerator factory;

  PartitionService(
      CircusTrainBigQueryConfiguration configuration,
      BigQueryTableFilterer filterer,
      HiveParitionKeyAdder adder,
      HivePartitionGenerator factory) {
    this.configuration = configuration;
    this.filterer = filterer;
    this.adder = adder;
    this.factory = factory;
  }

  public List<Partition> execute() {
    if (shouldPartition(configuration)) {
      log.info("Partitioning configured");
      return generatePartitions();
    } else {
      log.info("Partitioning not configured. No filter applied");
      return new ArrayList<>();
    }
  }

  private List<Partition> generatePartitions() {
    log.info("Filtering source table");
    TableResult result = filterer.filterTable();
    com.google.cloud.bigquery.Table filteredTable = filterer.getFilteredTable();

    log.info("Generating partition keys for source table metadata");
    adder.add(Objects.requireNonNull(getPartitionBy(configuration)).toLowerCase(),
        filteredTable.getDefinition().getSchema());

    log.info("Generating Hive Partitions for source table");
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
