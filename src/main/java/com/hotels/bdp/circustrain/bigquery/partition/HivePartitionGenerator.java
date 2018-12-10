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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.RuntimeConfiguration;
import com.hotels.bdp.circustrain.bigquery.conversion.BigQueryToHivePartitionConverter;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionContainerFactory;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;
import com.hotels.bdp.circustrain.bigquery.util.SchemaExtractor;

public class HivePartitionGenerator {

  private static final Logger log = LoggerFactory.getLogger(HivePartitionGenerator.class);

  private final Table sourceTableAsHive;
  private final BigQueryMetastore bigQueryMetastore;
  private final ExtractionService extractionService;
  private final ExtractionContainerFactory extractionContainerFactory;

  private final SchemaExtractor schemaExtractor;

  public HivePartitionGenerator(
      Table sourceTableAsHive,
      BigQueryMetastore bigQueryMetastore,
      ExtractionService extractionService,
      SchemaExtractor schemaExtractor) {
    this(sourceTableAsHive, bigQueryMetastore, extractionService,
        new ExtractionContainerFactory(extractionService, bigQueryMetastore, sourceTableAsHive), schemaExtractor);
  }

  @VisibleForTesting
  HivePartitionGenerator(
      Table sourceTableAsHive,
      BigQueryMetastore bigQueryMetastore,
      ExtractionService extractionService,
      ExtractionContainerFactory extractionContainerFactory,
      SchemaExtractor schemaExtractor) {
    this.sourceTableAsHive = sourceTableAsHive;
    this.bigQueryMetastore = bigQueryMetastore;
    this.extractionService = extractionService;
    this.extractionContainerFactory = extractionContainerFactory;
    this.schemaExtractor = schemaExtractor;
  }

  public List<Partition> generate(
      final String partitionKey,
      String partitionKeyType,
      Iterable<FieldValueList> results) {
    ExecutorService executorService = Executors.newFixedThreadPool(RuntimeConfiguration.DEFAULT.getThreadPoolSize());
    List<Partition> partitions = generate(executorService, partitionKey, partitionKeyType, results);
    executorService.shutdownNow();
    return partitions;
  }

  private List<Partition> generate(
      final ExecutorService executorService,
      String partitionKey,
      String partitionKeyType,
      Iterable<FieldValueList> results) {
    final String sourceTableName = sourceTableAsHive.getTableName();
    final String sourceDBName = sourceTableAsHive.getDbName();

    ExtractionContainer container = extractionContainerFactory.newInstance();
    final String tableBucket = container.getExtractionUri().getBucket();
    final String tableFolder = container.getExtractionUri().getFolder();

    List<GeneratePartitionTask> tasks = getTasks(sourceDBName, sourceTableName, partitionKey, partitionKeyType,
        tableBucket, tableFolder, results);

    try {
      List<Future<Optional<Partition>>> partitionFutures = executorService.invokeAll(tasks);
      List<Partition> generatedPartitions = new ArrayList<>();
      for (Future<Optional<Partition>> future : partitionFutures) {
        Optional<Partition> optionalPartition = future.get();
        if (optionalPartition.isPresent()) {
          generatedPartitions.add(optionalPartition.get());
        }
      }
      return generatedPartitions;
    } catch (InterruptedException | ExecutionException e) {
      throw new CircusTrainException(
          String.format("Couldn't generate Partitions for table %s.%s", sourceDBName, sourceTableName), e);
    }
  }

  private List<GeneratePartitionTask> getTasks(
      String sourceDBName,
      String sourceTableName,
      String partitionKey,
      String partitionKeyType,
      String tableBucket,
      String tableFolder,
      Iterable<FieldValueList> rows) {
    List<GeneratePartitionTask> tasks = new ArrayList<>();
    for (FieldValueList row : rows) {
      tasks
          .add(new GeneratePartitionTask(sourceDBName, sourceTableName, partitionKey, partitionKeyType, tableBucket,
              tableFolder, row));
    }
    return tasks;
  }

  private final class GeneratePartitionTask implements Callable<Optional<Partition>> {

    private final String sourceDBName;
    private final String sourceTableName;
    private final String partitionKey;
    private final String partitionKeyType;
    private final String tableBucket;
    private final String tableFolder;
    private final FieldValueList row;

    private GeneratePartitionTask(
        String sourceDBName,
        String sourceTableName,
        String partitionKey,
        String partitionKeyType,
        String tableBucket,
        String tableFolder,
        FieldValueList row) {
      this.sourceDBName = sourceDBName;
      this.sourceTableName = sourceTableName;
      this.partitionKey = partitionKey;
      this.partitionKeyType = partitionKeyType;
      this.tableBucket = tableBucket;
      this.tableFolder = tableFolder;
      this.row = row;
    }

    @Override
    public Optional<Partition> call() throws Exception {
      return generatePartition();
    }

    private Optional<Partition> generatePartition() {
      FieldValue partitionFieldValue = row.get(partitionKey);
      if (partitionFieldValue != null) {
        final String originalValue = partitionFieldValue.getValue().toString();
        String formattedValue = PartitionValueFormatter.formatValue(partitionFieldValue, partitionKeyType);
        Partition partition = new BigQueryToHivePartitionConverter().convert();
        ExtractionUri extractionUri = new BigQueryPartitionGenerator(bigQueryMetastore, extractionService, sourceDBName,
            sourceTableName, partitionKey, formattedValue, tableBucket, tableFolder, schemaExtractor)
                .generatePartition(partition);
        setPartitionParameters(partition, sourceTableAsHive.getDbName(), sourceTableAsHive.getTableName(),
            extractionUri.getTableLocation(), originalValue);

        log.info("Generated partition {}={}", partitionKey, originalValue);
        return Optional.of(partition);
      }
      return Optional.absent();
    }

    private void setPartitionParameters(
        Partition partition,
        String databaseName,
        String tableName,
        String location,
        String... partitionValues) {
      partition.setDbName(databaseName);
      partition.setTableName(tableName);
      partition.getSd().setLocation(location);
      partition.setValues(Arrays.asList(partitionValues));
    }
  }

}
