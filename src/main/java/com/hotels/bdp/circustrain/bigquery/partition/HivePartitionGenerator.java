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

import static com.hotels.bdp.circustrain.bigquery.RuntimeConstants.DEFAULT_THREADPOOL_SIZE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.FieldValueList;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.conversion.BigQueryToHivePartitionConverter;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionContainerFactory;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.util.CircusTrainBigQueryMetastore;

public class HivePartitionGenerator {

  private static final Logger log = LoggerFactory.getLogger(HivePartitionGenerator.class);

  private final Table sourceTableAsHive;
  private final CircusTrainBigQueryMetastore bigQueryMetastore;
  private final ExtractionService extractionService;
  private final ExtractionContainerFactory extractionContainerFactory;

  public HivePartitionGenerator(
      Table sourceTableAsHive,
      CircusTrainBigQueryMetastore bigQueryMetastore,
      ExtractionService extractionService) {
    this(sourceTableAsHive, bigQueryMetastore, extractionService,
        new ExtractionContainerFactory(extractionService, bigQueryMetastore, sourceTableAsHive));
  }

  @VisibleForTesting
  HivePartitionGenerator(
      Table sourceTableAsHive,
      CircusTrainBigQueryMetastore bigQueryMetastore,
      ExtractionService extractionService,
      ExtractionContainerFactory extractionContainerFactory) {
    this.sourceTableAsHive = sourceTableAsHive;
    this.bigQueryMetastore = bigQueryMetastore;
    this.extractionService = extractionService;
    this.extractionContainerFactory = extractionContainerFactory;
  }

  public List<Partition> generate(final String partitionKey, Iterable<FieldValueList> results) {
    ExecutorService executorService = Executors.newFixedThreadPool(DEFAULT_THREADPOOL_SIZE);
    List<Partition> partitions = generate(executorService, partitionKey, results);
    executorService.shutdownNow();
    return partitions;
  }

  private List<Partition> generate(
      final ExecutorService executorService,
      String partitionKey,
      Iterable<FieldValueList> results) {
    final String sourceTableName = sourceTableAsHive.getTableName();
    final String sourceDBName = sourceTableAsHive.getDbName();

    ExtractionContainer container = extractionContainerFactory.get();
    final String tableBucket = container.getExtractionUri().getBucket();
    final String tableFolder = container.getExtractionUri().getFolder();

    List<FieldSchema> cols = Collections.unmodifiableList(sourceTableAsHive.getSd().getCols());
    PartitionValueFormatter formatter = new PartitionValueFormatter(partitionKey, cols);

    List<GeneratePartitionTask> tasks = getTasks(sourceDBName, sourceTableName, partitionKey, tableBucket, tableFolder,
        results, cols, formatter);

    try {
      List<Future<com.google.common.base.Optional<Partition>>> partitionFutures = executorService.invokeAll(tasks);
      List<Partition> generatedPartitions = new ArrayList<>();
      for (Future<Optional<Partition>> future : partitionFutures) {
        com.google.common.base.Optional<Partition> optionalPartition = future.get();
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
      String tableBucket,
      String tableFolder,
      Iterable<FieldValueList> rows,
      List<FieldSchema> cols,
      PartitionValueFormatter formatter) {
    List<GeneratePartitionTask> tasks = new ArrayList<>();
    for (FieldValueList row : rows) {
      tasks.add(new GeneratePartitionTask(sourceDBName, sourceTableName, partitionKey, tableBucket, tableFolder, row,
          cols, formatter));
    }
    return tasks;
  }

  private class GeneratePartitionTask implements Callable<com.google.common.base.Optional<Partition>> {

    private final String sourceDBName;
    private final String sourceTableName;
    private final String partitionKey;
    private final String tableBucket;
    private final String tableFolder;
    private final FieldValueList row;
    private final List<FieldSchema> cols;
    private final PartitionValueFormatter formatter;

    private GeneratePartitionTask(
        String sourceDBName,
        String sourceTableName,
        String partitionKey,
        String tableBucket,
        String tableFolder,
        FieldValueList row,
        List<FieldSchema> cols,
        PartitionValueFormatter formatter) {
      this.sourceDBName = sourceDBName;
      this.sourceTableName = sourceTableName;
      this.partitionKey = partitionKey;
      this.tableBucket = tableBucket;
      this.tableFolder = tableFolder;
      this.row = row;
      this.cols = cols;
      this.formatter = formatter;
    }

    @Override
    public com.google.common.base.Optional<Partition> call() throws Exception {
      return generatePartition();
    }

    private com.google.common.base.Optional<Partition> generatePartition() {
      Object o = row.get(partitionKey).getValue();
      if (o != null) {
        final String originalValue = o.toString();
        final String formattedValue = formatter.format(objectToHiveString(o));
        ExtractionUri extractionUri = new BigQueryPartitionGenerator(bigQueryMetastore, extractionService, sourceDBName,
            sourceTableName, partitionKey, formattedValue, tableBucket, tableFolder).generatePart();

        Partition partition = new HivePartitionFactory(sourceTableAsHive.getDbName(), sourceTableAsHive.getTableName(),
            new HivePartitionLocationConverter(extractionUri).get(), cols, originalValue).get();
        log.info("Generated partition {}={}", partitionKey, formattedValue);
        log.debug("{}", partition);
        return com.google.common.base.Optional.of(partition);
      }
      return com.google.common.base.Optional.absent();
    }

    private String objectToHiveString(Object o) {
      final String originalValue = o.toString();
      if (isBlank(originalValue)) {
        return "__HIVE_DEFAULT_PARTITION__";
      }
      return originalValue;
    }
  }

  private class HivePartitionFactory {

    private final Partition partition;

    HivePartitionFactory(
        String databaseName,
        String tableName,
        String location,
        List<FieldSchema> cols,
        String... partitionValues) {
      this(databaseName, tableName, location, cols, Arrays.asList(partitionValues));
    }

    private HivePartitionFactory(
        String databaseName,
        String tableName,
        String location,
        List<FieldSchema> cols,
        List<String> partitionValues) {
      this.partition = new BigQueryToHivePartitionConverter()
          .withDatabaseName(databaseName)
          .withTableName(tableName)
          .withValues(partitionValues)
          .withCols(cols)
          .withLocation(location)
          .convert();
    }

    public Partition get() {
      return partition;
    }
  }

}
