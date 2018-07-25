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

import static com.hotels.bdp.circustrain.bigquery.util.BigQueryUriUtils.randomUri;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;

import com.hotels.bdp.circustrain.bigquery.context.CircusTrainBigQueryConfiguration;
import com.hotels.bdp.circustrain.bigquery.conversion.BigQueryToHivePartitionConverter;
import com.hotels.bdp.circustrain.bigquery.conversion.BigQueryToHiveTypeConverter;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;

//TODO: Refactor for test
public class PartitionGenerator {

  private static final Logger log = LoggerFactory.getLogger(PartitionGenerator.class);

  private final BigQueryMetastore bigQueryMetastore;
  private final CircusTrainBigQueryConfiguration configuration;
  private final ExtractionService service;

  public PartitionGenerator(
      BigQueryMetastore bigQueryMetastore,
      CircusTrainBigQueryConfiguration configuration,
      ExtractionService service) {
    this.bigQueryMetastore = bigQueryMetastore;
    this.configuration = configuration;
    this.service = service;
  }

  public List<Partition> generate(Table table) {
    if (shouldPartition()) {
      log.info("Partitioning table {}.{} with key '{}' and filter '{}'", table.getDbName(), table.getTableName(),
          getPartitionBy(), getPartitionFilter());
      return applyPartitionFilter(table);
    } else {
      log.info("Partitioning not configured for table {}.{}. No filter applied", table.getDbName(),
          table.getTableName());
      return new ArrayList<>();
    }
  }

  private boolean shouldPartition() {
    return isNotBlank(getPartitionBy());
  }

  private String getPartitionFilter() {
    if (configuration == null || configuration.getPartitionFilter() == null) {
      return null;
    }
    return configuration.getPartitionFilter().trim().toLowerCase();
  }

  private String getPartitionBy() {
    if (configuration == null || configuration.getPartitionBy() == null) {
      return null;
    }
    return configuration.getPartitionBy().trim().toLowerCase();
  }

  private List<Partition> applyPartitionFilter(Table hiveTable) {
    String selectStatement = getSelectStatment(hiveTable);
    String datasetName = hiveTable.getDbName();
    String tableName = randomTableName();
    log.info("Computing partitions");
    TableResult result = selectQueryIntoBigQueryTable(datasetName, tableName, selectStatement);
    log.info("Filtering table");
    com.google.cloud.bigquery.Table filteredTable = bigQueryMetastore.getTable(datasetName, tableName);

    ExtractionUri extractionUri = new ExtractionUri();
    ExtractionContainer container = new ExtractionContainer(filteredTable, extractionUri, false);
    service.register(container);

    log.info("Adding partition keys to source table metadata");
    addPartitionKeys(hiveTable, filteredTable.getDefinition().getSchema());
    log.info("Generating Hive Partitions");
    return generateHivePartitions(hiveTable, filteredTable, result);
  }

  private String getSelectStatment(Table hiveTable) {
    final String partitionBy = getPartitionBy();
    final String partitionFilter = getPartitionFilter();
    if (isNotBlank(partitionBy) && isNotBlank(partitionFilter)) {
      return String.format("select * from %s.%s where %s", hiveTable.getDbName(), hiveTable.getTableName(),
          getPartitionFilter());
    } else if (isNotBlank(partitionBy) && isBlank(partitionFilter)) {
      return String.format("select %s from %s.%s group by %s order by %s", partitionBy, hiveTable.getDbName(),
          hiveTable.getTableName(), partitionBy, partitionBy);
    } else {
      throw new IllegalStateException();
    }
  }

  private void addPartitionKeys(Table table, Schema filteredTableSchema) {
    if (filteredTableSchema == null) {
      return;
    }
    BigQueryToHiveTypeConverter typeConverter = new BigQueryToHiveTypeConverter();
    String partitionKey = Objects.requireNonNull(getPartitionBy()).toLowerCase();
    for (Field field : filteredTableSchema.getFields()) {
      String fieldName = field.getName().toLowerCase().trim();
      if (partitionKey.equals(fieldName)) {
        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.setName(fieldName);
        fieldSchema.setType(typeConverter.convert(field.getType().toString()).toLowerCase());
        table.addToPartitionKeys(fieldSchema);
        log.info("Adding partition key: {} to replication table object {}.{}", partitionKey, table.getDbName(),
            table.getTableName());
      }
    }
  }

  private TableResult selectQueryIntoBigQueryTable(String databaseName, String tableName, String partitionFilter) {
    log.debug("Executing '{}'", partitionFilter);
    return bigQueryMetastore.runJob(configureFilterJob(databaseName, tableName, partitionFilter));
  }

  private QueryJobConfiguration configureFilterJob(String databaseName, String tableName, String partitionFilter) {
    return QueryJobConfiguration
        .newBuilder(partitionFilter)
        .setDestinationTable(TableId.of(databaseName, tableName))
        .setUseLegacySql(true)
        .setAllowLargeResults(true)
        .build();
  }

  private String sanitisePartitionKey(Schema schema) {
    // Case sensitive in Google Cloud
    String partitionKey = getPartitionBy();
    for (Field field : schema.getFields()) {
      if (field.getName().toLowerCase().equals(partitionKey.toLowerCase())) {
        partitionKey = field.getName();
        break;
      }
    }
    return partitionKey;
  }

  private List<Partition> generateHivePartitions(
      Table hiveTable,
      com.google.cloud.bigquery.Table filteredTable,
      TableResult result) {
    Schema schema = filteredTable.getDefinition().getSchema();
    if (schema == null) {
      return new ArrayList<>();
    }

    final String partitionKey = sanitisePartitionKey(schema);
    log.info("Getting values for partition key '{}'", partitionKey);

    final String sourceTableName = hiveTable.getTableName();
    final String sourceDBName = hiveTable.getDbName();

    // TODO: Cache BQ tables
    com.google.cloud.bigquery.Table bigQueryRepresentation = bigQueryMetastore.getTable(hiveTable.getDbName(),
        hiveTable.getTableName());

    ExtractionContainer container = service.retrieve(bigQueryRepresentation);
    final String tableBucket = container.getExtractionUri().getBucket();
    final String tableFolder = container.getExtractionUri().getFolder();

    List<Partition> generatedPartitions = new ArrayList<>();
    List<FieldSchema> cols = Collections.unmodifiableList(hiveTable.getSd().getCols());
    for (FieldValueList row : result.iterateAll()) {
      Object o = row.get(partitionKey).getValue();
      if (o != null) {
        String value = o.toString();
        String valForQuery = null;
        if (isBlank(value) || value.contains(" ")) {
          // Value is empty string or string with spaces
          valForQuery = "\"" + value + "\"";
        } else {
          valForQuery = value;
        }
        final String statement = String.format("select * from %s.%s where %s = %s", sourceDBName, sourceTableName,
            partitionKey, valForQuery);

        String destinationTableName = randomTableName();
        selectQueryIntoBigQueryTable(sourceDBName, destinationTableName, statement);
        com.google.cloud.bigquery.Table part = bigQueryMetastore.getTable(sourceDBName, destinationTableName);
        String partitionBucket = tableBucket;
        String partitionFolder = tableFolder + "/" + randomUri();
        String fileId = value.replaceAll("[^A-Za-z0-9]", "")
            + new SimpleDateFormat("ddMMyyyyHHmmssSSSSSSS").format(new Date());
        String fileName = fileId + "-*";
        ExtractionUri extractionUri = new ExtractionUri(partitionBucket, partitionFolder, fileName);
        ExtractionContainer toRegister = new ExtractionContainer(part, extractionUri, true);
        service.register(toRegister);

        final String location = "gs://" + extractionUri.getBucket() + "/" + extractionUri.getFolder() + "/";
        log.info("Generated partition {}=\"{}\" with location {} using query {}", partitionKey, value, location,
            statement);

        Partition partition = new BigQueryToHivePartitionConverter()
            .withDatabaseName(hiveTable.getDbName())
            .withTableName(hiveTable.getTableName())
            .withValue(value)
            .withCols(cols)
            .withLocation(location)
            .convert();
        generatedPartitions.add(partition);
      }
    }
    return generatedPartitions;
  }

  private String randomTableName() {
    return UUID.randomUUID().toString().replaceAll("-", "_");
  }

}
