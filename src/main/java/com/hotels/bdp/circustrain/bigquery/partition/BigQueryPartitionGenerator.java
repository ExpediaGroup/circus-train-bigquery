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
package com.hotels.bdp.circustrain.bigquery.partition;

import static com.hotels.bdp.circustrain.bigquery.util.RandomStringGenerationUtils.randomTableName;
import static com.hotels.bdp.circustrain.bigquery.util.RandomStringGenerationUtils.randomUri;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.Table;

import com.hotels.bdp.circustrain.bigquery.extraction.container.DeleteTableAction;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.extraction.container.PostExtractionAction;
import com.hotels.bdp.circustrain.bigquery.extraction.container.UpdatePartitionSchemaAction;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;
import com.hotels.bdp.circustrain.bigquery.util.SchemaExtractor;

class BigQueryPartitionGenerator {

  private static final Logger log = LoggerFactory.getLogger(BigQueryPartitionGenerator.class);

  private final BigQueryMetastore bigQueryMetastore;
  private final ExtractionService extractionService;
  
  private final org.apache.hadoop.hive.metastore.api.Table sourceTableAsHive;
  
  private final String sourceDBName;
  private final String sourceTableName;
  private final String partitionKey;
  private final String partitionValue;
  private final String destinationBucket;
  private final String destinationFolder;

  private final SchemaExtractor schemaExtractor;

  BigQueryPartitionGenerator(
      BigQueryMetastore bigQueryMetastore,
      ExtractionService extractionService,
      org.apache.hadoop.hive.metastore.api.Table sourceTableAsHive,
      // String sourceDBName,
      // String sourceTableName,
      String partitionKey,
      String partitionValue,
      String destinationBucket,
      String destinationFolder,
      SchemaExtractor schemaExtractor) {
    this.bigQueryMetastore = bigQueryMetastore;
    this.extractionService = extractionService;
    this.sourceTableAsHive = sourceTableAsHive;
    this.sourceDBName = sourceTableAsHive.getDbName();
    this.sourceTableName = sourceTableAsHive.getTableName();
    this.partitionKey = partitionKey;
    this.partitionValue = partitionValue;
    this.destinationBucket = destinationBucket;
    this.destinationFolder = destinationFolder;
    this.schemaExtractor = schemaExtractor;
  }

  ExtractionUri generatePartition(Partition partition) {
    final String statement = getQueryStatement();
    final String destinationTableName = randomTableName();

    Table bigQueryPartition = createPartitionInBigQuery(sourceDBName, destinationTableName, statement);
    ExtractionUri extractionUri = scheduleForExtraction(bigQueryPartition, partition, sourceTableAsHive);
    return extractionUri;
  }

  private String getQueryStatement() {
    String query = String
        .format("select * except (%s) from %s.%s where %s = %s", partitionKey, sourceDBName, sourceTableName,
            partitionKey, partitionValue);
    log.info("Query statement is: {}", query);
    return query;
  }

  private Table createPartitionInBigQuery(
      String destinationDBName,
      String destinationTableName,
      String queryStatement) {
    log.debug("Generating BigQuery partition using query {}", queryStatement);
    bigQueryMetastore.executeIntoDestinationTable(destinationDBName, destinationTableName, queryStatement);
    Table part = bigQueryMetastore.getTable(destinationDBName, destinationTableName);
    return part;
  }

  private ExtractionUri scheduleForExtraction(
      Table table,
      Partition partition,
      org.apache.hadoop.hive.metastore.api.Table sourceTableAsHive) {
    ExtractionUri extractionUri = new ExtractionUri(destinationBucket, generateFolderName(), generateFileName());
    PostExtractionAction deleteTableAction = new DeleteTableAction(table);

    PostExtractionAction updatePartitionSchemaAction = new UpdatePartitionSchemaAction(partition,
        extractionService.getStorage(), extractionUri, schemaExtractor, sourceTableAsHive);

    ExtractionContainer toRegister = new ExtractionContainer(table, extractionUri,
        Arrays.asList(deleteTableAction, updatePartitionSchemaAction));
    extractionService.register(toRegister);
    return extractionUri;
  }

  private String generateFileName() {
    return partitionKey + "=" + pruneQuotes(partitionValue.replaceAll("\\s", "_"));
  }

  private String generateFolderName() {
    return destinationFolder + "/" + randomUri();
  }

  private String pruneQuotes(String partitionValue) {
    if (partitionValue.startsWith("\"") && partitionValue.endsWith("\"")) {
      return StringUtils.removeStart(StringUtils.removeEnd(partitionValue, "\""), "\"");
    }
    return partitionValue;
  }
}
