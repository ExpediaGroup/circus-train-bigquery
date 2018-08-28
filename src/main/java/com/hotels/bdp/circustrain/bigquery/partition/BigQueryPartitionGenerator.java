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

import static com.hotels.bdp.circustrain.bigquery.util.RandomStringGenerationUtils.randomTableName;
import static com.hotels.bdp.circustrain.bigquery.util.RandomStringGenerationUtils.randomUri;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.extraction.container.PostExtractionAction;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;

class BigQueryPartitionGenerator {

  private static final Logger log = LoggerFactory.getLogger(BigQueryPartitionGenerator.class);

  private final BigQueryMetastore bigQueryMetastore;
  private final ExtractionService extractionService;
  private final String sourceDBName;
  private final String sourceTableName;
  private final String partitionKey;
  private final String partitionValue;
  private final String destinationBucket;
  private final String destinationFolder;

  BigQueryPartitionGenerator(
      BigQueryMetastore bigQueryMetastore,
      ExtractionService extractionService,
      String sourceDBName,
      String sourceTableName,
      String partitionKey,
      String partitionValue,
      String destinationBucket,
      String destinationFolder) {
    this.bigQueryMetastore = bigQueryMetastore;
    this.extractionService = extractionService;
    this.partitionValue = partitionValue;
    this.sourceDBName = sourceDBName;
    this.sourceTableName = sourceTableName;
    this.partitionKey = partitionKey;
    this.destinationBucket = destinationBucket;
    this.destinationFolder = destinationFolder;
  }

  ExtractionUri generatePartition() {
    final String statement = getQueryStatement();
    final String destinationTableName = randomTableName();

    com.google.cloud.bigquery.Table part = createPartitionInBigQuery(sourceDBName, destinationTableName, statement);
    ExtractionUri extractionUri = scheduleForExtraction(part);
    return extractionUri;

  }

  private String getQueryStatement() {
    return String.format("select * from %s.%s where %s = %s", sourceDBName, sourceTableName, partitionKey,
        partitionValue);
  }

  private com.google.cloud.bigquery.Table createPartitionInBigQuery(
      String destinationDBName,
      String destinationTableName,
      String queryStatement) {
    log.debug("Generating BigQuery partition using query {}", queryStatement);
    bigQueryMetastore.executeIntoDestinationTable(destinationDBName, destinationTableName, queryStatement);
    com.google.cloud.bigquery.Table part = bigQueryMetastore.getTable(destinationDBName, destinationTableName);
    return part;
  }

  private ExtractionUri scheduleForExtraction(com.google.cloud.bigquery.Table table) {
    ExtractionUri extractionUri = new ExtractionUri(destinationBucket, generateFolderName(), generateFileName());
    ExtractionContainer toRegister = new ExtractionContainer(table, extractionUri, PostExtractionAction.DELETE);
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
