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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.FieldValueList;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;

class HivePartitionService {

  private static final Logger log = LoggerFactory.getLogger(HivePartitionService.class);

  private final Table sourceTableAsHive;
  private final BigQueryMetastore bigQueryMetastore;
  private final ExtractionService service;
  private final ExtractionContainerFactory extractionContainerFactory;

  HivePartitionService(Table sourceTableAsHive, BigQueryMetastore bigQueryMetastore, ExtractionService service) {
    this(sourceTableAsHive, bigQueryMetastore, service, new ExtractionContainerFactory(service, bigQueryMetastore, sourceTableAsHive));
  }

  @VisibleForTesting
  HivePartitionService(
      Table sourceTableAsHive,
      BigQueryMetastore bigQueryMetastore,
      ExtractionService service,
      ExtractionContainerFactory extractionContainerFactory) {
    this.sourceTableAsHive = sourceTableAsHive;
    this.bigQueryMetastore = bigQueryMetastore;
    this.service = service;
    this.extractionContainerFactory = extractionContainerFactory;
  }

  List<Partition> generate(final String partitionKey, Iterable<FieldValueList> results) {
    final String sourceTableName = sourceTableAsHive.getTableName();
    final String sourceDBName = sourceTableAsHive.getDbName();

    ExtractionContainer container = extractionContainerFactory.get();
    final String tableBucket = container.getExtractionUri().getBucket();
    final String tableFolder = container.getExtractionUri().getFolder();

    List<Partition> generatedPartitions = new ArrayList<>();
    List<FieldSchema> cols = Collections.unmodifiableList(sourceTableAsHive.getSd().getCols());
    PartitionValueFormatter formatter = new PartitionValueFormatter(partitionKey, cols);
    for (FieldValueList row : results) {
      Object o = row.get(partitionKey).getValue();
      if (o != null) {
        final String originalValue = o.toString();
        final String formattedValue = formatter.format(objectToHiveString(o));
        ExtractionUri extractionUri = new BigQueryPartitionService(bigQueryMetastore, service, sourceDBName,
            sourceTableName, partitionKey, formattedValue, tableBucket, tableFolder).generatePart();

        Partition partition = new HivePartitionFactory(sourceTableAsHive.getDbName(), sourceTableAsHive.getTableName(),
            new HivePartitionLocationConverter(extractionUri).get(), cols, originalValue).get();
        generatedPartitions.add(partition);
        log.info("Generated partition {}={}", partitionKey, formattedValue);
        log.debug("{}", partition);
      }
    }
    return generatedPartitions;
  }

  private String objectToHiveString(Object o) {
    final String originalValue = o.toString();
    if (isBlank(originalValue)) {
      return "__HIVE_DEFAULT_PARTITION__";
    }
    return originalValue;
  }

}
