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
package com.hotels.bdp.circustrain.bigquery.extraction;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.Table;
import com.google.common.annotations.VisibleForTesting;

//TODO: Refactor classes in this package
public class BigQueryDataExtractionManager {

  private static final Logger log = LoggerFactory.getLogger(BigQueryDataExtractionManager.class);

  private final BigQueryDataExtractionService service;
  private final Map<Table, ExtractionContainer> locationMap;

  public BigQueryDataExtractionManager(BigQueryDataExtractionService service) {
    this(service, new LinkedHashMap<Table, ExtractionContainer>());
  }

  @VisibleForTesting
  BigQueryDataExtractionManager(BigQueryDataExtractionService service, Map<Table, ExtractionContainer> locationMap) {
    this.service = service;
    this.locationMap = locationMap;
  }

  public void register(Table table) {
    this.register(table, new BigQueryExtractionData(), false);
  }

  public void register(Table table, boolean deleteTable) {
    this.register(table, new BigQueryExtractionData(), deleteTable);
  }

  public void register(Table table, BigQueryExtractionData extractionData) {
    Pair<BigQueryExtractionData, Boolean> deleteInfo = Pair.of(extractionData, false);
    this.register(table, extractionData, false);
  }

  public void register(Table table, BigQueryExtractionData extractionData, boolean deleteTable) {
    if (locationMap.containsKey(table)) {
      log.debug("Table {}.{} already registered for extraction. Skipping registration", table.getTableId().getDataset(),
          table.getTableId().getTable());
    }
    ExtractionContainer deleteInfo = new ExtractionContainer(extractionData, deleteTable, false);
    log.debug("Registering table {}.{} for extraction to {}", table.getTableId().getDataset(),
        table.getTableId().getTable(), extractionData.getUri());
    locationMap.put(table, deleteInfo);
  }

  public void extractAll() {
    for (Map.Entry<Table, ExtractionContainer> entry : locationMap.entrySet()) {
      Table table = entry.getKey();
      ExtractionContainer extractionContainer = entry.getValue();
      BigQueryExtractionData extractionData = extractionContainer.getExtractionData();
      Boolean extracted = extractionContainer.getExtracted();
      if (!extracted) {
        log.debug("Extracting table: {}.{}", table.getTableId().getDataset(), table.getTableId().getTable());
        service.extract(table, extractionData);
        extractionContainer.setExtracted(true);
      }
    }
  }

  public void cleanupAll() {
    Set<String> bucketsToDelete = new HashSet<>();
    for (Map.Entry<Table, ExtractionContainer> entry : locationMap.entrySet()) {
      Table table = entry.getKey();
      ExtractionContainer extractionContainer = entry.getValue();
      BigQueryExtractionData extractionData = extractionContainer.getExtractionData();
      Boolean deleteTable = extractionContainer.getDeleteTable();
      log.debug("Cleaning data from table:  {}.{}", table.getTableId().getDataset(), table.getTableId().getTable());
      bucketsToDelete.add(extractionData.getBucket().toLowerCase().trim());
      if (deleteTable) {
        log.debug("Deleting table {}.{}", table.getTableId().getDataset(), table.getTableId().getTable());
        table.delete();
      }
    }
    for (String bucket : bucketsToDelete) {
      service.deleteBucketAndContents(bucket);
    }
  }

  public String getExtractedDataBaseLocation(Table table) {
    if (!locationMap.containsKey(table)) {
      return null;
    }
    return "gs://"
        + locationMap.get(table).getExtractionData().getBucket()
        + "/"
        + locationMap.get(table).getExtractionData().getFolder()
        + "/";
  }

  public String getExtractedDataUri(Table table) {
    if (!locationMap.containsKey(table)) {
      return null;
    }
    return locationMap.get(table).getExtractionData().getUri();
  }

  public String getExtractedDataBucket(Table table) {
    if (!locationMap.containsKey(table)) {
      return null;
    }
    return locationMap.get(table).getExtractionData().getBucket();
  }

  public String getExtractedDataFolder(Table table) {
    if (!locationMap.containsKey(table)) {
      return null;
    }
    return locationMap.get(table).getExtractionData().getFolder();
  }

  public String getExtractedDataKey(Table table) {
    if (!locationMap.containsKey(table)) {
      return null;
    }
    return locationMap.get(table).getExtractionData().getKey();
  }
}
