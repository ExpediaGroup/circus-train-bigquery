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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.Table;

public class BigQueryDataExtractionManager {

  private static final Logger log = LoggerFactory.getLogger(BigQueryDataExtractionManager.class);

  private final BigQueryDataExtractionService service;

  private Table table;
  private BigQueryExtractionData data = new BigQueryExtractionData();

  public BigQueryDataExtractionManager(BigQueryDataExtractionService service) {
    this.service = service;
  }

  public void register(Table table) {
    this.table = table;
  }

  public void extract() {
    log.info("Extracting table: {}.{}", table.getTableId().getDataset(), table.getTableId().getTable());
    service.extract(table, data);
  }

  public void cleanup() {
    log.info("Cleaning data from table:  {}.{}", table.getTableId().getDataset(), table.getTableId().getTable());
    service.cleanup(data);
  }

  public String location() {
    return "gs://" + data.getDataBucket() + "/";
  }
}
