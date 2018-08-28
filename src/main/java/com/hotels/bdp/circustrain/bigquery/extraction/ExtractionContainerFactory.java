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

import org.apache.hadoop.hive.metastore.api.Table;

import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;

public class ExtractionContainerFactory {

  private final ExtractionService service;
  private final BigQueryMetastore bigQueryMetastore;
  private final Table hiveTable;

  public ExtractionContainerFactory(
      ExtractionService service,
      BigQueryMetastore bigQueryMetastore,
      Table hiveTable) {
    this.service = service;
    this.bigQueryMetastore = bigQueryMetastore;
    this.hiveTable = hiveTable;
  }

  public ExtractionContainer newInstance() {
    com.google.cloud.bigquery.Table bigQueryRepresentation = bigQueryMetastore.getTable(hiveTable.getDbName(),
        hiveTable.getTableName());

    ExtractionContainer container = service.retrieve(bigQueryRepresentation);
    return container;
  }
}
