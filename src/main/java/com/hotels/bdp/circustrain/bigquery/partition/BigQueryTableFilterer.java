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

import com.google.cloud.bigquery.TableResult;

import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.extraction.container.PostExtractionAction;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;

public class BigQueryTableFilterer {

  private final BigQueryMetastore bigQueryMetastore;
  private final ExtractionService service;
  private final String databaseName;
  private final String tableName;
  private final String filterQuery;

  public BigQueryTableFilterer(
      BigQueryMetastore bigQueryMetastore,
      ExtractionService service,
      String databaseName,
      String tableName,
      String filterQuery) {
    this.bigQueryMetastore = bigQueryMetastore;
    this.service = service;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.filterQuery = filterQuery;
  }

  public TableResult filterTable() {
    return bigQueryMetastore.executeIntoDestinationTable(databaseName, tableName, filterQuery);
  }

  public com.google.cloud.bigquery.Table getFilteredTable() {
    com.google.cloud.bigquery.Table filteredTable = bigQueryMetastore.getTable(databaseName, tableName);

    ExtractionUri extractionUri = new ExtractionUri();
    ExtractionContainer container = new ExtractionContainer(filteredTable, extractionUri, PostExtractionAction.DELETE);
    service.register(container);
    return filteredTable;
  }
}
