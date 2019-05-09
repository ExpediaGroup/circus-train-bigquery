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
package com.hotels.bdp.circustrain.bigquery.extraction.container;

import org.apache.hadoop.hive.metastore.api.Table;

import com.google.cloud.storage.Storage;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.client.HiveTableCache;
import com.hotels.bdp.circustrain.bigquery.util.AvroConstants;
import com.hotels.bdp.circustrain.bigquery.util.SchemaExtractor;

public class UpdateTableSchemaAction implements PostExtractionAction {

  private final String databaseName;
  private final String tableName;
  private final HiveTableCache cache;
  private final Storage storage;
  private final ExtractionUri extractionUri;
  private final SchemaExtractor schemaExtractor;

  public UpdateTableSchemaAction(
      String databaseName,
      String tableName,
      HiveTableCache cache,
      Storage storage,
      ExtractionUri extractionUri,
      SchemaExtractor schemaExtractor) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.cache = cache;
    this.storage = storage;
    this.extractionUri = extractionUri;
    this.schemaExtractor = schemaExtractor;
  }

  @Override
  public void run() {
    Table table = cache.get(databaseName, tableName);
    if (table == null) {
      throw new CircusTrainException("Unable to find pre-cached table: " + databaseName + "." + tableName);
    }
    String schema = schemaExtractor.getSchemaFromStorage(storage, extractionUri);
    table.getSd().getSerdeInfo().putToParameters(AvroConstants.SCHEMA_PARAMETER, schema);
  }

}
