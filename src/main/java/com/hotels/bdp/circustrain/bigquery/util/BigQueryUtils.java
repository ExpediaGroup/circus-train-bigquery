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
package com.hotels.bdp.circustrain.bigquery.util;

import com.google.cloud.bigquery.BigQuery;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

public class BigQueryUtils {
  public  static com.google.cloud.bigquery.Table getBigQueryTable(BigQuery bigQuery, String databaseName, String tableName)
      throws NoSuchObjectException {
    com.google.cloud.bigquery.Table table = bigQuery.getDataset(databaseName).get(tableName);
    if (table == null) {
      throw new NoSuchObjectException(databaseName + "." + tableName + " could not be found");
    }
    return table;
  }
}
