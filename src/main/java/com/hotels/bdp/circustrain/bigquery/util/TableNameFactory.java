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

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public final class TableNameFactory {

  private TableNameFactory() {}

  public static String newInstance(String databaseName, String tableName) {
    return databaseName.trim().toLowerCase() + "." + tableName.trim().toLowerCase();
  }

  public static String newInstance(Table table) {
    return newInstance(table.getDbName(), table.getTableName());
  }

  public static String newInstance(Partition partition) {
    return newInstance(partition.getDbName(), partition.getTableName());
  }

  public static String newInstance(com.google.cloud.bigquery.Table table) {
    return newInstance(table.getTableId().getDataset(), table.getTableId().getDataset());
  }
}
