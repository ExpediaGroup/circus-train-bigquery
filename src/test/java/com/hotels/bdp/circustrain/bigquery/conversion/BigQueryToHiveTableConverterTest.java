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
package com.hotels.bdp.circustrain.bigquery.conversion;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

public class BigQueryToHiveTableConverterTest {

  @Test
  public void withDatabaseName() {
    String dbName = "database";
    Table table = new BigQueryToHiveTableConverter().withDatabaseName(dbName).convert();
    assertEquals(table.getDbName(), dbName);
  }

  @Test
  public void withTableName() {
    String tableName = "database";
    Table table = new BigQueryToHiveTableConverter().withTableName(tableName).convert();
    assertEquals(table.getTableName(), tableName);
  }

  @Test
  public void withLocation() {
    String location = "getExtractedDataBaseLocation";
    Table table = new BigQueryToHiveTableConverter().withLocation(location).convert();
    assertEquals(table.getSd().getLocation(), location);
  }

}
