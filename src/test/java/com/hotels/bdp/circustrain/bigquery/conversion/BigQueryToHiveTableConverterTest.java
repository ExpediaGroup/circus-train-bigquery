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
package com.hotels.bdp.circustrain.bigquery.conversion;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import com.hotels.bdp.circustrain.bigquery.util.SchemaUtils;

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

  @Test
  public void withSchema() throws IOException {
    String schema = SchemaUtils.getTestSchema();
    Table table = new BigQueryToHiveTableConverter().withSchema(schema).convert();
    assertThat(table.getSd().getSerdeInfo().getParameters().get("avro.schema.literal"), is(schema));
  }

}
