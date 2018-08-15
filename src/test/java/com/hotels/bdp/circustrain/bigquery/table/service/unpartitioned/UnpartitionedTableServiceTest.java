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
package com.hotels.bdp.circustrain.bigquery.table.service.unpartitioned;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import com.hotels.bdp.circustrain.bigquery.conversion.BigQueryToHiveTableConverter;

public class UnpartitionedTableServiceTest {

  private Table table = new BigQueryToHiveTableConverter().withTableName("foo").withDatabaseName("bar").convert();
  private UnpartitionedTableService service = new UnpartitionedTableService(table);

  @Test
  public void getTable() {
    UnpartitionedTableService service = new UnpartitionedTableService(table);
    Table returned = service.getTable();
    assertEquals(table, returned);
    assertFalse(table == returned);
  }

  @Test
  public void getPartitions() {
    assertTrue(service.getPartitions().isEmpty());
  }
}
