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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Test;

public class HivePartitionFactoryTest {

  @Test
  public void get() {
    String dbName = "db";
    String tblName = "tbl";
    String location = "file:///foo/bar";
    List<FieldSchema> cols = new ArrayList<>();
    String[] partitionValues = { "val1", "val2" };
    Partition result = new HivePartitionFactory(dbName, tblName, location, cols, partitionValues).get();
    assertEquals(dbName, result.getDbName());
    assertEquals(tblName, result.getTableName());
    assertEquals(location, result.getSd().getLocation());
    assertEquals(cols, result.getSd().getCols());
    assertTrue(result.getValues().contains(partitionValues[0]));
    assertTrue(result.getValues().contains(partitionValues[1]));
    assertEquals(partitionValues.length, result.getValuesSize());
  }
}
