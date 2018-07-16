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

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryToHivePartitionConverterTest {

  @Test
  public void withDatabaseName() {
    String dbName = "database";
    Partition partition = new BigQueryToHivePartitionConverter().withDatabaseName(dbName).convert();
    assertEquals(partition.getDbName(), dbName);
  }

  @Test
  public void withpartitionName() {
    String partitionName = "database";
    Partition partition = new BigQueryToHivePartitionConverter().withTableName(partitionName).convert();
    assertEquals(partition.getTableName(), partitionName);
  }

  @Test
  public void withLocation() {
    String location = "getExtractedDataBaseLocation";
    Partition partition = new BigQueryToHivePartitionConverter().withLocation(location).convert();
    assertEquals(partition.getSd().getLocation(), location);
  }

  @Test
  public void withValues() {
    Partition partition = new BigQueryToHivePartitionConverter()
        .withValues(Collections.singletonList("foo=baz"))
        .convert();
    assertEquals(partition.getValues().get(0), "foo=baz");
  }
}
