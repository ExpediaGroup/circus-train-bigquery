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
package com.hotels.bdp.circustrain.bigquery.partition;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;

public class PartitionQueryFactoryTest {

  private final Table table = new Table();
  private final String partitionKey = "foo";
  private final String databaseName = "db";
  private final String tableName = "tbl";

  @Before
  public void setUp() {
    table.setDbName(databaseName);
    table.setTableName(tableName);
  }

  @Test(expected = IllegalStateException.class)
  public void notConfiguredThrowsException() {
    new PartitionQueryFactory().newInstance(new Table(), null, null);
  }

  @Test(expected = IllegalStateException.class)
  public void partitionFilterOnlyConfiguredThrowsException() {
    new PartitionQueryFactory().newInstance(new Table(), null, "foo > 5");
  }

  @Test
  public void configurePartitionByOnly() {
    String expected = String
        .format("select distinct(%s) from %s.%s order by %s", partitionKey, databaseName, tableName, partitionKey);
    String query = new PartitionQueryFactory().newInstance(table, partitionKey, null);
    assertThat(query, is(expected));
  }

  @Test
  public void configurePartitionByAndPartitionFilter() {
    String partitionFilter = "foo > 5";
    String expected = String
        .format("select distinct(%s) from %s.%s where %s order by %s", partitionKey, databaseName, tableName,
            partitionFilter, partitionKey);
    String query = new PartitionQueryFactory().newInstance(table, partitionKey, partitionFilter);
    assertThat(query, is(expected));
  }
}
