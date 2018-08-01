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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.hotels.bdp.circustrain.core.conf.SpringExpressionParser;

@RunWith(MockitoJUnitRunner.class)
public class PartitionQueryFactoryTest {

  private @Mock SpringExpressionParser expressionParser;

  @Before
  public void init() {
    when(expressionParser.parse(anyString())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        return (String) args[0];
      }
    });
  }

  @Test(expected = IllegalStateException.class)
  public void notConfiguredThrowsException() {
    new PartitionQueryFactory(expressionParser).get(new Table(), null, null);
  }

  @Test(expected = IllegalStateException.class)
  public void partitionFilterOnlyConfiguredThrowsException() {
    new PartitionQueryFactory(expressionParser).get(new Table(), null, "foo > 5");
  }

  @Test
  public void configurePartitionByOnly() {
    String partitionKey = "foo";
    Table table = new Table();
    String dbName = "db";
    String tblName = "tbl";
    table.setDbName(dbName);
    table.setTableName(tblName);
    String expected = String.format("select %s from %s.%s group by %s order by %s", partitionKey, dbName, tblName,
        partitionKey, partitionKey);
    assertEquals(expected, new PartitionQueryFactory(expressionParser).get(table, partitionKey, null));
  }

  @Test
  public void configurePartitionByAndPartitionFilter() {
    String partitionKey = "foo";
    String partitionFilter = "foo > 5";
    Table table = new Table();
    String dbName = "db";
    String tblName = "tbl";
    table.setDbName(dbName);
    table.setTableName(tblName);
    String expected = String.format("select * from %s.%s where %s", dbName, tblName, partitionFilter);
    assertEquals(expected, new PartitionQueryFactory(expressionParser).get(table, partitionKey, partitionFilter));
  }
}
