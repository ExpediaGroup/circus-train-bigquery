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

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableResult;

import com.hotels.bdp.circustrain.bigquery.context.CircusTrainBigQueryConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class PartitionedTableServiceTest {

  private CircusTrainBigQueryConfiguration configuration = new CircusTrainBigQueryConfiguration();
  private @Mock HiveParitionKeyAdder adder;
  private @Mock
  HivePartitionService factory;
  private @Mock TableResult result;
  private @Mock com.google.cloud.bigquery.Table filteredTable;

  private PartitionedTableService partitionedTableService;

  @Before
  public void init() {
    partitionedTableService = new PartitionedTableService(configuration, adder, factory, result, filteredTable);
  }

  @Test
  public void getTable() {
    String partitionBy = "foo";
    configuration.setPartitionBy(partitionBy);
    TableDefinition definition = mock(TableDefinition.class);
    when(filteredTable.getDefinition()).thenReturn(definition);
    Schema schema = Schema.of(Field.of(partitionBy, LegacySQLTypeName.STRING));
    when(definition.getSchema()).thenReturn(schema);
    partitionedTableService.getTable();
    Mockito.verify(adder).add(eq(partitionBy), eq(schema));
  }

  @Test
  public void getPartitions() {
    String partitionBy = "foo";
    configuration.setPartitionBy(partitionBy);
    TableDefinition definition = mock(TableDefinition.class);
    when(filteredTable.getDefinition()).thenReturn(definition);
    Schema schema = Schema.of(Field.of(partitionBy, LegacySQLTypeName.STRING));
    when(definition.getSchema()).thenReturn(schema);
    List<FieldValueList> rows = new ArrayList<>();
    when(result.iterateAll()).thenReturn(rows);

    partitionedTableService.getPartitions();

    Mockito.verify(factory).generate(eq(partitionBy), eq(rows));
  }
}
