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
package com.hotels.bdp.circustrain.bigquery.table.service.partitioned;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableResult;

import com.hotels.bdp.circustrain.bigquery.partition.HivePartitionGenerator;
import com.hotels.bdp.circustrain.bigquery.partition.HivePartitionKeyAdder;

@RunWith(MockitoJUnitRunner.class)
public class PartitionedTableServiceTest {

  private @Mock HivePartitionKeyAdder adder;
  private @Mock HivePartitionGenerator partitionGenerator;
  private @Mock TableResult tableResult;
  private @Mock TableDefinition definition;
  private @Mock com.google.cloud.bigquery.Table filteredTable;

  private final String type = "STRING";
  private final String partitionBy = "foo";
  private final Schema schema = Schema.of(Field.of(partitionBy, LegacySQLTypeName.STRING));
  private final List<FieldValueList> rows = new ArrayList<>();
  private final List<Partition> partition = Arrays.asList(new Partition());
  private final Table table = new Table();
  private PartitionedTableService partitionedTableService;

  @Before
  public void init() {
    when(filteredTable.getDefinition()).thenReturn(definition);
    when(definition.getSchema()).thenReturn(schema);
    when(adder.add(partitionBy, schema)).thenReturn(table);

    partitionedTableService = new PartitionedTableService(partitionBy, adder, partitionGenerator, tableResult,
        filteredTable);
  }

  @Test
  public void getTable() {
    Table result = partitionedTableService.getTable();
    assertThat(result, is(table));
  }

  @Test
  public void getPartitions() {
    when(tableResult.iterateAll()).thenReturn(rows);
    when(partitionGenerator.generate(eq(partitionBy), eq(type), eq(rows))).thenReturn(partition);
    List<Partition> result = partitionedTableService.getPartitions();
    assertThat(result, is(partition));
  }

  @Test(expected = IllegalStateException.class)
  public void getPartitionFieldsWithNoSchema() {
    Schema emptySchema = null;
    when(definition.getSchema()).thenReturn(emptySchema);
    partitionedTableService.getPartitions();
  }

}
