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
package com.hotels.bdp.circustrain.bigquery.table.service;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.bigquery.api.TableService;
import com.hotels.bdp.circustrain.bigquery.conf.PartitioningConfiguration;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.partition.PartitionQueryFactory;
import com.hotels.bdp.circustrain.bigquery.table.service.partitioned.PartitionedTableService;
import com.hotels.bdp.circustrain.bigquery.table.service.unpartitioned.UnpartitionedTableService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;
import com.hotels.bdp.circustrain.bigquery.util.SchemaExtractor;

@RunWith(MockitoJUnitRunner.class)
public class TableServiceFactoryTest {

  private @Mock BigQueryMetastore bigQueryMetastore;
  private @Mock ExtractionService service;
  private @Mock PartitioningConfiguration configuration;
  private @Mock SchemaExtractor schemaExtractor;

  private TableServiceFactory tableServiceFactory;
  private final PartitionQueryFactory partitionQueryFactory = new PartitionQueryFactory();
  private final Table table = new Table();
  private final Map<Table, TableService> map = new HashMap<>();

  @Before
  public void setUp() {
    tableServiceFactory = new TableServiceFactory(bigQueryMetastore, service, map, partitionQueryFactory, configuration,
        schemaExtractor);
    table.setDbName("database");
    table.setTableName("table");
    when(configuration.getPartitionByFor(eq(table))).thenReturn("column");
  }

  @Test
  public void newInstanceCachesServiceNoPartition() {
    TableService tableService = tableServiceFactory.newInstance(table);
    assertThat(map.size(), is(1));
    assertThat(tableService, instanceOf(UnpartitionedTableService.class));
  }

  @Test
  public void newInstanceTwiceResultsInCacheHit() {
    Map<Table, TableService> map = mock(Map.class);
    when(map.containsKey(eq(table))).thenReturn(false).thenReturn(true);

    tableServiceFactory = new TableServiceFactory(bigQueryMetastore, service, map, partitionQueryFactory, configuration,
        schemaExtractor);
    tableServiceFactory.newInstance(table);
    tableServiceFactory.newInstance(table);
    verify(map).put(eq(table), any(TableService.class));
    verify(map).get(table);
  }

  @Test
  public void newInstanceWithPartitionNoFilter() {
    when(configuration.isPartitioningConfigured(eq(table))).thenReturn(true);
    when(configuration.getPartitionFilterFor(eq(table))).thenReturn("");
    TableService tableService = tableServiceFactory.newInstance(table);
    assertThat(tableService, instanceOf(PartitionedTableService.class));
  }

  @Test
  public void newInstanceWithPartitionAndFilter() {
    when(configuration.isPartitioningConfigured(eq(table))).thenReturn(true);
    when(configuration.getPartitionFilterFor(eq(table))).thenReturn("column > 5");
    TableService tableService = tableServiceFactory.newInstance(table);
    assertThat(tableService, instanceOf(PartitionedTableService.class));
  }

}
