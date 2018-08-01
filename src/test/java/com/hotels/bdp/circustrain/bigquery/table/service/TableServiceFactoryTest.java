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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.bigquery.conf.PartitioningConfiguration;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.api.TableService;
import com.hotels.bdp.circustrain.bigquery.partition.PartitionQueryFactory;
import com.hotels.bdp.circustrain.bigquery.table.service.TableServiceFactory;
import com.hotels.bdp.circustrain.bigquery.util.CircusTrainBigQueryMetastore;

@RunWith(MockitoJUnitRunner.class)
public class TableServiceFactoryTest {

  private @Mock
  CircusTrainBigQueryMetastore bigQueryMetastore;
  private @Mock ExtractionService service;
  private @Mock
  PartitionQueryFactory partitionQueryFactory;
  private @Mock
  PartitioningConfiguration configuration;

  private TableServiceFactory tableServiceFactory;

  @Test
  public void newInstanceCachesService() {
    Map<Table, TableService> map = new HashMap<>();
    this.tableServiceFactory = new TableServiceFactory(bigQueryMetastore, service, map, partitionQueryFactory,
        configuration);
    Table table = new Table();
    when(partitionQueryFactory.get(eq(table), anyString(), anyString())).thenReturn("foo > 5");
    tableServiceFactory.newInstance(table);
    assertEquals(1, map.size());
  }

  @Test
  public void newInstanceTwiceResultsInCacheHit() {
    Map<Table, TableService> map = mock(Map.class);
    when(map.containsKey(anyString())).thenReturn(false).thenReturn(true);

    this.tableServiceFactory = new TableServiceFactory(bigQueryMetastore, service, map, partitionQueryFactory,
        configuration);
    Table table = new Table();
    when(partitionQueryFactory.get(eq(table), anyString(), anyString())).thenReturn("foo > 5");
    tableServiceFactory.newInstance(table);
    tableServiceFactory.newInstance(table);
    verify(map, times(1)).put(eq(table), any(TableService.class));
  }
}