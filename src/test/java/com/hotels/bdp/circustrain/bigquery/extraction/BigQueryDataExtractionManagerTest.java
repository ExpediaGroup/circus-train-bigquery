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
package com.hotels.bdp.circustrain.bigquery.extraction;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryDataExtractionManagerTest {

  private @Mock BigQueryDataExtractionService service;
  private List<Table> tables = new ArrayList<>();
  private Map<Table, Pair<BigQueryExtractionData, Boolean>> map = new HashMap<>();
  private BigQueryDataExtractionManager manager;

  @Before
  public void init() {
    manager = new BigQueryDataExtractionManager(service, map);
    for (int i = 0; i < 10; i++) {
      Table table = mock(Table.class);
      when(table.getTableId()).thenReturn(TableId.of("dataset" + i, "table" + i));
      tables.add(table);
    }
  }

  private void registerTables() {
    for (Table table : tables) {
      manager.register(table);
    }
  }

  @Test
  public void registrationTest() {
    registerTables();
    assertThat(map.size(), is(tables.size()));
  }

  @Test
  public void extractRegisteredTest() {
    registerTables();
    manager.extractAll();
    for (Table table : tables) {
      verify(service).extract(eq(table), eq(map.get(table).getKey()));
    }
  }

  @Test
  public void cleanupRegisteredTest() {
    registerTables();
    manager.extractAll();
    manager.cleanupAll();
    for (Table table : tables) {
      verify(service).cleanup(eq(map.get(table).getKey()));
    }
  }

  @Test
  public void extractEmptyRegistryTest() {
    manager.extractAll();
    verifyZeroInteractions(service);
  }

  @Test
  public void cleanupEmptyRegistryTest() {
    manager.cleanupAll();
    verifyZeroInteractions(service);
  }

  @Test
  public void locationTest() {
    Table table = tables.get(0);
    manager.register(table);
    String location = "gs://" + map.get(table).getKey().getBucket() + "/";
    assertThat(location, is(manager.getExtractedDataBaseLocation(table)));
  }

  @Test
  public void locationForUnregisteredTableTest() {
    Table table = tables.get(0);
    assertNull(manager.getExtractedDataBaseLocation(table));
  }

}
