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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryDataExtractionManagerTest {

  private @Mock BigQueryDataExtractionService service;
  private @Mock Table table;
  private BigQueryDataExtractionManager dataExtractionManager;

  @Before
  public void init() {
    dataExtractionManager = new BigQueryDataExtractionManager(service);
    dataExtractionManager.register(table);
    when(table.getTableId()).thenReturn(TableId.of("dataset", "table"));
  }

  @Test
  public void extractRegisteredTest() {
    Table table = mock(Table.class);
    when(table.getTableId()).thenReturn(TableId.of("datasetOne", "tableOne"));
    dataExtractionManager.extract();
    verify(service).extract(any(Table.class), any(BigQueryExtractionData.class));
  }

  @Test
  public void cleanupRegisteredTest() {
    Table table = mock(Table.class);
    when(table.getTableId()).thenReturn(TableId.of("datasetOne", "tableOne"));

    dataExtractionManager.register(table);
    dataExtractionManager.extract();
    dataExtractionManager.cleanup();

    verify(service).cleanup(any(BigQueryExtractionData.class));
  }

  @Test
  public void extractTest() {
    dataExtractionManager.extract();
    verify(service).extract(eq(table), any(BigQueryExtractionData.class));
  }

  @Test
  public void cleanupTest() {
    dataExtractionManager.cleanup();
    verify(service).cleanup(any(BigQueryExtractionData.class));
  }

  @Test
  public void locationTest() {
    dataExtractionManager.extract();
    ArgumentCaptor<BigQueryExtractionData> captor = ArgumentCaptor.forClass(BigQueryExtractionData.class);
    verify(service).extract(any(Table.class), captor.capture());
    BigQueryExtractionData data = captor.getValue();
    assertEquals("gs://" + data.getDataBucket() + "/", dataExtractionManager.location());
  }

  @Test
  public void locationForTableThatHasntBeenExtractedCachesAndReturnsLocation() {
    assertNotNull(dataExtractionManager.location());
  }
}
