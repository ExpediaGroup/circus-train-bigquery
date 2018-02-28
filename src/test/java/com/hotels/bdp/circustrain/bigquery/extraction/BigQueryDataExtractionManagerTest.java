/**
 * Copyright (C) 2015-2018 Expedia Inc.
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
import static org.junit.Assert.assertNull;
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

import com.hotels.bdp.circustrain.api.CircusTrainException;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryDataExtractionManagerTest {

  private @Mock BigQueryDataExtractionService service;
  private @Mock Table table;
  private BigQueryDataExtractionManager dataExtractionManager;

  @Before
  public void init() {
    dataExtractionManager = new BigQueryDataExtractionManager(service);
    when(table.getTableId()).thenReturn(TableId.of("dataset", "table"));
  }

  @Test
  public void extractTest() {
    dataExtractionManager.extract(table);
    ArgumentCaptor<BigQueryExtractionData> captor = ArgumentCaptor.forClass(BigQueryExtractionData.class);
    verify(service).extract(captor.capture());
    BigQueryExtractionData data = captor.getValue();
    assertEquals("dataset", data.getDatasetName());
    assertEquals("table", data.getTableName());
  }

  @Test(expected = CircusTrainException.class)
  public void extractingAlreadyExtractedTableThrowsExceptionTest() {
    dataExtractionManager.extract(table);
    dataExtractionManager.extract(table);
  }

  @Test
  public void cleanupTest() {
    dataExtractionManager.extract(table);
    dataExtractionManager.cleanup(table);
    ArgumentCaptor<BigQueryExtractionData> captor = ArgumentCaptor.forClass(BigQueryExtractionData.class);
    verify(service).cleanup(captor.capture());
    BigQueryExtractionData data = captor.getValue();
    assertEquals("dataset", data.getDatasetName());
    assertEquals("table", data.getTableName());
  }

  @Test(expected = CircusTrainException.class)
  public void cleanupTableWhichHasntBeenExtractedThrowsExceptionTest() {
    dataExtractionManager.cleanup(table);
  }

  @Test
  public void locationTest() {
    dataExtractionManager.extract(table);
    ArgumentCaptor<BigQueryExtractionData> captor = ArgumentCaptor.forClass(BigQueryExtractionData.class);
    verify(service).extract(captor.capture());
    BigQueryExtractionData data = captor.getValue();
    assertEquals("gs://" + data.getDataBucket() + "/", dataExtractionManager.location(table));
  }

  @Test
  public void locationForTableThatHasntBeenExtractedIsNull() {
    assertNull(dataExtractionManager.location(table));
  }
}
