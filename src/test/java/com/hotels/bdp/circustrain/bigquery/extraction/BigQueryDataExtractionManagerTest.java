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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static junit.framework.TestCase.assertTrue;

import java.util.HashSet;
import java.util.Set;

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
    when(table.getTableId()).thenReturn(TableId.of("dataset", "table"));
  }

  @Test
  public void extractRegisteredTest() {
    Table one = mock(Table.class);
    when(one.getTableId()).thenReturn(TableId.of("datasetOne", "tableOne"));

    Table two = mock(Table.class);
    when(two.getTableId()).thenReturn(TableId.of("datasetTwo", "tableTwo"));

    Table three = mock(Table.class);
    when(three.getTableId()).thenReturn(TableId.of("datasetThree", "tableThree"));

    dataExtractionManager.register(one, two, three);
    dataExtractionManager.extract();

    verify(service, times(3)).extract(any(BigQueryExtractionData.class));
  }

  @Test
  public void cleanupRegisteredTest() {
    Table one = mock(Table.class);
    when(one.getTableId()).thenReturn(TableId.of("datasetOne", "tableOne"));

    Table two = mock(Table.class);
    when(two.getTableId()).thenReturn(TableId.of("datasetTwo", "tableTwo"));

    Table three = mock(Table.class);
    when(three.getTableId()).thenReturn(TableId.of("datasetThree", "tableThree"));

    dataExtractionManager.register(one, two, three);
    dataExtractionManager.extract();
    dataExtractionManager.cleanup();

    verify(service, times(3)).cleanup(any(BigQueryExtractionData.class));
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

  @Test
  public void extractingAlreadyExtractedTableDoesntExecute() {
    when(service.extract(any(BigQueryExtractionData.class))).thenReturn(true, false);
    assertTrue(dataExtractionManager.extract(table));
    assertFalse(dataExtractionManager.extract(table));
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

  @Test
  public void cleanupTableWhichHasntBeenExtractedDoesntExecute() {
    dataExtractionManager.cleanup(table);
    verifyZeroInteractions(service);
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
  public void locationForTableThatHasntBeenExtractedCachesAndReturnsLocation() {
    assertNotNull(dataExtractionManager.location(table));
  }

  @Test
  public void tableWrapperHashCodeTest() {
    BigQueryDataExtractionManager.TableWrapper wrapperOne = new BigQueryDataExtractionManager.TableWrapper(table);
    BigQueryDataExtractionManager.TableWrapper wrapperTwo = new BigQueryDataExtractionManager.TableWrapper(table);

    assertEquals(wrapperOne.hashCode(), wrapperTwo.hashCode());
  }

  @Test
  public void tableWrapperEqualsTest() {
    BigQueryDataExtractionManager.TableWrapper wrapperOne = new BigQueryDataExtractionManager.TableWrapper(table);
    BigQueryDataExtractionManager.TableWrapper wrapperTwo = new BigQueryDataExtractionManager.TableWrapper(table);
    assertEquals(wrapperOne, wrapperTwo);
  }

  @Test
  public void tableWrapperNotEqualsTest() {
    BigQueryDataExtractionManager.TableWrapper wrapperOne = new BigQueryDataExtractionManager.TableWrapper(table);
    Table mock = mock(Table.class);
    when(mock.getTableId()).thenReturn(TableId.of("dataset2", "table2"));
    BigQueryDataExtractionManager.TableWrapper wrapperTwo = new BigQueryDataExtractionManager.TableWrapper(mock);
    assertNotEquals(wrapperOne, wrapperTwo);
  }

  @Test
  public void tableWrapperInSetTest() {
    BigQueryDataExtractionManager.TableWrapper wrapper = new BigQueryDataExtractionManager.TableWrapper(table);
    Set<BigQueryDataExtractionManager.TableWrapper> set = new HashSet<>();
    set.add(wrapper);
    set.add(wrapper);
    assertEquals(1, set.size());
    Table mock = mock(Table.class);
    when(mock.getTableId()).thenReturn(TableId.of("dataset2", "table2"));
    BigQueryDataExtractionManager.TableWrapper wrapperTwo = new BigQueryDataExtractionManager.TableWrapper(mock);
    set.add(wrapperTwo);
    assertEquals(2, set.size());

  }
}
