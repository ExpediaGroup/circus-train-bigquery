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
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryExtractionDataTest {

  private @Mock Table table;

  @Test
  public void objectsEqual() {
    when(table.getTableId()).thenReturn(TableId.of("dataset", "table"));
    BigQueryExtractionData data = new BigQueryExtractionData(table);
    BigQueryExtractionData copy = new BigQueryExtractionData(table);
    assertTrue(data.equals(copy));
  }

  @Test
  public void objectsNotEqualDueToDifferentTableName() {
    when(table.getTableId()).thenReturn(TableId.of("dataset", "table"));
    BigQueryExtractionData data = new BigQueryExtractionData(table);
    Table secondTable = mock(Table.class);
    when(secondTable.getTableId()).thenReturn(TableId.of("dataset", "second-table"));
    BigQueryExtractionData other = new BigQueryExtractionData(secondTable);
    assertFalse(data.equals(other));
  }

  @Test
  public void objectsNotEqualDueToDifferentDatasetName() {
    when(table.getTableId()).thenReturn(TableId.of("dataset", "table"));
    BigQueryExtractionData data = new BigQueryExtractionData(table);
    Table secondTable = mock(Table.class);
    when(secondTable.getTableId()).thenReturn(TableId.of("second-dataset", "table"));
    BigQueryExtractionData other = new BigQueryExtractionData(secondTable);
    assertFalse(data.equals(other));
  }

  @Test
  public void objectsNotEqualDueToDifferentDatasetAndTableNames() {
    when(table.getTableId()).thenReturn(TableId.of("dataset", "table"));
    BigQueryExtractionData data = new BigQueryExtractionData(table);
    Table secondTable = mock(Table.class);
    when(secondTable.getTableId()).thenReturn(TableId.of("second-dataset", "second-table"));
    BigQueryExtractionData other = new BigQueryExtractionData(secondTable);
    assertFalse(data.equals(other));
  }

  @Test
  public void objectsNotEqualDueToNullOther() {
    when(table.getTableId()).thenReturn(TableId.of("dataset", "table"));
    BigQueryExtractionData data = new BigQueryExtractionData(table);
    assertFalse(data.equals(null));
  }

  @Test
  public void hashCodeEqualsTest() {
    when(table.getTableId()).thenReturn(TableId.of("dataset", "table"));
    BigQueryExtractionData dataOne = new BigQueryExtractionData(table);
    BigQueryExtractionData dataTwo = new BigQueryExtractionData(table);

    assertEquals(dataOne.hashCode(), dataTwo.hashCode());
  }

  @Test
  public void hashCodeNotEqualsTestDueToDifferentTableName() {
    when(table.getTableId()).thenReturn(TableId.of("dataset", "table"));
    BigQueryExtractionData dataOne = new BigQueryExtractionData(table);
    Table secondTable = mock(Table.class);
    when(secondTable.getTableId()).thenReturn(TableId.of("dataset", "second-table"));
    BigQueryExtractionData dataTwo = new BigQueryExtractionData(secondTable);
    assertNotEquals(dataOne.hashCode(), dataTwo.hashCode());
  }

  @Test
  public void hashCodeNotEqualsTestDueToDifferentDatasetName() {
    when(table.getTableId()).thenReturn(TableId.of("dataset", "table"));
    BigQueryExtractionData dataOne = new BigQueryExtractionData(table);

    Table secondTable = mock(Table.class);
    when(secondTable.getTableId()).thenReturn(TableId.of("second-dataset", "table"));
    BigQueryExtractionData dataTwo = new BigQueryExtractionData(secondTable);

    assertNotEquals(dataOne.hashCode(), dataTwo.hashCode());
  }

  @Test
  public void hashCodeNotEqualsTestDueToDifferentDatasetAndTableNames() {
    when(table.getTableId()).thenReturn(TableId.of("dataset", "table"));
    BigQueryExtractionData dataOne = new BigQueryExtractionData(table);
    Table secondTable = mock(Table.class);
    when(secondTable.getTableId()).thenReturn(TableId.of("second-dataset", "second-table"));
    BigQueryExtractionData dataTwo = new BigQueryExtractionData(secondTable);
    assertNotEquals(dataOne.hashCode(), dataTwo.hashCode());
  }

}
