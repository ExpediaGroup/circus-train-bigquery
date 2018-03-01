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
package com.hotels.bdp.circustrain.bigquery.listener;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Table;
import com.google.cloud.storage.Storage;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.event.EventReplicaTable;
import com.hotels.bdp.circustrain.api.event.EventSourceTable;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.bigquery.extraction.BigQueryDataExtractionManager;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryReplicationListenerTest {

  private @Mock EventTableReplication eventTableReplication;
  private @Mock BigQueryDataExtractionManager dataExtractionManager;
  private @Mock BigQuery bigQuery;
  private @Mock Storage storage;

  private BigQueryReplicationListener listener;

  @Before
  public void init() {
    listener = new BigQueryReplicationListener(dataExtractionManager, bigQuery);
    when(eventTableReplication.getSourceTable()).thenReturn(mock(EventSourceTable.class));
    when(eventTableReplication.getReplicaTable()).thenReturn(mock(EventReplicaTable.class));
  }

  @Test
  public void tableReplicationStart() {
    Dataset dataset = mock(Dataset.class);
    when(bigQuery.getDataset(anyString())).thenReturn(dataset);
    Table table = mock(Table.class);
    when(dataset.get(anyString())).thenReturn(table);
    listener.tableReplicationStart(eventTableReplication, "eventId");
    verify(dataExtractionManager).extract(table);
  }

  @Test(expected = CircusTrainException.class)
  public void tableReplicationStartTableMissingThrowsException() {
    Dataset dataset = mock(Dataset.class);
    when(bigQuery.getDataset(anyString())).thenReturn(dataset);
    when(dataset.get(anyString())).thenReturn(null);
    listener.tableReplicationStart(eventTableReplication, "eventId");

  }

  @Test
  public void tableReplicationSuccess() {
    BigQueryReplicationListener listener = new BigQueryReplicationListener(dataExtractionManager, bigQuery);
    Dataset dataset = mock(Dataset.class);
    when(bigQuery.getDataset(anyString())).thenReturn(dataset);
    Table table = mock(Table.class);
    when(dataset.get(anyString())).thenReturn(table);
    listener.tableReplicationSuccess(eventTableReplication, "eventId");
    verify(dataExtractionManager).cleanup(table);
  }

  @Test
  public void tableReplicationFailure() {
    BigQueryReplicationListener listener = new BigQueryReplicationListener(dataExtractionManager, bigQuery);
    Dataset dataset = mock(Dataset.class);
    when(bigQuery.getDataset(anyString())).thenReturn(dataset);
    Table table = mock(Table.class);
    when(dataset.get(anyString())).thenReturn(table);
    listener.tableReplicationFailure(eventTableReplication, "eventId", mock(CircusTrainException.class));
    verify(dataExtractionManager).cleanup(table);
  }
}
