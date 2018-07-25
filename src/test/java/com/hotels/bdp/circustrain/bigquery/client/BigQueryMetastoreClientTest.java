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
package com.hotels.bdp.circustrain.bigquery.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;

import com.hotels.bdp.circustrain.bigquery.cache.MetastoreClientCache;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryMetastoreClientTest {

  private @Mock BigQuery bigQuery;
  private @Mock ExtractionService extractionService;
  private @Mock Job job;
  private @Mock JobStatus jobStatus;

  private BigQueryMetastoreClient bigQueryMetastoreClient;

  @Before
  public void init() {
    bigQueryMetastoreClient = new BigQueryMetastoreClient(null, new BigQueryMetastore(bigQuery), extractionService,
        new MetastoreClientCache());
  }

  @Test
  public void getDatabaseTest() throws TException {
    when(bigQuery.getDataset(anyString())).thenReturn(mock(Dataset.class));
    Database database = bigQueryMetastoreClient.getDatabase("test");
    assertEquals("test", database.getName());
  }

  @Test(expected = UnknownDBException.class)
  public void getDatabaseWhenDatabaseDoesntExistThrowsExceptionTest() throws TException {
    when(bigQuery.getDataset(anyString())).thenReturn(null);
    bigQueryMetastoreClient.getDatabase("test");
  }

  @Test
  public void tableExistsTest() throws TException {
    Dataset dataset = mock(Dataset.class);
    Table table = mock(Table.class);
    when(bigQuery.getDataset(anyString())).thenReturn(dataset);
    when(dataset.get(anyString())).thenReturn(table);
    bigQueryMetastoreClient.tableExists("database", "table");
    verify(bigQuery, times(2)).getDataset(anyString());
    verify(dataset).get(anyString());
  }

  @Test(expected = UnknownDBException.class)
  public void tableExistsWhenDatabaseDoesntExistThrowsExceptionTest() throws TException {
    when(bigQuery.getDataset(anyString())).thenReturn(null);
    bigQueryMetastoreClient.tableExists("database", "table");
  }

  @Test
  public void tableExistsReturnsFalseWhenTableDoesntExist() throws TException {
    Dataset dataset = mock(Dataset.class);
    when(bigQuery.getDataset(anyString())).thenReturn(dataset);
    when(dataset.get(anyString())).thenReturn(null);
    assertFalse(bigQueryMetastoreClient.tableExists("database", "table"));
  }

  @Test
  public void getTableTest() throws TException, InterruptedException {
    Dataset dataset = mock(Dataset.class);
    Schema schema = Schema.of(Field.of("id", LegacySQLTypeName.INTEGER), Field.of("name", LegacySQLTypeName.STRING));
    Table table = mock(Table.class);
    String dbName = "database";
    String tableName = "table";
    TableId tableId = TableId.of(dbName, tableName);
    String location = "gs://foo/baz";

    ExtractionContainer container = mock(ExtractionContainer.class);
    when(extractionService.retrieve(table)).thenReturn(container);
    when(bigQuery.getDataset(anyString())).thenReturn(dataset);
    when(dataset.get(anyString())).thenReturn(table);
    TableDefinition tableDefinition = mock(TableDefinition.class);
    when(table.getDefinition()).thenReturn(tableDefinition);
    when(table.getTableId()).thenReturn(tableId);
    when(tableDefinition.getSchema()).thenReturn(schema);
    when(table.extract(anyString(), anyString())).thenReturn(job);
    when(job.waitFor(Matchers.<RetryOption> anyVararg())).thenReturn(job);
    when(table.getTableId()).thenReturn(tableId);
    when(job.getStatus()).thenReturn(jobStatus);
    when(jobStatus.getError()).thenReturn(null);

    org.apache.hadoop.hive.metastore.api.Table hiveTable = bigQueryMetastoreClient.getTable("database", "table");

    assertEquals(dbName, hiveTable.getDbName());
    assertEquals(tableName, hiveTable.getTableName());
    List<FieldSchema> fields = hiveTable.getSd().getCols();
    assertEquals("id", fields.get(0).getName());
    assertEquals("bigint", fields.get(0).getType());
    assertEquals("name", fields.get(1).getName());
    assertEquals("string", fields.get(1).getType());
    assertNotNull(location, hiveTable.getSd().getLocation());
  }

}
