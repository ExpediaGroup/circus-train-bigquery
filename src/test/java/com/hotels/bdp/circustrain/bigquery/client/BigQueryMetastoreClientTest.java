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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.bigquery.util.BigQueryKey.makeKey;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;

import com.hotels.bdp.circustrain.bigquery.cache.MetastoreClientCache;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.partition.TableService;
import com.hotels.bdp.circustrain.bigquery.partition.TableServiceFactory;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryMetastoreClientTest {

  private @Mock BigQuery bigQuery;
  private @Mock ExtractionService extractionService;
  private @Mock TableServiceFactory factory;
  private @Mock TableService tableService;
  private @Mock MetastoreClientCache cache;
  private @Mock Job job;
  private @Mock BigQueryMetastore bigQueryMetastore;
  private @Mock JobStatus jobStatus;

  private BigQueryMetastoreClient bigQueryMetastoreClient;

  @Before
  public void init() {
    bigQueryMetastoreClient = new BigQueryMetastoreClient(bigQueryMetastore, extractionService, cache, factory);
  }

  @Test
  public void getDatabaseTest() throws TException {
    when(bigQuery.getDataset(anyString())).thenReturn(mock(Dataset.class));
    Database database = bigQueryMetastoreClient.getDatabase("test");
    assertEquals("test", database.getName());
  }

  @Test(expected = UnknownDBException.class)
  public void getDatabaseWhenDatabaseDoesntExistThrowsExceptionTest() throws TException {
    String dbName = "database";
    doThrow(new UnknownDBException()).when(bigQueryMetastore).checkDbExists(dbName);
    bigQueryMetastoreClient.getDatabase(dbName);
  }

  @Test
  public void tableExistsTest() throws TException {
    Dataset dataset = mock(Dataset.class);
    Table table = mock(Table.class);
    when(bigQuery.getDataset(anyString())).thenReturn(dataset);
    when(dataset.get(anyString())).thenReturn(table);
    String dbName = "database";
    String tblName = "table";
    bigQueryMetastoreClient.tableExists(dbName, tblName);
    verify(bigQueryMetastore).tableExists(dbName, tblName);
  }

  @Test(expected = UnknownDBException.class)
  public void tableExistsWhenDatabaseDoesntExistThrowsExceptionTest() throws TException {
    String dbName = "database";
    String tblName = "table";
    doThrow(new UnknownDBException()).when(bigQueryMetastore).tableExists(dbName, tblName);
    bigQueryMetastoreClient.tableExists(dbName, tblName);
  }

  @Test
  public void tableExistsReturnsFalseWhenTableDoesntExist() throws TException {
    String dbName = "database";
    String tblName = "table";
    when(bigQueryMetastore.tableExists(dbName, tblName)).thenReturn(false);
    assertFalse(bigQueryMetastoreClient.tableExists("database", "table"));
  }

  @Test
  public void getTableTestPartitioningNotConfigured() throws TException, InterruptedException {
    String dbName = "database";
    String tableName = "table";

    Table mockTable = mock(Table.class);
    TableDefinition mockTableDefinition = mock(TableDefinition.class);
    when(mockTable.getDefinition()).thenReturn(mockTableDefinition);
    when(mockTableDefinition.getSchema()).thenReturn(Schema.of());
    when(bigQueryMetastore.getTable(dbName, tableName)).thenReturn(mockTable);

    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(factory.newInstance(any(org.apache.hadoop.hive.metastore.api.Table.class))).thenReturn(tableService);
    when(tableService.getTable()).thenReturn(hiveTable);

    bigQueryMetastoreClient.getTable(dbName, tableName);

    verify(cache).containsTable(makeKey(dbName, tableName));
    verify(bigQueryMetastore).getTable(dbName, tableName);
    verify(cache).cacheTable(any(org.apache.hadoop.hive.metastore.api.Table.class));
    verify(factory).newInstance(any(org.apache.hadoop.hive.metastore.api.Table.class));
    verify(tableService).getTable();

  }

}
