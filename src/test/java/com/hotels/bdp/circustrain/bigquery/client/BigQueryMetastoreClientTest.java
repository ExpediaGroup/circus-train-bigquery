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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
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

import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.table.service.TableServiceFactory;
import com.hotels.bdp.circustrain.bigquery.table.service.partitioned.PartitionedTableService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;
import com.hotels.bdp.circustrain.bigquery.util.TableNameFactory;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryMetastoreClientTest {

  private final String dbName = "database";
  private final String tblName = "table";

  private @Mock BigQuery bigQuery;
  private @Mock ExtractionService extractionService;
  private @Mock TableServiceFactory factory;
  private @Mock PartitionedTableService partitionedTableService;
  private @Mock HiveTableCache cache;
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
    bigQueryMetastoreClient.tableExists(dbName, tblName);
    verify(bigQueryMetastore).tableExists(dbName, tblName);
  }

  @Test(expected = UnknownDBException.class)
  public void tableExistsWhenDatabaseDoesntExistThrowsExceptionTest() throws TException {
    doThrow(new UnknownDBException()).when(bigQueryMetastore).tableExists(dbName, tblName);
    bigQueryMetastoreClient.tableExists(dbName, tblName);
  }

  @Test
  public void tableExistsReturnsFalseWhenTableDoesntExist() throws TException {
    when(bigQueryMetastore.tableExists(dbName, tblName)).thenReturn(false);
    assertFalse(bigQueryMetastoreClient.tableExists("database", "table"));
  }

  @Test
  public void getTableTestPartitioningNotConfigured() throws TException, InterruptedException {
    Table mockTable = mock(Table.class);
    TableDefinition mockTableDefinition = mock(TableDefinition.class);
    when(mockTable.getDefinition()).thenReturn(mockTableDefinition);
    when(mockTableDefinition.getSchema()).thenReturn(Schema.of());
    when(bigQueryMetastore.getTable(dbName, tblName)).thenReturn(mockTable);

    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(factory.newInstance(any(org.apache.hadoop.hive.metastore.api.Table.class)))
        .thenReturn(partitionedTableService);
    when(partitionedTableService.getTable()).thenReturn(hiveTable);

    bigQueryMetastoreClient.getTable(dbName, tblName);

    verify(cache).contains(dbName, tblName);
    verify(bigQueryMetastore).getTable(dbName, tblName);
    verify(cache).put(any(org.apache.hadoop.hive.metastore.api.Table.class));
    verify(factory).newInstance(any(org.apache.hadoop.hive.metastore.api.Table.class));
    verify(partitionedTableService).getTable();
  }

  @Test
  public void listPartitionsWithTableCachedTest() throws TException {
    String dbName = "db";
    String tblName = "tbl";
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(cache.get(dbName, tblName)).thenReturn(hiveTable);
    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> partitions = new ArrayList<>();
    Partition partition = new Partition();
    partitions.add(partition);
    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(dbName, tblName, (short) 10);
    assertEquals(results.get(0), partition);
    assertEquals(1, results.size());
  }

  @Test
  public void listPartitionsWithTableCachedAndLimitedSizeTest() throws TException {
    String dbName = "db";
    String tblName = "tbl";
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(cache.get(dbName, tblName)).thenReturn(hiveTable);
    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      partitions.add(new Partition());
    }

    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(dbName, tblName, (short) 5);
    assertEquals(5, results.size());
    assertEquals(10, partitions.size());
  }

  @Test
  public void listPartitionsWithoutTableCachedTest() throws TException {
    String dbName = "db";
    String tblName = "tbl";
    String key = TableNameFactory.newInstance(dbName, tblName);
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(cache.get(dbName, tblName)).thenReturn(null).thenReturn(hiveTable);

    Table mockTable = mock(Table.class);
    TableDefinition mockTableDefinition = mock(TableDefinition.class);
    when(mockTable.getDefinition()).thenReturn(mockTableDefinition);
    when(mockTableDefinition.getSchema()).thenReturn(Schema.of());
    when(bigQueryMetastore.getTable(dbName, tblName)).thenReturn(mockTable);

    when(factory.newInstance(any(org.apache.hadoop.hive.metastore.api.Table.class)))
        .thenReturn(partitionedTableService);
    when(partitionedTableService.getTable()).thenReturn(hiveTable);

    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> partitions = new ArrayList<>();
    Partition partition = new Partition();
    partitions.add(partition);
    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(dbName, tblName, (short) 10);
    assertEquals(results.get(0), partition);
    assertEquals(1, results.size());
    verify(cache).get(dbName, tblName);
  }

  @Test
  public void listPartitionsWithoutTableCachedAndPartitionsSizeLimitedTest() throws TException {
    String dbName = "db";
    String tblName = "tbl";
    String key = TableNameFactory.newInstance(dbName, tblName);
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(cache.get(dbName, tblName)).thenReturn(null).thenReturn(hiveTable);

    Table mockTable = mock(Table.class);
    TableDefinition mockTableDefinition = mock(TableDefinition.class);
    when(mockTable.getDefinition()).thenReturn(mockTableDefinition);
    when(mockTableDefinition.getSchema()).thenReturn(Schema.of());
    when(bigQueryMetastore.getTable(dbName, tblName)).thenReturn(mockTable);

    when(factory.newInstance(any(org.apache.hadoop.hive.metastore.api.Table.class)))
        .thenReturn(partitionedTableService);
    when(partitionedTableService.getTable()).thenReturn(hiveTable);

    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      partitions.add(new Partition());
    }

    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(dbName, tblName, (short) 5);
    assertEquals(5, results.size());
    assertEquals(10, partitions.size());
    verify(cache).get(dbName, tblName);
  }

  @Test
  public void listPartitionsWithTableCachedAndPartitionsSizeNegativeReturnsAllPartitionsTest() throws TException {
    String dbName = "db";
    String tblName = "tbl";
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(cache.get(dbName, tblName)).thenReturn(hiveTable);
    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      partitions.add(new Partition());
    }

    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(dbName, tblName, (short) -1);
    assertEquals(10, results.size());
  }

  @Test
  public void listPartitionsWithoutTableCachedAndPartitionsSizeNegativeReturnsAllPartitionsTest() throws TException {
    String dbName = "db";
    String tblName = "tbl";
    String key = TableNameFactory.newInstance(dbName, tblName);
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(cache.get(dbName, tblName)).thenReturn(null).thenReturn(hiveTable);

    Table mockTable = mock(Table.class);
    TableDefinition mockTableDefinition = mock(TableDefinition.class);
    when(mockTable.getDefinition()).thenReturn(mockTableDefinition);
    when(mockTableDefinition.getSchema()).thenReturn(Schema.of());
    when(bigQueryMetastore.getTable(dbName, tblName)).thenReturn(mockTable);

    when(factory.newInstance(any(org.apache.hadoop.hive.metastore.api.Table.class)))
        .thenReturn(partitionedTableService);
    when(partitionedTableService.getTable()).thenReturn(hiveTable);

    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      partitions.add(new Partition());
    }

    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(dbName, tblName, (short) -1);
    assertEquals(10, results.size());
    verify(cache).get(dbName, tblName);
  }
}
