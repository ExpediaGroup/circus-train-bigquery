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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;

import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.table.service.TableServiceFactory;
import com.hotels.bdp.circustrain.bigquery.table.service.partitioned.PartitionedTableService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;
import com.hotels.bdp.circustrain.bigquery.util.SchemaUtils;
import com.hotels.bdp.circustrain.bigquery.util.TableNameFactory;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryMetastoreClientTest {

  private final String databaseName = "database";
  private final String tableName = "table";

  private @Mock BigQuery bigQuery;
  private @Mock ExtractionService extractionService;
  private @Mock TableServiceFactory factory;
  private @Mock PartitionedTableService partitionedTableService;
  private @Mock HiveTableCache cache;
  private @Mock Job job;
  private @Mock BigQueryMetastore bigQueryMetastore;
  private @Mock JobStatus jobStatus;
  private @Mock Dataset dataset;
  private @Mock Table table;
  private @Mock TableDefinition tableDefinition;
  private @Mock Storage storage;
  private @Mock Blob blob;
  private @Mock Page<Blob> blobs;

  String schema;
  private BigQueryMetastoreClient bigQueryMetastoreClient;

  @Before
  public void init() throws IOException {
    setUpSchema();
    bigQueryMetastoreClient = new BigQueryMetastoreClient(bigQueryMetastore, extractionService, cache, factory);
  }

  @Test
  public void getDatabase() throws TException {
    when(bigQuery.getDataset(anyString())).thenReturn(dataset);
    Database database = bigQueryMetastoreClient.getDatabase(databaseName);
    assertEquals(databaseName, database.getName());
  }

  @Test(expected = UnknownDBException.class)
  public void getDatabaseWhenDatabaseDoesntExistThrowsException() throws TException {
    doThrow(UnknownDBException.class).when(bigQueryMetastore).checkDbExists(databaseName);
    bigQueryMetastoreClient.getDatabase(databaseName);
  }

  @Test
  public void tableExists() throws TException {
    when(bigQuery.getDataset(anyString())).thenReturn(dataset);
    when(dataset.get(anyString())).thenReturn(table);
    bigQueryMetastoreClient.tableExists(databaseName, tableName);
    verify(bigQueryMetastore).tableExists(databaseName, tableName);
  }

  @Test(expected = UnknownDBException.class)
  public void tableExistsWhenDatabaseDoesntExistThrowsException() throws TException {
    when(bigQueryMetastore.tableExists(databaseName, tableName)).thenThrow(new UnknownDBException());
    bigQueryMetastoreClient.tableExists(databaseName, tableName);
  }

  @Test
  public void tableExistsReturnsFalseWhenTableDoesntExist() throws TException {
    when(bigQueryMetastore.tableExists(databaseName, tableName)).thenReturn(false);
    assertFalse(bigQueryMetastoreClient.tableExists("database", "table"));
  }

  @Test
  public void getTablePartitioningNotConfigured() throws TException, InterruptedException {
    when(table.getDefinition()).thenReturn(tableDefinition);
    when(tableDefinition.getSchema()).thenReturn(Schema.of());
    when(bigQueryMetastore.getTable(databaseName, tableName)).thenReturn(table);

    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(factory.newInstance(any(org.apache.hadoop.hive.metastore.api.Table.class)))
        .thenReturn(partitionedTableService);
    when(partitionedTableService.getTable()).thenReturn(hiveTable);

    org.apache.hadoop.hive.metastore.api.Table result = bigQueryMetastoreClient.getTable(databaseName, tableName);

    verify(cache).contains(databaseName, tableName);
    verify(bigQueryMetastore).getTable(databaseName, tableName);
    verify(cache).put(any(org.apache.hadoop.hive.metastore.api.Table.class));
    verify(factory).newInstance(any(org.apache.hadoop.hive.metastore.api.Table.class));
    verify(partitionedTableService).getTable();
    assertThat(result, is(hiveTable));
  }

  @Test
  public void listPartitionsWithTableCached() throws TException {
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(cache.get(databaseName, tableName)).thenReturn(hiveTable);
    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> partitions = new ArrayList<>();
    Partition partition = new Partition();
    partitions.add(partition);
    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(databaseName, tableName, (short) 10);
    assertEquals(results.get(0), partition);
    assertEquals(1, results.size());
  }

  @Test
  public void listPartitionsWithTableCachedAndLimitedSize() throws TException {
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(cache.get(databaseName, tableName)).thenReturn(hiveTable);
    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      partitions.add(new Partition());
    }

    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(databaseName, tableName, (short) 5);
    assertEquals(5, results.size());
    assertEquals(10, partitions.size());
  }

  @Test
  public void listPartitionsWithoutTableCached() throws TException {
    String key = TableNameFactory.newInstance(databaseName, tableName);
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(cache.get(databaseName, tableName)).thenReturn(null).thenReturn(hiveTable);

    TableDefinition mockTableDefinition = mock(TableDefinition.class);
    when(table.getDefinition()).thenReturn(mockTableDefinition);
    when(mockTableDefinition.getSchema()).thenReturn(Schema.of());
    when(bigQueryMetastore.getTable(databaseName, tableName)).thenReturn(table);

    when(factory.newInstance(any(org.apache.hadoop.hive.metastore.api.Table.class)))
        .thenReturn(partitionedTableService);
    when(partitionedTableService.getTable()).thenReturn(hiveTable);

    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> partitions = new ArrayList<>();
    Partition partition = new Partition();
    partitions.add(partition);
    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(databaseName, tableName, (short) 10);
    assertEquals(results.get(0), partition);
    assertEquals(1, results.size());
    verify(cache).get(databaseName, tableName);
  }

  @Test
  public void listPartitionsWithoutTableCachedAndPartitionsSizeLimited() throws TException {
    String key = TableNameFactory.newInstance(databaseName, tableName);
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(cache.get(databaseName, tableName)).thenReturn(null).thenReturn(hiveTable);

    when(table.getDefinition()).thenReturn(tableDefinition);
    when(tableDefinition.getSchema()).thenReturn(Schema.of());
    when(bigQueryMetastore.getTable(databaseName, tableName)).thenReturn(table);

    when(factory.newInstance(any(org.apache.hadoop.hive.metastore.api.Table.class)))
        .thenReturn(partitionedTableService);
    when(partitionedTableService.getTable()).thenReturn(hiveTable);

    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      partitions.add(new Partition());
    }

    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(databaseName, tableName, (short) 5);
    assertEquals(5, results.size());
    assertEquals(10, partitions.size());
    verify(cache).get(databaseName, tableName);
  }

  @Test
  public void listPartitionsWithTableCachedAndPartitionsSizeNegativeReturnsAllPartitions() throws TException {
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(cache.get(databaseName, tableName)).thenReturn(hiveTable);
    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      partitions.add(new Partition());
    }

    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(databaseName, tableName, (short) -1);
    assertEquals(10, results.size());
  }

  @Test
  public void listPartitionsWithoutTableCachedAndPartitionsSizeNegativeReturnsAllPartitions() throws TException {
    String key = TableNameFactory.newInstance(databaseName, tableName);
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    when(cache.get(databaseName, tableName)).thenReturn(null).thenReturn(hiveTable);

    when(table.getDefinition()).thenReturn(tableDefinition);
    when(tableDefinition.getSchema()).thenReturn(Schema.of());
    when(bigQueryMetastore.getTable(databaseName, tableName)).thenReturn(table);

    when(factory.newInstance(any(org.apache.hadoop.hive.metastore.api.Table.class)))
        .thenReturn(partitionedTableService);
    when(partitionedTableService.getTable()).thenReturn(hiveTable);

    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      partitions.add(new Partition());
    }

    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(databaseName, tableName, (short) -1);
    assertEquals(10, results.size());
    verify(cache).get(databaseName, tableName);
  }

  private void setUpSchema() throws IOException {
    byte[] content = SchemaUtils.getTestData();
    when(extractionService.getStorage()).thenReturn(storage);
    when(storage.list(anyString(), any(BlobListOption.class), any(BlobListOption.class))).thenReturn(blobs);
    when(blobs.iterateAll()).thenReturn(Arrays.asList(blob));
    when(blob.getContent()).thenReturn(content);
    schema = SchemaUtils.getTestSchema();
  }

}
