/**
 * Copyright (C) 2018-2019 Expedia, Inc.
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
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.table.service.TableServiceFactory;
import com.hotels.bdp.circustrain.bigquery.table.service.partitioned.PartitionedTableService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;
import com.hotels.bdp.circustrain.bigquery.util.SchemaExtractor;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryMetastoreClientTest {

  private final String databaseName = "database";
  private final String tableName = "table";

  private @Mock ExtractionService extractionService;
  private @Mock TableServiceFactory factory;
  private @Mock PartitionedTableService partitionedTableService;
  private @Mock BigQueryMetastore bigQueryMetastore;
  private @Mock com.google.cloud.bigquery.Table table;
  private @Mock SchemaExtractor schemaExtractor;

  private BigQueryMetastoreClient bigQueryMetastoreClient;
  private List<Partition> partitions = new ArrayList<>();
  private final HiveTableCache cache = new HiveTableCache();
  private final Table hiveTable = new Table();

  @Before
  public void init() throws IOException {
    hiveTable.setTableName(tableName);
    hiveTable.setDbName(databaseName);
    bigQueryMetastoreClient = new BigQueryMetastoreClient(bigQueryMetastore, extractionService, cache, factory,
        schemaExtractor);

    for (int i = 0; i < 10; i++) {
      partitions.add(new Partition());
    }
    when(partitionedTableService.getTable()).thenReturn(hiveTable);
    when(partitionedTableService.getPartitions()).thenReturn(partitions);
  }

  @Test
  public void getDatabase() throws TException {
    Database database = bigQueryMetastoreClient.getDatabase(databaseName);
    assertThat(database.getName(), is(databaseName));
  }

  @Test(expected = UnknownDBException.class)
  public void getDatabaseWhenDatabaseDoesntExistThrowsException() throws TException {
    doThrow(UnknownDBException.class).when(bigQueryMetastore).checkDbExists(databaseName);
    bigQueryMetastoreClient.getDatabase(databaseName);
  }

  @Test
  public void tableExists() throws TException {
    boolean tableExists = bigQueryMetastoreClient.tableExists(databaseName, tableName);
    verify(bigQueryMetastore).tableExists(databaseName, tableName);
    assertThat(tableExists, is(false));
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
  public void getTable() throws TException, IOException {
    HiveTableCache cache = new HiveTableCache();
    bigQueryMetastoreClient = new BigQueryMetastoreClient(bigQueryMetastore, extractionService, cache, factory,
        schemaExtractor);
    when(factory.newInstance(any(Table.class))).thenReturn(partitionedTableService);

    // check that the cache has been populated after the first run
    Table result = bigQueryMetastoreClient.getTable(databaseName, tableName);
    assertThat(cache.contains(databaseName, tableName), is(true));
    assertThat(cache.get(databaseName, tableName), is(result));

    // check that the object from cache is returned for the second run
    Table differentTable = new Table();
    when(partitionedTableService.getTable()).thenReturn(differentTable);
    result = bigQueryMetastoreClient.getTable(databaseName, tableName);
    assertThat(result, is(hiveTable));
    assertThat(result, not(is(differentTable)));
  }

  @Test
  public void getTableFromPartitionedTableService() throws TException, InterruptedException {
    when(factory.newInstance(any(Table.class))).thenReturn(partitionedTableService);
    Table result = bigQueryMetastoreClient.getTable(databaseName, tableName);
    assertThat(cache.contains(databaseName, tableName), is(true));
    assertThat(result, is(hiveTable));
  }

  @Test
  public void listPartitionsWithTableCached() throws TException {
    cache.put(hiveTable);
    Partition partition = new Partition();
    partitions = Arrays.asList(partition);
    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    when(partitionedTableService.getPartitions()).thenReturn(partitions);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(databaseName, tableName, (short) 10);
    assertThat(partition, is(results.get(0)));
    assertThat(results.size(), is(1));
  }

  @Test
  public void listPartitionsWithTableCachedAndLimitedSize() throws TException {
    cache.put(hiveTable);
    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(databaseName, tableName, (short) 5);
    assertThat(results.size(), is(5));
  }

  @Test
  public void listPartitionsWithoutTableCached() throws TException {
    when(factory.newInstance(any(Table.class))).thenReturn(partitionedTableService);
    assertThat(cache.contains(databaseName, tableName), is(false));
    List<Partition> results = bigQueryMetastoreClient.listPartitions(databaseName, tableName, (short) 10);
    assertThat(results.size(), is(10));
    assertThat(cache.contains(databaseName, tableName), is(true));
  }

  @Test
  public void listPartitionsWithoutTableCachedAndPartitionsSizeLimited() throws TException {
    when(factory.newInstance(any(Table.class))).thenReturn(partitionedTableService);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(databaseName, tableName, (short) 5);
    assertThat(results.size(), is(5));
    assertThat(cache.contains(databaseName, tableName), is(true));
  }

  @Test
  public void listPartitionsWithTableCachedAndPartitionsSizeNegativeReturnsAllPartitions() throws TException {
    cache.put(hiveTable);
    when(factory.newInstance(hiveTable)).thenReturn(partitionedTableService);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(databaseName, tableName, (short) -1);
    assertThat(results.size(), is(10));
  }

  @Test
  public void listPartitionsWithoutTableCachedAndPartitionsSizeNegativeReturnsAllPartitions() throws TException {
    when(factory.newInstance(any(Table.class))).thenReturn(partitionedTableService);
    List<Partition> results = bigQueryMetastoreClient.listPartitions(databaseName, tableName, (short) -1);
    assertThat(results.size(), is(10));
    assertThat(cache.contains(databaseName, tableName), is(true));
  }

}
