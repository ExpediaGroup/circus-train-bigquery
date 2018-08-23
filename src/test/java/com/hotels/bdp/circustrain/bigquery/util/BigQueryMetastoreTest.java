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
package com.hotels.bdp.circustrain.bigquery.util;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;

import com.hotels.bdp.circustrain.api.CircusTrainException;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryMetastoreTest {

  private @Mock BigQuery client;
  private @Mock Dataset dataset;
  private @Mock Table table;
  private @Mock Job job;
  private @Mock JobStatus jobStatus;
  private @Mock TableResult tableResult;

  private BigQueryMetastore bigQueryMetastore;
  private QueryJobConfiguration queryJobConfiguration;

  private final String databaseName = "testDatabase";
  private final String tableName = "testTable";
  private final String query = "testData < 10";

  @Before
  public void init() {
    bigQueryMetastore = new BigQueryMetastore(client);
    when(client.getDataset(databaseName)).thenReturn(dataset);
    when(dataset.get(tableName)).thenReturn(table);
    when(client.create((JobInfo) anyObject())).thenReturn(job);
    queryJobConfiguration = QueryJobConfiguration.newBuilder(query).build();
    when(job.getStatus()).thenReturn(jobStatus);
  }

  @Test(expected = UnknownDBException.class)
  public void checkDbExistsOnAbsentDatabase() throws UnknownDBException {
    when(client.getDataset(databaseName)).thenReturn(null);
    bigQueryMetastore.checkDbExists(databaseName);
  }

  @Test
  public void checkTableExistsOnExistingTable() throws TException {
    boolean result = bigQueryMetastore.tableExists(databaseName, tableName);
    assertThat(result, is(true));
  }

  @Test
  public void checkTableExistsOnAbsentTable() throws TException {
    when(dataset.get(tableName)).thenReturn(null);
    boolean result = bigQueryMetastore.tableExists(databaseName, tableName);
    assertThat(result, is(false));
  }

  @Test
  public void getTableOnExistingTable() {
    Table result = bigQueryMetastore.getTable(databaseName, tableName);
    assertThat(result, is(table));
  }

  @Test(expected = CircusTrainException.class)
  public void getTableOnAbsentDatabase() {
    when(client.getDataset(databaseName)).thenReturn(null);
    bigQueryMetastore.getTable(databaseName, tableName);
  }

  @Test(expected = CircusTrainException.class)
  public void getTableOnAbsentTable() {
    when(dataset.get(tableName)).thenReturn(null);
    bigQueryMetastore.getTable(databaseName, tableName);
  }

  @Test
  public void runJob() throws InterruptedException {
    when(job.waitFor()).thenReturn(job);
    when(job.getQueryResults()).thenReturn(tableResult);
    TableResult result = bigQueryMetastore.runJob(queryJobConfiguration);
    assertThat(result, is(tableResult));
  }

  @Test(expected = RuntimeException.class)
  public void runNullJob() {
    bigQueryMetastore.runJob(queryJobConfiguration);
  }

  @Test(expected = CircusTrainException.class)
  public void runInterruptedJob() throws InterruptedException {
    when(job.waitFor()).thenThrow(InterruptedException.class);
    bigQueryMetastore.runJob(queryJobConfiguration);
  }

  @Test(expected = RuntimeException.class)
  public void runJobWithError() throws InterruptedException {
    BigQueryError bigQueryError = new BigQueryError("reason", "location", "error message");
    when(job.waitFor()).thenReturn(job);
    when(jobStatus.getError()).thenReturn(bigQueryError);
    assertNotNull(bigQueryError);
    bigQueryMetastore.runJob(queryJobConfiguration);
  }

  @Test
  public void executeIntoDestinationTable() throws InterruptedException {
    when(job.waitFor()).thenReturn(job);
    when(job.getQueryResults()).thenReturn(tableResult);
    TableResult result = bigQueryMetastore.executeIntoDestinationTable(databaseName, tableName, query);
    assertThat(result, is(tableResult));
  }

}
