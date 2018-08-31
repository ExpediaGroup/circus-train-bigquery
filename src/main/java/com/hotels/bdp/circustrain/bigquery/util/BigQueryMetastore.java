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

import java.util.UUID;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;

import com.hotels.bdp.circustrain.api.CircusTrainException;

@Component
public class BigQueryMetastore {

  private static final Logger log = LoggerFactory.getLogger(BigQueryMetastore.class);

  private final BigQuery client;

  @Autowired
  public BigQueryMetastore(BigQuery client) {
    this.client = client;
  }

  public void checkDbExists(String databaseName) throws UnknownDBException {
    if (client.getDataset(databaseName) == null) {
      throw new UnknownDBException("Dataset " + databaseName + " doesn't exist in BigQuery");
    }
  }

  public boolean tableExists(String databaseName, String tableName) throws TException {
    checkDbExists(databaseName);
    return client.getDataset(databaseName).get(tableName) != null;
  }

  public Table getTable(String databaseName, String tableName) {
    try {
      if (tableExists(databaseName, tableName)) {
        Table table = client.getDataset(databaseName).get(tableName);
        // try {
        // log.info("table schema: {}", table.getDefinition().getSchema());
        // log.info("table partition: {}", table.getDefinition());
        // } catch (Exception e) {
        // log.info("Could not get table schema");
        // }
        // com.google.cloud.bigquery.Schema schema = table.getDefinition().getSchema();
        // TableDefinition definition = table.getDefinition().toBuilder().setSchema(schema).build();
        // table = table.toBuilder().setDefinition(definition).build();
        return table;
      } else {
        throw new NoSuchObjectException(databaseName + "." + tableName + " could not be found");
      }
    } catch (TException e) {
      throw new CircusTrainException(e);
    }
  }

  public TableResult runJob(QueryJobConfiguration configuration) {
    try {
      JobId jobId = JobId.of(UUID.randomUUID().toString());
      Job queryJob = client.create(JobInfo.newBuilder(configuration).setJobId(jobId).build());
      queryJob = queryJob.waitFor();

      if (queryJob == null) {
        throw new RuntimeException("Job no longer exists");
      } else if (queryJob.getStatus().getError() != null) {
        throw new RuntimeException(queryJob.getStatus().getError().toString());
      }
      return queryJob.getQueryResults();
    } catch (InterruptedException e) {
      throw new CircusTrainException(e);
    }
  }

  public TableResult executeIntoDestinationTable(String destinationDBName, String destinationTableName, String query) {
    return runJob(configureFilterJob(destinationDBName, destinationTableName, query));
  }

  private QueryJobConfiguration configureFilterJob(String databaseName, String tableName, String partitionFilter) {
    return QueryJobConfiguration.newBuilder(partitionFilter).setDestinationTable(TableId.of(databaseName, tableName))
        .setUseLegacySql(false).setAllowLargeResults(true).build();
  }
}
