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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.event.EventTableReplication;
import com.hotels.bdp.circustrain.api.event.TableReplicationListener;
import com.hotels.bdp.circustrain.bigquery.extraction.BigQueryDataExtractionManager;

public class BigQueryReplicationListener implements TableReplicationListener {

  private static final Logger log = LoggerFactory.getLogger(BigQueryReplicationListener.class);

  private final BigQueryDataExtractionManager dataExtractionManager;
  private final BigQuery bigQuery;

  public BigQueryReplicationListener(BigQueryDataExtractionManager dataExtractionManager, BigQuery bigQuery) {
    this.dataExtractionManager = dataExtractionManager;
    this.bigQuery = bigQuery;
  }

  @Override
  public void tableReplicationStart(EventTableReplication eventTableReplication, String eventId) {
    log.info("Table replication from {}.{} -> {}.{} has started.",
        eventTableReplication.getSourceTable().getDatabaseName(), eventTableReplication.getSourceTable().getTableName(),
        eventTableReplication.getReplicaTable().getDatabaseName(),
        eventTableReplication.getReplicaTable().getTableName());
    Table table = getTable(eventTableReplication);
    dataExtractionManager.register(table);
  }

  @Override
  public void tableReplicationSuccess(EventTableReplication eventTableReplication, String eventId) {
    log.info("Table replication {}.{} -> {}.{} succeeded. Cleaning up temporary data",
        eventTableReplication.getSourceTable().getDatabaseName(), eventTableReplication.getSourceTable().getTableName(),
        eventTableReplication.getReplicaTable().getDatabaseName(),
        eventTableReplication.getReplicaTable().getTableName());
    dataExtractionManager.cleanupAll();
  }

  @Override
  public void tableReplicationFailure(EventTableReplication eventTableReplication, String eventId, Throwable t) {
    log.warn("Table replication {}.{} -> {}.{} failed. Cleaning up temporary data",
        eventTableReplication.getSourceTable().getDatabaseName(), eventTableReplication.getSourceTable().getTableName(),
        eventTableReplication.getReplicaTable().getDatabaseName(),
        eventTableReplication.getReplicaTable().getTableName());
    dataExtractionManager.cleanupAll();
  }

  private Table getTable(EventTableReplication tableReplication) {
    String databaseName = tableReplication.getSourceTable().getDatabaseName();
    String tableName = tableReplication.getSourceTable().getTableName();
    com.google.cloud.bigquery.Table table = bigQuery.getDataset(databaseName).get(tableName);
    if (table == null) {
      throw new CircusTrainException(databaseName + "." + tableName + " could not be found");
    }
    return table;
  }
}
