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
package com.hotels.bdp.circustrain.bigquery.conf;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

import static com.hotels.bdp.circustrain.bigquery.CircusTrainBigQueryConstants.PARTITION_BY;
import static com.hotels.bdp.circustrain.bigquery.CircusTrainBigQueryConstants.PARTITION_FILTER;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.conf.SourceTable;
import com.hotels.bdp.circustrain.api.conf.TableReplication;
import com.hotels.bdp.circustrain.api.conf.TableReplications;

public class PartitioningConfigurationTest {

  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "table";

  private final TableReplications tableReplications = new TableReplications();
  private final Map<String, Object> copierOptions = new HashMap<>();
  private final SourceTable sourceTable = new SourceTable();
  private final Table table = new Table();
  private PartitioningConfiguration configuration;

  @Before
  public void init() {
    table.setDbName(DB_NAME);
    table.setTableName(TABLE_NAME);
    sourceTable.setDatabaseName(table.getDbName());
    sourceTable.setTableName(table.getTableName());
    TableReplication replication = new TableReplication();
    replication.setSourceTable(sourceTable);
    replication.setCopierOptions(copierOptions);
    tableReplications.setTableReplications(Collections.singletonList(replication));
    configuration = new PartitioningConfiguration(tableReplications);
  }

  @Test
  public void getPartitionFilterPartitionFilterConfigured() {
    String filter = "foo > 5";
    copierOptions.put(PARTITION_FILTER, filter);
    assertEquals(filter, configuration.getPartitionFilterFor(table));
  }

  @Test
  public void getPartitionByPartitionFilterNotConfigured() {
    String partitionKey = "foo";
    copierOptions.put(PARTITION_BY, partitionKey);
    assertNull(configuration.getPartitionFilterFor(table));
  }

  @Test
  public void getPartitionByPartitionPartitionByIsNull() {
    copierOptions.put(PARTITION_BY, null);
    assertNull(configuration.getPartitionByFor(table));
  }

  @Test
  public void getPartitionByPartitionByConfigured() {
    String partitionKey = "foo";
    copierOptions.put(PARTITION_BY, partitionKey);
    assertEquals(partitionKey, configuration.getPartitionByFor(table));
  }

  @Test
  public void getPartitionByPartitionByNotConfigured() {
    String filter = "foo > 5";
    copierOptions.put(PARTITION_FILTER, filter);
    assertNull(configuration.getPartitionByFor(table));
  }

  @Test
  public void getPartitionFilterPartitionPartitionFilterIsNull() {
    copierOptions.put(PARTITION_FILTER, null);
    assertNull(configuration.getPartitionFilterFor(table));
  }

  @Test(expected = CircusTrainException.class)
  public void sameTableConfiguredTwiceThrowsException() {
    TableReplication replication = tableReplications.getTableReplications().get(0);
    tableReplications.setTableReplications(ImmutableList.<TableReplication> of(replication, replication));
    new PartitioningConfiguration(tableReplications);
  }

  @Test
  public void noConfiguration() {
    assertNull(configuration.getPartitionByFor(table));
    assertNull(configuration.getPartitionFilterFor(table));
  }

  @Test
  public void twoRepartitionsConfigured() {
    TableReplication replicationOne = tableReplications.getTableReplications().get(0);
    TableReplication replicationTwo = new TableReplication();
    SourceTable secondTable = new SourceTable();
    secondTable.setDatabaseName("second");
    secondTable.setTableName("table");
    Map<String, Object> secondCopierOptions = new HashMap<>();
    replicationTwo.setCopierOptions(secondCopierOptions);
    replicationTwo.setSourceTable(secondTable);
    tableReplications.setTableReplications(ImmutableList.<TableReplication> of(replicationOne, replicationTwo));
    Table secondHiveTable = new Table();
    secondHiveTable.setDbName(secondTable.getDatabaseName());
    secondHiveTable.setTableName(secondTable.getTableName());

    String partitionKeyOne = "foo";
    copierOptions.put(PARTITION_BY, partitionKeyOne);
    String filterOne = "foo > 5";
    copierOptions.put(PARTITION_FILTER, filterOne);

    String partitionKeyTwo = "foo";
    secondCopierOptions.put(PARTITION_BY, partitionKeyTwo);
    String filterTwo = "bar > 5";
    secondCopierOptions.put(PARTITION_FILTER, filterTwo);

    PartitioningConfiguration configuration = new PartitioningConfiguration(tableReplications);
    assertEquals(filterOne, configuration.getPartitionFilterFor(table));
    assertEquals(partitionKeyOne, configuration.getPartitionByFor(table));

    assertEquals(filterTwo, configuration.getPartitionFilterFor(secondHiveTable));
    assertEquals(partitionKeyTwo, configuration.getPartitionByFor(secondHiveTable));
  }
}
