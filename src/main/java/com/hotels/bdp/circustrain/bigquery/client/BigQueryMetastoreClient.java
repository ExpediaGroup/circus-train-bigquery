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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.bigquery.conversion.BigQueryToHiveTableConverter;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.extraction.container.PostExtractionAction;
import com.hotels.bdp.circustrain.bigquery.extraction.container.UpdateTableSchemaPostExtractionAction;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.table.service.TableServiceFactory;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

class BigQueryMetastoreClient implements CloseableMetaStoreClient {

  private static final Logger log = LoggerFactory.getLogger(BigQueryMetastoreClient.class);
  private static final String ERROR_MESSAGE = "Cannot execute {} on Big Query";

  private final BigQueryMetastore bigQueryMetastore;
  private final ExtractionService extractionService;
  private final TableServiceFactory tableServiceFactory;
  private final HiveTableCache cache;

  BigQueryMetastoreClient(BigQueryMetastore bigQueryMetastore, ExtractionService extractionService,
      HiveTableCache cache, TableServiceFactory tableServiceFactory) {
    this.bigQueryMetastore = bigQueryMetastore;
    this.extractionService = extractionService;
    this.tableServiceFactory = tableServiceFactory;
    this.cache = cache;
  }

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public void close() {
  }

  @Override
  public Database getDatabase(String databaseName) throws TException {
    log.info("Getting database '{}' from BigQuery", databaseName);
    bigQueryMetastore.checkDbExists(databaseName);
    return new Database(databaseName, null, null, null);
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) throws TException {
    return bigQueryMetastore.tableExists(databaseName, tableName);
  }

  @Override
  public Table getTable(final String databaseName, final String tableName) throws TException {
    log.info("Getting table '{}.{}' from BigQuery", databaseName, tableName);
    if (cache.contains(databaseName, tableName)) {
      return cache.get(databaseName, tableName);
    }

    com.google.cloud.bigquery.Table bigQueryTable =
        bigQueryMetastore.getTable(databaseName, tableName);

    ExtractionUri extractionUri = new ExtractionUri();
    List<PostExtractionAction> extractionActions = new ArrayList<>();
    extractionActions.add(new UpdateTableSchemaPostExtractionAction(databaseName, tableName, cache,
        extractionService.getStorage(), extractionUri));
    ExtractionContainer container =
        new ExtractionContainer(bigQueryTable, extractionUri, extractionActions);
    extractionService.register(container);

    Table hiveTable = tableServiceFactory
        .newInstance(new BigQueryToHiveTableConverter().withDatabaseName(databaseName)
            .withTableName(tableName).withLocation(extractionUri.getTableLocation()).convert())
        .getTable();

    cache.put(hiveTable);
    return hiveTable;
  }

  @Override
  public List<Partition> listPartitions(String dbName, String tblName, short max)
      throws NoSuchObjectException, MetaException, TException {
    log.info("Listing partitions for table {}.{}", dbName, tblName);
    Table hiveTable = cache.get(dbName, tblName);
    if (hiveTable == null) {
      hiveTable = getTable(dbName, tblName);
    }
    List<Partition> partitions = tableServiceFactory.newInstance(hiveTable).getPartitions();
    if (max < 0) {
      return partitions;
    }
    final int fromIndex = 0;
    int toIndex = Math.min(max, partitions.size());
    return partitions.subList(fromIndex, toIndex);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName,
      String tableName, List<String> partNames, List<String> colNames)
          throws NoSuchObjectException, MetaException, TException {
    return Collections.emptyMap();
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws NoSuchObjectException, MetaException, TException {
    return Collections.emptyList();
  }

  @Override
  public void alter_table(String s, String s1, Table table)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "alter_table"));
  }

  @Override
  public boolean tableExists(String s) throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "tableExists"));
  }

  @Override
  public boolean isCompatibleWith(HiveConf hiveConf) {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "isCompatibleWith"));
  }

  @Override
  public void setHiveAddedJars(String s) {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "setHiveAddedJars"));
  }

  @Override
  public boolean isLocalMetaStore() {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "isLocalMetaStore"));
  }

  @Override
  public void reconnect() throws MetaException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "reconnect"));
  }

  @Override
  public void setMetaConf(String s, String s1) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "setMetaConf"));
  }

  @Override
  public String getMetaConf(String s) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getMetaConf"));
  }

  @Override
  public List<String> getDatabases(String s) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getDatabases"));
  }

  @Override
  public List<String> getAllDatabases() throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getAllDatabases"));
  }

  @Override
  public List<String> getTables(String s, String s1)
      throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getTables"));
  }

  @Override
  public List<String> getTables(String s, String s1, TableType tableType)
      throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getTables"));
  }

  @Override
  public List<TableMeta> getTableMeta(String s, String s1, List<String> list)
      throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getTableMeta"));
  }

  @Override
  public List<String> getAllTables(String s) throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getAllTables"));
  }

  @Override
  public List<String> listTableNamesByFilter(String s, String s1, short i)
      throws MetaException, TException, InvalidOperationException, UnknownDBException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "listTableNamesByFilter"));
  }

  @Override
  public void dropTable(String s, String s1, boolean b, boolean b1)
      throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropTable"));
  }

  @Override
  public void dropTable(String s, String s1, boolean b, boolean b1, boolean b2)
      throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropTable"));
  }

  @Override
  public void dropTable(String s, boolean b)
      throws MetaException, UnknownTableException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropTable"));
  }

  @Override
  public void dropTable(String s, String s1)
      throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropTable"));
  }

  @Override
  public Table getTable(String s) throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getTable"));
  }

  @Override
  public List<Table> getTableObjectsByName(String s, List<String> list)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getTableObjectsByName"));
  }

  @Override
  public Partition appendPartition(String s, String s1, List<String> list)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "appendPartition"));
  }

  @Override
  public Partition appendPartition(String s, String s1, String s2)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "appendPartition"));
  }

  @Override
  public Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "add_partition"));
  }

  @Override
  public int add_partitions(List<Partition> list)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "add_partitions"));
  }

  @Override
  public int add_partitions_pspec(PartitionSpecProxy partitionSpecProxy)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "add_partitions_pspec"));
  }

  @Override
  public List<Partition> add_partitions(List<Partition> list, boolean b, boolean b1)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "add_partitions"));
  }

  @Override
  public Partition getPartition(String s, String s1, List<String> list)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getPartition"));
  }

  @Override
  public Partition exchange_partition(Map<String, String> map, String s, String s1, String s2,
      String s3) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "exchange_partition"));
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> map, String s, String s1,
      String s2, String s3)
          throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "exchange_partitions"));
  }

  @Override
  public Partition getPartition(String s, String s1, String s2)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getPartition"));
  }

  @Override
  public Partition getPartitionWithAuthInfo(String s, String s1, List<String> list, String s2,
      List<String> list1)
          throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "getPartitionWithAuthInfo"));
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String s, String s1, int i) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "listPartitionSpecs"));
  }

  @Override
  public List<Partition> listPartitions(String s, String s1, List<String> list, short i)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "listPartitions"));
  }

  @Override
  public List<String> listPartitionNames(String s, String s1, short i)
      throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "listPartitionNames"));
  }

  @Override
  public List<String> listPartitionNames(String s, String s1, List<String> list, short i)
      throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "listPartitionNames"));
  }

  @Override
  public int getNumPartitionsByFilter(String s, String s1, String s2)
      throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "getNumPartitionsByFilter"));
  }

  @Override
  public List<Partition> listPartitionsByFilter(String s, String s1, String s2, short i)
      throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "listPartitionsByFilter"));
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String s, String s1, String s2, int i)
      throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "listPartitionSpecsByFilter"));
  }

  @Override
  public boolean listPartitionsByExpr(String s, String s1, byte[] bytes, String s2, short i,
      List<Partition> list) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "listPartitionsByExpr"));
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String s, String s1, short i, String s2,
      List<String> list) throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "listPartitionsWithAuthInfo"));
  }

  @Override
  public List<Partition> getPartitionsByNames(String s, String s1, List<String> list)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getPartitionsByNames"));
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String s, String s1, List<String> list, short i,
      String s2, List<String> list1) throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "listPartitionsWithAuthInfo"));
  }

  @Override
  public void markPartitionForEvent(String s, String s1, Map<String, String> map,
      PartitionEventType partitionEventType)
          throws MetaException, NoSuchObjectException, TException, UnknownTableException,
          UnknownDBException, UnknownPartitionException, InvalidPartitionException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "markPartitionForEvent"));
  }

  @Override
  public boolean isPartitionMarkedForEvent(String s, String s1, Map<String, String> map,
      PartitionEventType partitionEventType)
          throws MetaException, NoSuchObjectException, TException, UnknownTableException,
          UnknownDBException, UnknownPartitionException, InvalidPartitionException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "isPartitionMarkedForEvent"));
  }

  @Override
  public void validatePartitionNameCharacters(List<String> list) throws TException, MetaException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "validatePartitionNameCharacters"));
  }

  @Override
  public void createTable(Table table) throws AlreadyExistsException, InvalidObjectException,
  MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "createTable"));
  }

  @Override
  public void alter_table(String s, String s1, Table table, boolean b)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "alter_table"));
  }

  @Override
  public void alter_table_with_environmentContext(String s, String s1, Table table,
      EnvironmentContext environmentContext)
          throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "alter_table_with_environmentContext"));
  }

  @Override
  public void createDatabase(Database database)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "createDatabase"));
  }

  @Override
  public void dropDatabase(String s)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropDatabase"));
  }

  @Override
  public void dropDatabase(String s, boolean b, boolean b1)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropDatabase"));
  }

  @Override
  public void dropDatabase(String s, boolean b, boolean b1, boolean b2)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropDatabase"));
  }

  @Override
  public void alterDatabase(String s, Database database)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "alterDatabase"));
  }

  @Override
  public boolean dropPartition(String s, String s1, List<String> list, boolean b)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropPartition"));
  }

  @Override
  public boolean dropPartition(String s, String s1, List<String> list,
      PartitionDropOptions partitionDropOptions) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropPartition"));
  }

  @Override
  public List<Partition> dropPartitions(String s, String s1, List<ObjectPair<Integer, byte[]>> list,
      boolean b, boolean b1) throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropPartitions"));
  }

  @Override
  public List<Partition> dropPartitions(String s, String s1, List<ObjectPair<Integer, byte[]>> list,
      boolean b, boolean b1, boolean b2) throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropPartitions"));
  }

  @Override
  public List<Partition> dropPartitions(String s, String s1, List<ObjectPair<Integer, byte[]>> list,
      PartitionDropOptions partitionDropOptions) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropPartitions"));
  }

  @Override
  public boolean dropPartition(String s, String s1, String s2, boolean b)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropPartition"));
  }

  @Override
  public void alter_partition(String s, String s1, Partition partition)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "alter_partition"));
  }

  @Override
  public void alter_partition(String s, String s1, Partition partition,
      EnvironmentContext environmentContext)
          throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "alter_partition"));
  }

  @Override
  public void alter_partitions(String s, String s1, List<Partition> list)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "alter_partitions"));
  }

  @Override
  public void alter_partitions(String s, String s1, List<Partition> list,
      EnvironmentContext environmentContext)
          throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "alter_partitions"));
  }

  @Override
  public void renamePartition(String s, String s1, List<String> list, Partition partition)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "renamePartition"));
  }

  @Override
  public List<FieldSchema> getFields(String s, String s1)
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getFields"));
  }

  @Override
  public List<FieldSchema> getSchema(String s, String s1)
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getSchema"));
  }

  @Override
  public String getConfigValue(String s, String s1) throws TException, ConfigValSecurityException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getConfigValue"));
  }

  @Override
  public List<String> partitionNameToVals(String s) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "partitionNameToVals"));
  }

  @Override
  public Map<String, String> partitionNameToSpec(String s) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "partitionNameToSpec"));
  }

  @Override
  public void createIndex(Index index, Table table) throws InvalidObjectException, MetaException,
  NoSuchObjectException, TException, AlreadyExistsException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "createIndex"));
  }

  @Override
  public void alter_index(String s, String s1, String s2, Index index)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "alter_index"));
  }

  @Override
  public Index getIndex(String s, String s1, String s2)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getIndex"));
  }

  @Override
  public List<Index> listIndexes(String s, String s1, short i)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "listIndexes"));
  }

  @Override
  public List<String> listIndexNames(String s, String s1, short i)
      throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "listIndexNames"));
  }

  @Override
  public boolean dropIndex(String s, String s1, String s2, boolean b)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropIndex"));
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      InvalidInputException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "updateTableColumnStatistics"));
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      InvalidInputException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "updatePartitionColumnStatistics"));
  }

  @Override
  public boolean deletePartitionColumnStatistics(String s, String s1, String s2, String s3)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException,
      InvalidInputException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "deletePartitionColumnStatistics"));
  }

  @Override
  public boolean deleteTableColumnStatistics(String s, String s1, String s2)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException,
      InvalidInputException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "deleteTableColumnStatistics"));
  }

  @Override
  public boolean create_role(Role role) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "create_role"));
  }

  @Override
  public boolean drop_role(String s) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "drop_role"));
  }

  @Override
  public List<String> listRoleNames() throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "listRoleNames"));
  }

  @Override
  public boolean grant_role(String s, String s1, PrincipalType principalType, String s2,
      PrincipalType principalType1, boolean b) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "grant_role"));
  }

  @Override
  public boolean revoke_role(String s, String s1, PrincipalType principalType, boolean b)
      throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "revoke_role"));
  }

  @Override
  public List<Role> list_roles(String s, PrincipalType principalType)
      throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "list_roles"));
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObjectRef, String s,
      List<String> list) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "get_privilege_set"));
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String s, PrincipalType principalType,
      HiveObjectRef hiveObjectRef) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "list_privileges"));
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privilegeBag) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "grant_privileges"));
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privilegeBag, boolean b)
      throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "revoke_privileges"));
  }

  @Override
  public String getDelegationToken(String s, String s1) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getDelegationToken"));
  }

  @Override
  public long renewDelegationToken(String s) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "renewDelegationToken"));
  }

  @Override
  public void cancelDelegationToken(String s) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "cancelDelegationToken"));
  }

  @Override
  public String getTokenStrForm() throws IOException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getTokenStrForm"));
  }

  @Override
  public boolean addToken(String s, String s1) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "addToken"));
  }

  @Override
  public boolean removeToken(String s) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "removeToken"));
  }

  @Override
  public String getToken(String s) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getToken"));
  }

  @Override
  public List<String> getAllTokenIdentifiers() throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getAllTokenIdentifiers"));
  }

  @Override
  public int addMasterKey(String s) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "addMasterKey"));
  }

  @Override
  public void updateMasterKey(Integer integer, String s)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "updateMasterKey"));
  }

  @Override
  public boolean removeMasterKey(Integer integer) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "removeMasterKey"));
  }

  @Override
  public String[] getMasterKeys() throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getMasterKeys"));
  }

  @Override
  public void createFunction(Function function)
      throws InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "createFunction"));
  }

  @Override
  public void alterFunction(String s, String s1, Function function)
      throws InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "alterFunction"));
  }

  @Override
  public void dropFunction(String s, String s1) throws MetaException, NoSuchObjectException,
  InvalidObjectException, InvalidInputException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropFunction"));
  }

  @Override
  public Function getFunction(String s, String s1) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getFunction"));
  }

  @Override
  public List<String> getFunctions(String s, String s1) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getFunctions"));
  }

  @Override
  public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getAllFunctions"));
  }

  @Override
  public ValidTxnList getValidTxns() throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getValidTxns"));
  }

  @Override
  public ValidTxnList getValidTxns(long l) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getValidTxns"));
  }

  @Override
  public long openTxn(String s) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "openTxn"));
  }

  @Override
  public OpenTxnsResponse openTxns(String s, int i) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "openTxns"));
  }

  @Override
  public void rollbackTxn(long l) throws NoSuchTxnException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "rollbackTxn"));
  }

  @Override
  public void commitTxn(long l) throws NoSuchTxnException, TxnAbortedException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "commitTxn"));
  }

  @Override
  public void abortTxns(List<Long> list) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "abortTxns"));
  }

  @Override
  public GetOpenTxnsInfoResponse showTxns() throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "showTxns"));
  }

  @Override
  public LockResponse lock(LockRequest lockRequest)
      throws NoSuchTxnException, TxnAbortedException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "lock"));
  }

  @Override
  public LockResponse checkLock(long l)
      throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "checkLock"));
  }

  @Override
  public void unlock(long l) throws NoSuchLockException, TxnOpenException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "unlock"));
  }

  @Override
  public ShowLocksResponse showLocks() throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "showLocks"));
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "showLocks"));
  }

  @Override
  public void heartbeat(long l, long l1)
      throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "heartbeat"));
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(long l, long l1) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "heartbeatTxnRange"));
  }

  @Override
  public void compact(String s, String s1, String s2, CompactionType compactionType)
      throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "compact"));
  }

  @Override
  public void compact(String s, String s1, String s2, CompactionType compactionType,
      Map<String, String> map) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "compact"));
  }

  @Override
  public CompactionResponse compact2(String s, String s1, String s2, CompactionType compactionType,
      Map<String, String> map) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "compact2"));
  }

  @Override
  public ShowCompactResponse showCompactions() throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "showCompactions"));
  }

  @Override
  public void addDynamicPartitions(long l, String s, String s1, List<String> list)
      throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "addDynamicPartitions"));
  }

  @Override
  public void addDynamicPartitions(long l, String s, String s1, List<String> list,
      DataOperationType dataOperationType) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "addDynamicPartitions"));
  }

  @Override
  public void insertTable(Table table, boolean b) throws MetaException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "insertTable"));
  }

  @Override
  public NotificationEventResponse getNextNotification(long l, int i,
      NotificationFilter notificationFilter) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getNextNotification"));
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "getCurrentNotificationEventId"));
  }

  @Override
  public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "fireListenerEvent"));
  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(
      GetPrincipalsInRoleRequest getPrincipalsInRoleRequest) throws MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "get_principals_in_role"));
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest getRoleGrantsForPrincipalRequest)
          throws MetaException, TException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "get_role_grants_for_principal"));
  }

  @Override
  public AggrStats getAggrColStatsFor(String s, String s1, List<String> list, List<String> list1)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getAggrColStatsFor"));
  }

  @Override
  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest setPartitionsStatsRequest)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      InvalidInputException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "setPartitionColumnStatistics"));
  }

  @Override
  public void flushCache() {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "flushCache"));
  }

  @Override
  public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> list) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getFileMetadata"));
  }

  @Override
  public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(List<Long> list,
      ByteBuffer byteBuffer, boolean b) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getFileMetadataBySarg"));
  }

  @Override
  public void clearFileMetadata(List<Long> list) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "clearFileMetadata"));
  }

  @Override
  public void putFileMetadata(List<Long> list, List<ByteBuffer> list1) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "putFileMetadata"));
  }

  @Override
  public boolean isSameConfObj(HiveConf hiveConf) {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "isSameConfObj"));
  }

  @Override
  public boolean cacheFileMetadata(String s, String s1, String s2, boolean b) throws TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "cacheFileMetadata"));
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest primaryKeysRequest)
      throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getPrimaryKeys"));
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest)
      throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "getForeignKeys"));
  }

  @Override
  public void createTableWithConstraints(Table table, List<SQLPrimaryKey> list,
      List<SQLForeignKey> list1) throws AlreadyExistsException, InvalidObjectException,
  MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(
        String.format(ERROR_MESSAGE, "createTableWithConstraints"));
  }

  @Override
  public void dropConstraint(String s, String s1, String s2)
      throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "dropConstraint"));
  }

  @Override
  public void addPrimaryKey(List<SQLPrimaryKey> list)
      throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "addPrimaryKey"));
  }

  @Override
  public void addForeignKey(List<SQLForeignKey> list)
      throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException(String.format(ERROR_MESSAGE, "addForeignKey"));
  }
}
