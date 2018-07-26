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

import static com.hotels.bdp.circustrain.bigquery.util.BigQueryKey.makeKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
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

import com.google.cloud.bigquery.Schema;

import com.hotels.bdp.circustrain.bigquery.cache.MetastoreClientCache;
import com.hotels.bdp.circustrain.bigquery.conversion.BigQueryToHiveTableConverter;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.partition.PartitionService;
import com.hotels.bdp.circustrain.bigquery.partition.PartitionServiceFactory;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

class BigQueryMetastoreClient implements CloseableMetaStoreClient {

  private static final Logger log = LoggerFactory.getLogger(BigQueryMetastoreClient.class);

  private final BigQueryMetastore bigQueryMetastore;
  private final ExtractionService extractionService;
  private final PartitionServiceFactory partitionServiceFactory;
  private final MetastoreClientCache cache;

  BigQueryMetastoreClient(
      BigQueryMetastore bigQueryMetastore,
      ExtractionService extractionService,
      MetastoreClientCache metastoreClientCache,
      PartitionServiceFactory partitionServiceFactory) {
    this.bigQueryMetastore = bigQueryMetastore;
    this.extractionService = extractionService;
    this.partitionServiceFactory = partitionServiceFactory;
    this.cache = metastoreClientCache;
  }

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public void close() {}

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
    String tableKey = makeKey(databaseName, tableName);
    if (cache.containsTable(tableKey)) {
      log.debug("Loading table '{}.{}' from tableCache", databaseName, tableName);
      return cache.getTable(tableKey);
    }

    com.google.cloud.bigquery.Table bigQueryTable = bigQueryMetastore.getTable(databaseName, tableName);

    ExtractionUri extractionUri = new ExtractionUri();
    ExtractionContainer container = new ExtractionContainer(bigQueryTable, extractionUri, false);
    extractionService.register(container);

    final String location = "gs://" + extractionUri.getBucket() + "/" + extractionUri.getFolder() + "/";
    final Schema schema = bigQueryTable.getDefinition().getSchema();
    Table hiveTable = new BigQueryToHiveTableConverter()
        .withDatabaseName(databaseName)
        .withTableName(tableName)
        .withSchema(schema)
        .withCols(schema)
        .withLocation(location)
        .convert();

    PartitionService partitionService = partitionServiceFactory.newInstance(hiveTable);
    for (Partition partition : partitionService.execute()) {
      cache.cachePartition(partition);
    }
    cache.cacheTable(hiveTable);
    return hiveTable;
  }

  @Override
  public List<Partition> listPartitions(String dbName, String tblName, short max)
    throws NoSuchObjectException, MetaException, TException {
    String key = makeKey(dbName, tblName);
    List<Partition> partitions = cache.getPartitions(key);
    log.info("Fetched {} partition(s)", partitions.size());
    return partitions;
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String dbName,
      String tableName,
      List<String> partNames,
      List<String> colNames)
    throws NoSuchObjectException, MetaException, TException {
    return new HashMap<>();
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames)
    throws NoSuchObjectException, MetaException, TException {
    return new ArrayList<>();
  }

  @Override
  public void alter_table(String s, String s1, Table table)
    throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean tableExists(String s) throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCompatibleWith(HiveConf hiveConf) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHiveAddedJars(String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isLocalMetaStore() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reconnect() throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setMetaConf(String s, String s1) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getMetaConf(String s) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getDatabases(String s) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAllDatabases() throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getTables(String s, String s1) throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getTables(String s, String s1, TableType tableType)
    throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TableMeta> getTableMeta(String s, String s1, List<String> list)
    throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAllTables(String s) throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listTableNamesByFilter(String s, String s1, short i)
    throws MetaException, TException, InvalidOperationException, UnknownDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropTable(String s, String s1, boolean b, boolean b1)
    throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropTable(String s, String s1, boolean b, boolean b1, boolean b2)
    throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropTable(String s, boolean b)
    throws MetaException, UnknownTableException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropTable(String s, String s1) throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Table getTable(String s) throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Table> getTableObjectsByName(String s, List<String> list)
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition appendPartition(String s, String s1, List<String> list)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition appendPartition(String s, String s1, String s2)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition add_partition(Partition partition)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int add_partitions(List<Partition> list)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int add_partitions_pspec(PartitionSpecProxy partitionSpecProxy)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> add_partitions(List<Partition> list, boolean b, boolean b1)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition getPartition(String s, String s1, List<String> list)
    throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition exchange_partition(Map<String, String> map, String s, String s1, String s2, String s3)
    throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> map, String s, String s1, String s2, String s3)
    throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition getPartition(String s, String s1, String s2)
    throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition getPartitionWithAuthInfo(String s, String s1, List<String> list, String s2, List<String> list1)
    throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String s, String s1, int i) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> listPartitions(String s, String s1, List<String> list, short i)
    throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listPartitionNames(String s, String s1, short i) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listPartitionNames(String s, String s1, List<String> list, short i)
    throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumPartitionsByFilter(String s, String s1, String s2)
    throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> listPartitionsByFilter(String s, String s1, String s2, short i)
    throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String s, String s1, String s2, int i)
    throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean listPartitionsByExpr(String s, String s1, byte[] bytes, String s2, short i, List<Partition> list)
    throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String s, String s1, short i, String s2, List<String> list)
    throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> getPartitionsByNames(String s, String s1, List<String> list)
    throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(
      String s,
      String s1,
      List<String> list,
      short i,
      String s2,
      List<String> list1)
    throws MetaException, TException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markPartitionForEvent(String s, String s1, Map<String, String> map, PartitionEventType partitionEventType)
    throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
    UnknownPartitionException, InvalidPartitionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPartitionMarkedForEvent(
      String s,
      String s1,
      Map<String, String> map,
      PartitionEventType partitionEventType)
    throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
    UnknownPartitionException, InvalidPartitionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void validatePartitionNameCharacters(List<String> list) throws TException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTable(Table table)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_table(String s, String s1, Table table, boolean b)
    throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_table_with_environmentContext(
      String s,
      String s1,
      Table table,
      EnvironmentContext environmentContext)
    throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createDatabase(Database database)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropDatabase(String s)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropDatabase(String s, boolean b, boolean b1)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropDatabase(String s, boolean b, boolean b1, boolean b2)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterDatabase(String s, Database database) throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropPartition(String s, String s1, List<String> list, boolean b)
    throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropPartition(String s, String s1, List<String> list, PartitionDropOptions partitionDropOptions)
    throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> dropPartitions(
      String s,
      String s1,
      List<ObjectPair<Integer, byte[]>> list,
      boolean b,
      boolean b1)
    throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> dropPartitions(
      String s,
      String s1,
      List<ObjectPair<Integer, byte[]>> list,
      boolean b,
      boolean b1,
      boolean b2)
    throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> dropPartitions(
      String s,
      String s1,
      List<ObjectPair<Integer, byte[]>> list,
      PartitionDropOptions partitionDropOptions)
    throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropPartition(String s, String s1, String s2, boolean b)
    throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_partition(String s, String s1, Partition partition)
    throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_partition(String s, String s1, Partition partition, EnvironmentContext environmentContext)
    throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_partitions(String s, String s1, List<Partition> list)
    throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_partitions(String s, String s1, List<Partition> list, EnvironmentContext environmentContext)
    throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void renamePartition(String s, String s1, List<String> list, Partition partition)
    throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<FieldSchema> getFields(String s, String s1)
    throws MetaException, TException, UnknownTableException, UnknownDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<FieldSchema> getSchema(String s, String s1)
    throws MetaException, TException, UnknownTableException, UnknownDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getConfigValue(String s, String s1) throws TException, ConfigValSecurityException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> partitionNameToVals(String s) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, String> partitionNameToSpec(String s) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createIndex(Index index, Table table)
    throws InvalidObjectException, MetaException, NoSuchObjectException, TException, AlreadyExistsException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alter_index(String s, String s1, String s2, Index index)
    throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Index getIndex(String s, String s1, String s2)
    throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Index> listIndexes(String s, String s1, short i) throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listIndexNames(String s, String s1, short i) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropIndex(String s, String s1, String s2, boolean b)
    throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean deletePartitionColumnStatistics(String s, String s1, String s2, String s3)
    throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean deleteTableColumnStatistics(String s, String s1, String s2)
    throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean create_role(Role role) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean drop_role(String s) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listRoleNames() throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean grant_role(
      String s,
      String s1,
      PrincipalType principalType,
      String s2,
      PrincipalType principalType1,
      boolean b)
    throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean revoke_role(String s, String s1, PrincipalType principalType, boolean b)
    throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Role> list_roles(String s, PrincipalType principalType) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObjectRef, String s, List<String> list)
    throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String s, PrincipalType principalType, HiveObjectRef hiveObjectRef)
    throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privilegeBag) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privilegeBag, boolean b) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDelegationToken(String s, String s1) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long renewDelegationToken(String s) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cancelDelegationToken(String s) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getTokenStrForm() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addToken(String s, String s1) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeToken(String s) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getToken(String s) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAllTokenIdentifiers() throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int addMasterKey(String s) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateMasterKey(Integer integer, String s) throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeMasterKey(Integer integer) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] getMasterKeys() throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createFunction(Function function) throws InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterFunction(String s, String s1, Function function)
    throws InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropFunction(String s, String s1)
    throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Function getFunction(String s, String s1) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getFunctions(String s, String s1) throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ValidTxnList getValidTxns() throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ValidTxnList getValidTxns(long l) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long openTxn(String s) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public OpenTxnsResponse openTxns(String s, int i) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollbackTxn(long l) throws NoSuchTxnException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitTxn(long l) throws NoSuchTxnException, TxnAbortedException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void abortTxns(List<Long> list) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetOpenTxnsInfoResponse showTxns() throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public LockResponse lock(LockRequest lockRequest) throws NoSuchTxnException, TxnAbortedException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public LockResponse checkLock(long l)
    throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unlock(long l) throws NoSuchLockException, TxnOpenException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ShowLocksResponse showLocks() throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void heartbeat(long l, long l1)
    throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(long l, long l1) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void compact(String s, String s1, String s2, CompactionType compactionType) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void compact(String s, String s1, String s2, CompactionType compactionType, Map<String, String> map)
    throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompactionResponse compact2(
      String s,
      String s1,
      String s2,
      CompactionType compactionType,
      Map<String, String> map)
    throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ShowCompactResponse showCompactions() throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addDynamicPartitions(long l, String s, String s1, List<String> list) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addDynamicPartitions(long l, String s, String s1, List<String> list, DataOperationType dataOperationType)
    throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void insertTable(Table table, boolean b) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public NotificationEventResponse getNextNotification(long l, int i, NotificationFilter notificationFilter)
    throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincipalsInRoleRequest)
    throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest getRoleGrantsForPrincipalRequest)
    throws MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public AggrStats getAggrColStatsFor(String s, String s1, List<String> list, List<String> list1)
    throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest setPartitionsStatsRequest)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flushCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> list) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
      List<Long> list,
      ByteBuffer byteBuffer,
      boolean b)
    throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearFileMetadata(List<Long> list) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFileMetadata(List<Long> list, List<ByteBuffer> list1) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSameConfObj(HiveConf hiveConf) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean cacheFileMetadata(String s, String s1, String s2, boolean b) throws TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest primaryKeysRequest)
    throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest)
    throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTableWithConstraints(Table table, List<SQLPrimaryKey> list, List<SQLForeignKey> list1)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropConstraint(String s, String s1, String s2) throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addPrimaryKey(List<SQLPrimaryKey> list) throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addForeignKey(List<SQLForeignKey> list) throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException();
  }
}