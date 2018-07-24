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
package com.hotels.bdp.circustrain.bigquery.metastore;

import java.io.IOException;
import java.nio.ByteBuffer;
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

import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public class BigQueryReplicaMetastoreClient implements CloseableMetaStoreClient {

  private static final Logger log = LoggerFactory.getLogger(BigQueryReplicaMetastoreClient.class);

  CloseableMetaStoreClient metaStoreClient;

  BigQueryReplicaMetastoreClient(CloseableMetaStoreClient client) throws MetaException {
    this.metaStoreClient = client;
  }

  public boolean isCompatibleWith(HiveConf hiveConf) {
    return metaStoreClient.isCompatibleWith(hiveConf);
  }

  public void setHiveAddedJars(String s) {
    metaStoreClient.setHiveAddedJars(s);
  }

  public boolean isLocalMetaStore() {
    return metaStoreClient.isLocalMetaStore();
  }

  public void reconnect() throws MetaException {
    metaStoreClient.reconnect();
  }

  public void close() {
    metaStoreClient.close();
  }

  public void setMetaConf(String s, String s1) throws MetaException, TException {
    metaStoreClient.setMetaConf(s, s1);
  }

  public String getMetaConf(String s) throws MetaException, TException {
    return metaStoreClient.getMetaConf(s);
  }

  public List<String> getDatabases(String s) throws MetaException, TException {
    return metaStoreClient.getDatabases(s);
  }

  public List<String> getAllDatabases() throws MetaException, TException {
    return metaStoreClient.getAllDatabases();
  }

  public List<String> getTables(String s, String s1) throws MetaException, TException, UnknownDBException {
    return metaStoreClient.getTables(s, s1);
  }

  public List<String> getTables(String s, String s1, TableType tableType)
    throws MetaException, TException, UnknownDBException {
    return metaStoreClient.getTables(s, s1, tableType);
  }

  public List<TableMeta> getTableMeta(String s, String s1, List<String> list)
    throws MetaException, TException, UnknownDBException {
    return metaStoreClient.getTableMeta(s, s1, list);
  }

  public List<String> getAllTables(String s) throws MetaException, TException, UnknownDBException {
    return metaStoreClient.getAllTables(s);
  }

  public List<String> listTableNamesByFilter(String s, String s1, short i)
    throws MetaException, TException, InvalidOperationException, UnknownDBException {
    return metaStoreClient.listTableNamesByFilter(s, s1, i);
  }

  public void dropTable(String s, String s1, boolean b, boolean b1)
    throws MetaException, TException, NoSuchObjectException {
    metaStoreClient.dropTable(s, s1, b, b1);
  }

  public void dropTable(String s, String s1, boolean b, boolean b1, boolean b2)
    throws MetaException, TException, NoSuchObjectException {
    metaStoreClient.dropTable(s, s1, b, b1, b2);
  }

  public void dropTable(String s, boolean b)
    throws MetaException, UnknownTableException, TException, NoSuchObjectException {
    metaStoreClient.dropTable(s, b);
  }

  public void dropTable(String s, String s1) throws MetaException, TException, NoSuchObjectException {
    metaStoreClient.dropTable(s, s1);
  }

  public boolean tableExists(String s, String s1) throws MetaException, TException, UnknownDBException {
    return metaStoreClient.tableExists(s, s1);
  }

  public boolean tableExists(String s) throws MetaException, TException, UnknownDBException {
    return metaStoreClient.tableExists(s);
  }

  public Table getTable(String s) throws MetaException, TException, NoSuchObjectException {
    return metaStoreClient.getTable(s);
  }

  public Database getDatabase(String s) throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.getDatabase(s);
  }

  public Table getTable(String s, String s1) throws MetaException, TException, NoSuchObjectException {
    return metaStoreClient.getTable(s, s1);
  }

  public List<Table> getTableObjectsByName(String s, List<String> list)
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return metaStoreClient.getTableObjectsByName(s, list);
  }

  public Partition appendPartition(String s, String s1, List<String> list)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return metaStoreClient.appendPartition(s, s1, list);
  }

  public Partition appendPartition(String s, String s1, String s2)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return metaStoreClient.appendPartition(s, s1, s2);
  }

  public Partition add_partition(Partition partition)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return metaStoreClient.add_partition(partition);
  }

  public int add_partitions(List<Partition> list)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return metaStoreClient.add_partitions(list);
  }

  public int add_partitions_pspec(PartitionSpecProxy partitionSpecProxy)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return metaStoreClient.add_partitions_pspec(partitionSpecProxy);
  }

  public List<Partition> add_partitions(List<Partition> list, boolean b, boolean b1)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return metaStoreClient.add_partitions(list, b, b1);
  }

  public Partition getPartition(String s, String s1, List<String> list)
    throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.getPartition(s, s1, list);
  }

  public Partition exchange_partition(Map<String, String> map, String s, String s1, String s2, String s3)
    throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return metaStoreClient.exchange_partition(map, s, s1, s2, s3);
  }

  public List<Partition> exchange_partitions(Map<String, String> map, String s, String s1, String s2, String s3)
    throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return metaStoreClient.exchange_partitions(map, s, s1, s2, s3);
  }

  public Partition getPartition(String s, String s1, String s2)
    throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return metaStoreClient.getPartition(s, s1, s2);
  }

  public Partition getPartitionWithAuthInfo(String s, String s1, List<String> list, String s2, List<String> list1)
    throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return metaStoreClient.getPartitionWithAuthInfo(s, s1, list, s2, list1);
  }

  public List<Partition> listPartitions(String s, String s1, short i)
    throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.listPartitions(s, s1, i);
  }

  public PartitionSpecProxy listPartitionSpecs(String s, String s1, int i) throws TException {
    return metaStoreClient.listPartitionSpecs(s, s1, i);
  }

  public List<Partition> listPartitions(String s, String s1, List<String> list, short i)
    throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.listPartitions(s, s1, list, i);
  }

  public List<String> listPartitionNames(String s, String s1, short i) throws MetaException, TException {
    return metaStoreClient.listPartitionNames(s, s1, i);
  }

  public List<String> listPartitionNames(String s, String s1, List<String> list, short i)
    throws MetaException, TException, NoSuchObjectException {
    return metaStoreClient.listPartitionNames(s, s1, list, i);
  }

  public int getNumPartitionsByFilter(String s, String s1, String s2)
    throws MetaException, NoSuchObjectException, TException {
    return metaStoreClient.getNumPartitionsByFilter(s, s1, s2);
  }

  public List<Partition> listPartitionsByFilter(String s, String s1, String s2, short i)
    throws MetaException, NoSuchObjectException, TException {
    return metaStoreClient.listPartitionsByFilter(s, s1, s2, i);
  }

  public PartitionSpecProxy listPartitionSpecsByFilter(String s, String s1, String s2, int i)
    throws MetaException, NoSuchObjectException, TException {
    return metaStoreClient.listPartitionSpecsByFilter(s, s1, s2, i);
  }

  public boolean listPartitionsByExpr(String s, String s1, byte[] bytes, String s2, short i, List<Partition> list)
    throws TException {
    return metaStoreClient.listPartitionsByExpr(s, s1, bytes, s2, i, list);
  }

  public List<Partition> listPartitionsWithAuthInfo(String s, String s1, short i, String s2, List<String> list)
    throws MetaException, TException, NoSuchObjectException {
    return metaStoreClient.listPartitionsWithAuthInfo(s, s1, i, s2, list);
  }

  public List<Partition> getPartitionsByNames(String s, String s1, List<String> list)
    throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.getPartitionsByNames(s, s1, list);
  }

  public List<Partition> listPartitionsWithAuthInfo(
      String s,
      String s1,
      List<String> list,
      short i,
      String s2,
      List<String> list1)
    throws MetaException, TException, NoSuchObjectException {
    return metaStoreClient.listPartitionsWithAuthInfo(s, s1, list, i, s2, list1);
  }

  public void markPartitionForEvent(String s, String s1, Map<String, String> map, PartitionEventType partitionEventType)
    throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
    UnknownPartitionException, InvalidPartitionException {
    metaStoreClient.markPartitionForEvent(s, s1, map, partitionEventType);
  }

  public boolean isPartitionMarkedForEvent(
      String s,
      String s1,
      Map<String, String> map,
      PartitionEventType partitionEventType)
    throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
    UnknownPartitionException, InvalidPartitionException {
    return metaStoreClient.isPartitionMarkedForEvent(s, s1, map, partitionEventType);
  }

  public void validatePartitionNameCharacters(List<String> list) throws TException, MetaException {
    metaStoreClient.validatePartitionNameCharacters(list);
  }

  public void createTable(Table table)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    metaStoreClient.createTable(table);
  }

  public void alter_table(String s, String s1, Table table)
    throws InvalidOperationException, MetaException, TException {
    metaStoreClient.alter_table(s, s1, table);
  }

  public void alter_table(String s, String s1, Table table, boolean b)
    throws InvalidOperationException, MetaException, TException {
    metaStoreClient.alter_table(s, s1, table, b);
  }

  public void alter_table_with_environmentContext(
      String s,
      String s1,
      Table table,
      EnvironmentContext environmentContext)
    throws InvalidOperationException, MetaException, TException {
    metaStoreClient.alter_table_with_environmentContext(s, s1, table, environmentContext);
  }

  public void createDatabase(Database database)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    metaStoreClient.createDatabase(database);
  }

  public void dropDatabase(String s)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    metaStoreClient.dropDatabase(s);
  }

  public void dropDatabase(String s, boolean b, boolean b1)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    metaStoreClient.dropDatabase(s, b, b1);
  }

  public void dropDatabase(String s, boolean b, boolean b1, boolean b2)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    metaStoreClient.dropDatabase(s, b, b1, b2);
  }

  public void alterDatabase(String s, Database database) throws NoSuchObjectException, MetaException, TException {
    metaStoreClient.alterDatabase(s, database);
  }

  public boolean dropPartition(String s, String s1, List<String> list, boolean b)
    throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.dropPartition(s, s1, list, b);
  }

  public boolean dropPartition(String s, String s1, List<String> list, PartitionDropOptions partitionDropOptions)
    throws TException {
    return metaStoreClient.dropPartition(s, s1, list, partitionDropOptions);
  }

  public List<Partition> dropPartitions(
      String s,
      String s1,
      List<ObjectPair<Integer, byte[]>> list,
      boolean b,
      boolean b1)
    throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.dropPartitions(s, s1, list, b, b1);
  }

  public List<Partition> dropPartitions(
      String s,
      String s1,
      List<ObjectPair<Integer, byte[]>> list,
      boolean b,
      boolean b1,
      boolean b2)
    throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.dropPartitions(s, s1, list, b, b1, b2);
  }

  public List<Partition> dropPartitions(
      String s,
      String s1,
      List<ObjectPair<Integer, byte[]>> list,
      PartitionDropOptions partitionDropOptions)
    throws TException {
    return metaStoreClient.dropPartitions(s, s1, list, partitionDropOptions);
  }

  public boolean dropPartition(String s, String s1, String s2, boolean b)
    throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.dropPartition(s, s1, s2, b);
  }

  public void alter_partition(String s, String s1, Partition partition)
    throws InvalidOperationException, MetaException, TException {
    metaStoreClient.alter_partition(s, s1, partition);
  }

  public void alter_partition(String s, String s1, Partition partition, EnvironmentContext environmentContext)
    throws InvalidOperationException, MetaException, TException {
    metaStoreClient.alter_partition(s, s1, partition, environmentContext);
  }

  public void alter_partitions(String dbName, String tableName, List<Partition> partitions)
    throws InvalidOperationException, MetaException, TException {
    for (Partition partition : partitions) {
      log.info("\n");
      StringBuilder sb = new StringBuilder();

      for (int i = 0; i < partition.getValues().size(); ++i) {
        partition.getValues().set(i, partition.getValues().get(i).trim());
        sb.append(partition.getValues().get(i)).append("  ");
      }
      log.info("Partition = {}", partition.toString());
      log.info("Partition SD = {}", partition.getSd().toString());
      log.info("Values = {}", sb.toString());
      log.info("Location = {}\n", partition.getSd().getLocation());
    }
    metaStoreClient.alter_partitions(dbName, tableName, partitions);
  }

  public void alter_partitions(
      String dbName,
      String tableName,
      List<Partition> partitions,
      EnvironmentContext environmentContext)
    throws InvalidOperationException, MetaException, TException {
    for (Partition partition : partitions) {
      log.info("\n\n");
      StringBuilder sb = new StringBuilder();

      for (int i = 0; i < partition.getValues().size(); ++i) {
        partition.getValues().set(i, partition.getValues().get(i).trim());
        sb.append(partition.getValues().get(i)).append("  ");
      }
      log.info("Partition = {}\n", partition.toString());
      log.info("Partition SD = {}\n", partition.getSd().toString());
      log.info("Values = {}\n", sb.toString());
      log.info("Location = {}\n", partition.getSd().getLocation());
    }

    metaStoreClient.alter_partitions(dbName, tableName, partitions, environmentContext);
  }

  public void renamePartition(String s, String s1, List<String> list, Partition partition)
    throws InvalidOperationException, MetaException, TException {
    metaStoreClient.renamePartition(s, s1, list, partition);
  }

  public List<FieldSchema> getFields(String s, String s1)
    throws MetaException, TException, UnknownTableException, UnknownDBException {
    return metaStoreClient.getFields(s, s1);
  }

  public List<FieldSchema> getSchema(String s, String s1)
    throws MetaException, TException, UnknownTableException, UnknownDBException {
    return metaStoreClient.getSchema(s, s1);
  }

  public String getConfigValue(String s, String s1) throws TException, ConfigValSecurityException {
    return metaStoreClient.getConfigValue(s, s1);
  }

  public List<String> partitionNameToVals(String s) throws MetaException, TException {
    return metaStoreClient.partitionNameToVals(s);
  }

  public Map<String, String> partitionNameToSpec(String s) throws MetaException, TException {
    return metaStoreClient.partitionNameToSpec(s);
  }

  public void createIndex(Index index, Table table)
    throws InvalidObjectException, MetaException, NoSuchObjectException, TException, AlreadyExistsException {
    metaStoreClient.createIndex(index, table);
  }

  public void alter_index(String s, String s1, String s2, Index index)
    throws InvalidOperationException, MetaException, TException {
    metaStoreClient.alter_index(s, s1, s2, index);
  }

  public Index getIndex(String s, String s1, String s2)
    throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return metaStoreClient.getIndex(s, s1, s2);
  }

  public List<Index> listIndexes(String s, String s1, short i) throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.listIndexes(s, s1, i);
  }

  public List<String> listIndexNames(String s, String s1, short i) throws MetaException, TException {
    return metaStoreClient.listIndexNames(s, s1, i);
  }

  public boolean dropIndex(String s, String s1, String s2, boolean b)
    throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.dropIndex(s, s1, s2, b);
  }

  public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
    return metaStoreClient.updateTableColumnStatistics(columnStatistics);
  }

  public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
    return metaStoreClient.updatePartitionColumnStatistics(columnStatistics);
  }

  public List<ColumnStatisticsObj> getTableColumnStatistics(String s, String s1, List<String> list)
    throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.getTableColumnStatistics(s, s1, list);
  }

  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String s,
      String s1,
      List<String> list,
      List<String> list1)
    throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.getPartitionColumnStatistics(s, s1, list, list1);
  }

  public boolean deletePartitionColumnStatistics(String s, String s1, String s2, String s3)
    throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    return metaStoreClient.deletePartitionColumnStatistics(s, s1, s2, s3);
  }

  public boolean deleteTableColumnStatistics(String s, String s1, String s2)
    throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    return metaStoreClient.deleteTableColumnStatistics(s, s1, s2);
  }

  public boolean create_role(Role role) throws MetaException, TException {
    return metaStoreClient.create_role(role);
  }

  public boolean drop_role(String s) throws MetaException, TException {
    return metaStoreClient.drop_role(s);
  }

  public List<String> listRoleNames() throws MetaException, TException {
    return metaStoreClient.listRoleNames();
  }

  public boolean grant_role(
      String s,
      String s1,
      PrincipalType principalType,
      String s2,
      PrincipalType principalType1,
      boolean b)
    throws MetaException, TException {
    return metaStoreClient.grant_role(s, s1, principalType, s2, principalType1, b);
  }

  public boolean revoke_role(String s, String s1, PrincipalType principalType, boolean b)
    throws MetaException, TException {
    return metaStoreClient.revoke_role(s, s1, principalType, b);
  }

  public List<Role> list_roles(String s, PrincipalType principalType) throws MetaException, TException {
    return metaStoreClient.list_roles(s, principalType);
  }

  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObjectRef, String s, List<String> list)
    throws MetaException, TException {
    return metaStoreClient.get_privilege_set(hiveObjectRef, s, list);
  }

  public List<HiveObjectPrivilege> list_privileges(String s, PrincipalType principalType, HiveObjectRef hiveObjectRef)
    throws MetaException, TException {
    return metaStoreClient.list_privileges(s, principalType, hiveObjectRef);
  }

  public boolean grant_privileges(PrivilegeBag privilegeBag) throws MetaException, TException {
    return metaStoreClient.grant_privileges(privilegeBag);
  }

  public boolean revoke_privileges(PrivilegeBag privilegeBag, boolean b) throws MetaException, TException {
    return metaStoreClient.revoke_privileges(privilegeBag, b);
  }

  public String getDelegationToken(String s, String s1) throws MetaException, TException {
    return metaStoreClient.getDelegationToken(s, s1);
  }

  public long renewDelegationToken(String s) throws MetaException, TException {
    return metaStoreClient.renewDelegationToken(s);
  }

  public void cancelDelegationToken(String s) throws MetaException, TException {
    metaStoreClient.cancelDelegationToken(s);
  }

  public String getTokenStrForm() throws IOException {
    return metaStoreClient.getTokenStrForm();
  }

  public boolean addToken(String s, String s1) throws TException {
    return metaStoreClient.addToken(s, s1);
  }

  public boolean removeToken(String s) throws TException {
    return metaStoreClient.removeToken(s);
  }

  public String getToken(String s) throws TException {
    return metaStoreClient.getToken(s);
  }

  public List<String> getAllTokenIdentifiers() throws TException {
    return metaStoreClient.getAllTokenIdentifiers();
  }

  public int addMasterKey(String s) throws MetaException, TException {
    return metaStoreClient.addMasterKey(s);
  }

  public void updateMasterKey(Integer integer, String s) throws NoSuchObjectException, MetaException, TException {
    metaStoreClient.updateMasterKey(integer, s);
  }

  public boolean removeMasterKey(Integer integer) throws TException {
    return metaStoreClient.removeMasterKey(integer);
  }

  public String[] getMasterKeys() throws TException {
    return metaStoreClient.getMasterKeys();
  }

  public void createFunction(Function function) throws InvalidObjectException, MetaException, TException {
    metaStoreClient.createFunction(function);
  }

  public void alterFunction(String s, String s1, Function function)
    throws InvalidObjectException, MetaException, TException {
    metaStoreClient.alterFunction(s, s1, function);
  }

  public void dropFunction(String s, String s1)
    throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    metaStoreClient.dropFunction(s, s1);
  }

  public Function getFunction(String s, String s1) throws MetaException, TException {
    return metaStoreClient.getFunction(s, s1);
  }

  public List<String> getFunctions(String s, String s1) throws MetaException, TException {
    return metaStoreClient.getFunctions(s, s1);
  }

  public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
    return metaStoreClient.getAllFunctions();
  }

  public ValidTxnList getValidTxns() throws TException {
    return metaStoreClient.getValidTxns();
  }

  public ValidTxnList getValidTxns(long l) throws TException {
    return metaStoreClient.getValidTxns(l);
  }

  public long openTxn(String s) throws TException {
    return metaStoreClient.openTxn(s);
  }

  public OpenTxnsResponse openTxns(String s, int i) throws TException {
    return metaStoreClient.openTxns(s, i);
  }

  public void rollbackTxn(long l) throws NoSuchTxnException, TException {
    metaStoreClient.rollbackTxn(l);
  }

  public void commitTxn(long l) throws NoSuchTxnException, TxnAbortedException, TException {
    metaStoreClient.commitTxn(l);
  }

  public void abortTxns(List<Long> list) throws TException {
    metaStoreClient.abortTxns(list);
  }

  public GetOpenTxnsInfoResponse showTxns() throws TException {
    return metaStoreClient.showTxns();
  }

  public LockResponse lock(LockRequest lockRequest) throws NoSuchTxnException, TxnAbortedException, TException {
    return metaStoreClient.lock(lockRequest);
  }

  public LockResponse checkLock(long l)
    throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    return metaStoreClient.checkLock(l);
  }

  public void unlock(long l) throws NoSuchLockException, TxnOpenException, TException {
    metaStoreClient.unlock(l);
  }

  public ShowLocksResponse showLocks() throws TException {
    return metaStoreClient.showLocks();
  }

  public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
    return metaStoreClient.showLocks(showLocksRequest);
  }

  public void heartbeat(long l, long l1)
    throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    metaStoreClient.heartbeat(l, l1);
  }

  public HeartbeatTxnRangeResponse heartbeatTxnRange(long l, long l1) throws TException {
    return metaStoreClient.heartbeatTxnRange(l, l1);
  }

  public void compact(String s, String s1, String s2, CompactionType compactionType) throws TException {
    metaStoreClient.compact(s, s1, s2, compactionType);
  }

  public void compact(String s, String s1, String s2, CompactionType compactionType, Map<String, String> map)
    throws TException {
    metaStoreClient.compact(s, s1, s2, compactionType, map);
  }

  public CompactionResponse compact2(
      String s,
      String s1,
      String s2,
      CompactionType compactionType,
      Map<String, String> map)
    throws TException {
    return metaStoreClient.compact2(s, s1, s2, compactionType, map);
  }

  public ShowCompactResponse showCompactions() throws TException {
    return metaStoreClient.showCompactions();
  }

  public void addDynamicPartitions(long l, String s, String s1, List<String> list) throws TException {
    metaStoreClient.addDynamicPartitions(l, s, s1, list);
  }

  public void addDynamicPartitions(long l, String s, String s1, List<String> list, DataOperationType dataOperationType)
    throws TException {
    metaStoreClient.addDynamicPartitions(l, s, s1, list, dataOperationType);
  }

  public void insertTable(Table table, boolean b) throws MetaException {
    metaStoreClient.insertTable(table, b);
  }

  public NotificationEventResponse getNextNotification(long l, int i, NotificationFilter notificationFilter)
    throws TException {
    return metaStoreClient.getNextNotification(l, i, notificationFilter);
  }

  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    return metaStoreClient.getCurrentNotificationEventId();
  }

  public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
    return metaStoreClient.fireListenerEvent(fireEventRequest);
  }

  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincipalsInRoleRequest)
    throws MetaException, TException {
    return metaStoreClient.get_principals_in_role(getPrincipalsInRoleRequest);
  }

  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest getRoleGrantsForPrincipalRequest)
    throws MetaException, TException {
    return metaStoreClient.get_role_grants_for_principal(getRoleGrantsForPrincipalRequest);
  }

  public AggrStats getAggrColStatsFor(String s, String s1, List<String> list, List<String> list1)
    throws NoSuchObjectException, MetaException, TException {
    return metaStoreClient.getAggrColStatsFor(s, s1, list, list1);
  }

  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest setPartitionsStatsRequest)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
    log.info("Setting partition column statistics to: {}", setPartitionsStatsRequest.toString());
    return metaStoreClient.setPartitionColumnStatistics(setPartitionsStatsRequest);
  }

  public void flushCache() {
    metaStoreClient.flushCache();
  }

  public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> list) throws TException {
    return metaStoreClient.getFileMetadata(list);
  }

  public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
      List<Long> list,
      ByteBuffer byteBuffer,
      boolean b)
    throws TException {
    return metaStoreClient.getFileMetadataBySarg(list, byteBuffer, b);
  }

  public void clearFileMetadata(List<Long> list) throws TException {
    metaStoreClient.clearFileMetadata(list);
  }

  public void putFileMetadata(List<Long> list, List<ByteBuffer> list1) throws TException {
    metaStoreClient.putFileMetadata(list, list1);
  }

  public boolean isSameConfObj(HiveConf hiveConf) {
    return metaStoreClient.isSameConfObj(hiveConf);
  }

  public boolean cacheFileMetadata(String s, String s1, String s2, boolean b) throws TException {
    return metaStoreClient.cacheFileMetadata(s, s1, s2, b);
  }

  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest primaryKeysRequest)
    throws MetaException, NoSuchObjectException, TException {
    return metaStoreClient.getPrimaryKeys(primaryKeysRequest);
  }

  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest)
    throws MetaException, NoSuchObjectException, TException {
    return metaStoreClient.getForeignKeys(foreignKeysRequest);
  }

  public void createTableWithConstraints(Table table, List<SQLPrimaryKey> list, List<SQLForeignKey> list1)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    metaStoreClient.createTableWithConstraints(table, list, list1);
  }

  public void dropConstraint(String s, String s1, String s2) throws MetaException, NoSuchObjectException, TException {
    metaStoreClient.dropConstraint(s, s1, s2);
  }

  public void addPrimaryKey(List<SQLPrimaryKey> list) throws MetaException, NoSuchObjectException, TException {
    metaStoreClient.addPrimaryKey(list);
  }

  public void addForeignKey(List<SQLForeignKey> list) throws MetaException, NoSuchObjectException, TException {
    metaStoreClient.addForeignKey(list);
  }

  public boolean isOpen() {
    return metaStoreClient.isOpen();
  }
}
