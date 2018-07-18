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

import static com.hotels.bdp.circustrain.bigquery.extraction.BigQueryDataExtractionKey.makeKey;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang.StringUtils.isNotEmpty;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
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
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
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
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.context.CircusTrainBigQueryConfiguration;
import com.hotels.bdp.circustrain.bigquery.conversion.BigQueryToHivePartitionConverter;
import com.hotels.bdp.circustrain.bigquery.conversion.BigQueryToHiveTableConverter;
import com.hotels.bdp.circustrain.bigquery.conversion.BigQueryToHiveTypeConverter;
import com.hotels.bdp.circustrain.bigquery.extraction.BigQueryDataExtractionManager;
import com.hotels.bdp.circustrain.bigquery.extraction.BigQueryExtractionData;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

class BigQueryMetastoreClient implements CloseableMetaStoreClient {

  private static final Logger log = LoggerFactory.getLogger(BigQueryMetastoreClient.class);
  private static final String PART_KEY = "part";
  private static final String BOOLEAN_COL = "boolCol";
  private static final String BOOLEAN_TYPE = "boolean";
  private static final String BOOLEAN_VAL = "true";
  private static final String LONG_COL = "longCol";
  private static final String LONG_TYPE = "long";
  private static final String INT_TYPE = "int";
  private static final String INT_VAL = "1234";
  private static final String DOUBLE_COL = "doubleCol";
  private static final String DOUBLE_TYPE = "double";
  private static final String DOUBLE_VAL = "3.1415";
  private static final String STRING_COL = "stringCol";
  private static final String STRING_TYPE = "string";
  private static final String STRING_VAL = "stringval";
  private static final String BINARY_COL = "binaryCol";
  private static final String BINARY_TYPE = "binary";
  private static final String BINARY_VAL = "1";
  private static final String DECIMAL_COL = "decimalCol";
  private static final String DECIMAL_TYPE = "decimal(5,3)";
  private static final String DECIMAL_VAL = "12.123";

  private final CircusTrainBigQueryConfiguration circusTrainBigQueryConfiguration;
  private final BigQuery bigQuery;
  private final BigQueryDataExtractionManager dataExtractionManager;
  private final Map<String, Table> tableCache = new HashMap<>();
  private final Map<String, List<Partition>> partitionCache = new HashMap<>();

  BigQueryMetastoreClient(
      CircusTrainBigQueryConfiguration circusTrainBigQueryConfiguration,
      BigQuery bigQuery,
      BigQueryDataExtractionManager dataExtractionManager) {
    this.circusTrainBigQueryConfiguration = circusTrainBigQueryConfiguration;
    this.bigQuery = bigQuery;
    this.dataExtractionManager = dataExtractionManager;
  }

  private void checkDbExists(String databaseName) throws UnknownDBException {
    if (bigQuery.getDataset(databaseName) == null) {
      throw new UnknownDBException("Dataset " + databaseName + " doesn't exist in BigQuery");
    }
  }

  private String getPartitionFilter() {
    if (circusTrainBigQueryConfiguration == null || circusTrainBigQueryConfiguration.getPartitionFilter() == null) {
      return null;
    }
    return circusTrainBigQueryConfiguration.getPartitionFilter().trim().toLowerCase();
  }

  private String getPartitionBy() {
    if (circusTrainBigQueryConfiguration == null || circusTrainBigQueryConfiguration.getPartitionBy() == null) {
      return null;
    }
    return circusTrainBigQueryConfiguration.getPartitionBy().trim().toLowerCase();
  }

  @Override
  public Database getDatabase(String databaseName) throws TException {
    log.info("Getting database {} from BigQuery", databaseName);
    checkDbExists(databaseName);
    return new Database(databaseName, null, null, null);
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) throws TException {
    checkDbExists(databaseName);
    return bigQuery.getDataset(databaseName).get(tableName) != null;
  }

  @Override
  public Table getTable(String databaseName, String tableName) throws TException {
    String tableKey = makeKey(databaseName, tableName);
    if (tableCache.containsKey(tableKey)) {
      log.info("Loading table {}.{} from tableCache", databaseName, tableName);
      return tableCache.get(tableKey);
    }
    log.info("Getting table {}.{} from BigQuery", databaseName, tableName);
    checkDbExists(databaseName);
    com.google.cloud.bigquery.Table bigQueryTable = getBigQueryTable(databaseName, tableName);
    dataExtractionManager.register(bigQueryTable);
    Table hiveTable = convertBigQueryTableToHiveTable(bigQueryTable);
    addPartitionIfFiltered(hiveTable);
    cacheTable(hiveTable);
    return hiveTable;
  }

  private boolean shouldPartition() {
    return isNotBlank(getPartitionBy());
  }

  private void addPartitionIfFiltered(Table table) {
    if (shouldPartition()) {
      log.info("Partitioning table {}.{} with key '{}' and filter '{}'", table.getDbName(), table.getTableName(),
          getPartitionBy(), getPartitionFilter());
      applyPartitionFilter(table);
    } else {
      log.info("Partitioning not configured for table {}.{}. No filter applied", table.getDbName(),
          table.getTableName());
    }
  }

  private void cacheTable(Table table) {
    tableCache.put(makeKey(table.getDbName(), table.getTableName()), table);
  }

  private void cachePartition(Partition partition) {
    log.debug("Caching partition {}", partition);
    String partitionKey = makeKey(partition.getDbName(), partition.getTableName());

    if (partitionCache.containsKey(partitionKey)) {
      partitionCache.get(partitionKey).add(partition);
    } else {
      List<Partition> partitions = new ArrayList<>();
      partitions.add(partition);
      partitionCache.put(partitionKey, partitions);
    }
  }

  private com.google.cloud.bigquery.Table getBigQueryTableHelper(String databaseName, String tableName)
    throws NoSuchObjectException {
    com.google.cloud.bigquery.Table table = bigQuery.getDataset(databaseName).get(tableName);
    if (table == null) {
      throw new NoSuchObjectException(databaseName + "." + tableName + " could not be found");
    }
    return table;
  }

  private com.google.cloud.bigquery.Table getBigQueryTable(String databaseName, String tableName) {
    try {
      return getBigQueryTableHelper(databaseName, tableName);
    } catch (NoSuchObjectException e) {
      throw new CircusTrainException(e);
    }
  }

  private Table convertBigQueryTableToHiveTable(com.google.cloud.bigquery.Table table) {
    final String databaseName = table.getTableId().getDataset();
    final String tableName = table.getTableId().getTable();
    final String tableLocation = dataExtractionManager.getExtractedDataBaseLocation(table);
    final Schema schema = table.getDefinition().getSchema();
    final List<FieldSchema> cols = Collections.unmodifiableList(getCols(table));
    return new BigQueryToHiveTableConverter()
        .withDatabaseName(databaseName)
        .withTableName(tableName)
        .withSchema(schema)
        .withCols(cols)
        .withLocation(tableLocation)
        .convert();
  }

  private String getSelectStatment(Table hiveTable) {
    final String partitionBy = getPartitionBy();
    final String partitionFilter = getPartitionFilter();
    if (isNotBlank(partitionBy) && isNotBlank(partitionFilter)) {
      return String.format("select * from %s.%s where %s", hiveTable.getDbName(), hiveTable.getTableName(),
          getPartitionFilter());
    } else if (isNotBlank(partitionBy) && isBlank(partitionFilter)) {
      return String.format("select * from %s.%s", hiveTable.getDbName(), hiveTable.getTableName());
    } else {
      throw new IllegalStateException();
    }
  }

  private void applyPartitionFilter(Table hiveTable) {
    String selectStatement = getSelectStatment(hiveTable);
    String datasetName = hiveTable.getDbName();
    String tableName = randomTableName();
    TableResult result = selectQueryIntoBigQueryTable(datasetName, tableName, selectStatement);
    com.google.cloud.bigquery.Table filteredTable = getBigQueryTable(datasetName, tableName);
    dataExtractionManager.register(filteredTable);
    addPartitionKeys(hiveTable, filteredTable.getDefinition().getSchema());
    generateHivePartitions(hiveTable, filteredTable, result);
  }

  private String randomTableName() {
    return UUID.randomUUID().toString().replaceAll("-", "_");
  }

  private String sanitisePartitionKey(Schema schema) {
    // Case sensitive in Google Cloud
    String partitionKey = getPartitionBy();
    for (Field field : schema.getFields()) {
      if (field.getName().toLowerCase().equals(partitionKey.toLowerCase())) {
        partitionKey = field.getName();
        break;
      }
    }
    return partitionKey;
  }

  private void generateHivePartitions(
      Table hiveTable,
      com.google.cloud.bigquery.Table filteredTable,
      TableResult result) {
    Schema schema = filteredTable.getDefinition().getSchema();
    if (schema == null) {
      return;
    }

    String partitionKey = sanitisePartitionKey(schema);
    log.info("Getting values for partition key '{}'", partitionKey);
    Set<String> values = new LinkedHashSet<>();

    for (FieldValueList row : result.iterateAll()) {
      values.add(row.get(partitionKey).getValue().toString());
    }

    log.info("Generating Hive partitions");
    Map<com.google.cloud.bigquery.Table, String> partitionMap = new LinkedHashMap<>();
    final String sourceDBName = filteredTable.getTableId().getDataset();
    final String sourceTableName = filteredTable.getTableId().getTable();
    final String destinationDBName = hiveTable.getDbName();

    // TODO: Cache BQ tables
    com.google.cloud.bigquery.Table bigQueryRepresentation = getBigQueryTable(hiveTable.getDbName(),
        hiveTable.getTableName());

    final String tableBucket = dataExtractionManager.getExtractedDataBucket(bigQueryRepresentation);
    final String tableFolder = dataExtractionManager.getExtractedDataFolder(bigQueryRepresentation);

    for (String value : values) {
      log.info("Generated partition value '{}' for key '{}'", value, partitionKey);
      String statement = String.format("select * from %s.%s where %s = %s", sourceDBName, sourceTableName, partitionKey,
          value);
      String destinationTableName = randomTableName();
      selectQueryIntoBigQueryTable(destinationDBName, destinationTableName, statement);
      com.google.cloud.bigquery.Table part = getBigQueryTable(destinationDBName, destinationTableName);
      String partitionBucket = tableBucket;
      String partitionFolder = tableFolder + "/" + BigQueryExtractionData.randomUri();
      String fileName = partitionKey + "=" + value;
      BigQueryExtractionData extractionData = new BigQueryExtractionData(partitionBucket, partitionFolder, fileName);
      dataExtractionManager.register(part, extractionData, true);
      partitionMap.put(part, value);
    }

    List<FieldSchema> cols = Collections.unmodifiableList(hiveTable.getSd().getCols());
    for (Map.Entry<com.google.cloud.bigquery.Table, String> entry : partitionMap.entrySet()) {
      com.google.cloud.bigquery.Table part = entry.getKey();
      String value = entry.getValue();
      Partition partition = new BigQueryToHivePartitionConverter()
          .withDatabaseName(hiveTable.getDbName())
          .withTableName(hiveTable.getTableName())
          .withValue(value)
          .withCols(cols)
          .withLocation(dataExtractionManager.getExtractedDataBaseLocation(part))
          .convert();
      log.info("Generated partition {}", partition);
      cachePartition(partition);
      // cachePartition(partitionName, partition);
    }
  }

  private List<FieldSchema> getCols(com.google.cloud.bigquery.Table table) {
    BigQueryToHiveTypeConverter typeConverter = new BigQueryToHiveTypeConverter();
    List<FieldSchema> partitionKeys = new ArrayList<>();

    for (Field field : table.getDefinition().getSchema().getFields()) {
      String fieldName = field.getName().toLowerCase();
      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setName(fieldName);
      fieldSchema.setType(typeConverter.convert(field.getType().toString()).toLowerCase());
      partitionKeys.add(fieldSchema);
    }
    return partitionKeys;
  }

  private void addPartitionKeys(Table table, Schema filteredTableSchema) {
    if (filteredTableSchema == null) {
      return;
    }
    BigQueryToHiveTypeConverter typeConverter = new BigQueryToHiveTypeConverter();
    List<FieldSchema> partitionKeys = new ArrayList<>();
    String partitionKey = getPartitionBy().toLowerCase();
    for (Field field : filteredTableSchema.getFields()) {
      String fieldName = field.getName().toLowerCase();
      if (partitionKey.equals(fieldName)) {
        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.setName(fieldName);
        fieldSchema.setType(typeConverter.convert(field.getType().toString()).toLowerCase());
        partitionKeys.add(fieldSchema);
      }
    }
    table.setPartitionKeys(partitionKeys);
    log.info("Added partition keys: {} to replication table object {}.{}", table.getPartitionKeys(), table.getDbName(),
        table.getTableName());
  }

  private TableResult selectQueryIntoBigQueryTable(String databaseName, String tableName, String partitionFilter) {
    log.debug("Executing '{}'", partitionFilter);
    return executeJob(configureFilterJob(databaseName, tableName, partitionFilter));
  }

  private QueryJobConfiguration configureFilterJob(String databaseName, String tableName, String partitionFilter) {
    return QueryJobConfiguration
        .newBuilder(partitionFilter)
        .setDestinationTable(TableId.of(databaseName, tableName))
        .setUseLegacySql(true)
        .setAllowLargeResults(true)
        .build();
  }

  private TableResult executeJob(QueryJobConfiguration configuration) {
    try {
      JobId jobId = JobId.of(UUID.randomUUID().toString());
      Job queryJob = bigQuery.create(JobInfo.newBuilder(configuration).setJobId(jobId).build());
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

  @Override
  public List<Partition> listPartitions(String dbName, String tblName, short max)
    throws NoSuchObjectException, MetaException, TException {
    log.info("Getting cached partitions for {}.{}", dbName, tblName);
    String key = makeKey(dbName, tblName);
    List<Partition> partitions = partitionCache.get(key);
    log.info("Fetched {} partition(s)", partitions.size());
    return partitions;
  }

  private boolean validFieldSchema(FieldSchema fieldSchema) {
    return isNotEmpty(fieldSchema.getName().trim()) && isNotEmpty(fieldSchema.getType().trim());
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String dbName,
      String tableName,
      List<String> partNames,
      List<String> colNames)
    throws NoSuchObjectException, MetaException, TException {
    // TODO: Revisit logic for supporting multiple partitions
    // TODO: Use cached table
    // TODO: Get working

    Map<String, List<ColumnStatisticsObj>> map = new HashMap<>();
    List<ColumnStatisticsObj> statObjects = getTableColumnStatistics(dbName, tableName, colNames);
    for (String partName : partNames) {
      log.info("Setting statistics objects for partition {}", partName);
      map.put(partName, statObjects);
    }
    return map;

  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames)
    throws NoSuchObjectException, MetaException, TException {
    com.google.cloud.bigquery.Table table = getBigQueryTable(dbName, tableName);
    List<FieldSchema> cols = getCols(table);
    List<ColumnStatisticsObj> statObjects = new ArrayList<>();
    for (FieldSchema fieldSchema : cols) {
      String type = fieldSchema.getType();
      switch (type) {
        case BOOLEAN_TYPE :
          statObjects.add(mockBooleanStats(10));
          break;
        case LONG_TYPE :
          statObjects.add(mockLongStats(10));
          break;
        case STRING_TYPE:
          statObjects.add(mockStringStats(10));
          break;
        case DOUBLE_TYPE:
          statObjects.add(mockDoubleStats(10));
          break;
        case BINARY_TYPE:
          statObjects.add(mockBinaryStats(10));
          break;
        case DECIMAL_TYPE:
          statObjects.add(mockDecimalStats(10));
          break;
        default:
          break;
      }
    }
    return statObjects;
  }



  private ColumnStatisticsObj mockBooleanStats(int i) {
    long trues = 37 + 100*i;
    long falses = 12 + 50*i;
    long nulls = 2 + i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(BOOLEAN_COL);
    colStatsObj.setColType(BOOLEAN_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    BooleanColumnStatsData boolData = new BooleanColumnStatsData();
    boolData.setNumTrues(trues);
    boolData.setNumFalses(falses);
    boolData.setNumNulls(nulls);
    data.setBooleanStats(boolData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private ColumnStatisticsObj mockLongStats(int i) {
    long high = 120938479124L + 100*i;
    long low = -12341243213412124L - 50*i;
    long nulls = 23 + i;
    long dVs = 213L + 10*i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(LONG_COL);
    colStatsObj.setColType(LONG_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    LongColumnStatsData longData = new LongColumnStatsData();
    longData.setHighValue(high);
    longData.setLowValue(low);
    longData.setNumNulls(nulls);
    longData.setNumDVs(dVs);
    data.setLongStats(longData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private ColumnStatisticsObj mockDoubleStats(int i) {
    double high = 123423.23423 + 100*i;
    double low = 0.00001234233 - 50*i;
    long nulls = 92 + i;
    long dVs = 1234123421L + 10*i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(DOUBLE_COL);
    colStatsObj.setColType(DOUBLE_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
    doubleData.setHighValue(high);
    doubleData.setLowValue(low);
    doubleData.setNumNulls(nulls);
    doubleData.setNumDVs(dVs);
    data.setDoubleStats(doubleData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private ColumnStatisticsObj mockStringStats(int i) {
    long maxLen = 1234 + 10*i;
    double avgLen = 32.3 + i;
    long nulls = 987 + 10*i;
    long dVs = 906 + i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(STRING_COL);
    colStatsObj.setColType(STRING_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    StringColumnStatsData stringData = new StringColumnStatsData();
    stringData.setMaxColLen(maxLen);
    stringData.setAvgColLen(avgLen);
    stringData.setNumNulls(nulls);
    stringData.setNumDVs(dVs);
    data.setStringStats(stringData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private ColumnStatisticsObj mockBinaryStats(int i) {;
    long maxLen = 123412987L + 10*i;
    double avgLen = 76.98 + i;
    long nulls = 976998797L + 10*i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(BINARY_COL);
    colStatsObj.setColType(BINARY_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    BinaryColumnStatsData binaryData = new BinaryColumnStatsData();
    binaryData.setMaxColLen(maxLen);
    binaryData.setAvgColLen(avgLen);
    binaryData.setNumNulls(nulls);
    data.setBinaryStats(binaryData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private ColumnStatisticsObj mockDecimalStats(int i) {
    Decimal high = new Decimal();
    high.setScale((short)3);
    String strHigh = String.valueOf(3876 + 100*i);
    high.setUnscaled(strHigh.getBytes());
    Decimal low = new Decimal();
    low.setScale((short)3);
    String strLow = String.valueOf(38 + i);
    low.setUnscaled(strLow.getBytes());
    long nulls = 13 + i;
    long dVs = 923947293L + 100*i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(DECIMAL_COL);
    colStatsObj.setColType(DECIMAL_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    DecimalColumnStatsData decimalData = new DecimalColumnStatsData();
    decimalData.setHighValue(high);
    decimalData.setLowValue(low);
    decimalData.setNumNulls(nulls);
    decimalData.setNumDVs(dVs);
    data.setDecimalStats(decimalData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  @Override
  public void close() {
    // Do Nothing
  }

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public void alter_table(String s, String s1, Table table)
    throws InvalidOperationException, MetaException, TException {
    // Ignore
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
    return 0;
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
    return 0;
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
    return 0;
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
    return 0;
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
    return new String[0];
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
    return 0;
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
