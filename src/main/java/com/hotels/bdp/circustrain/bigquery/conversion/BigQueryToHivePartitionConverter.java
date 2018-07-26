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
package com.hotels.bdp.circustrain.bigquery.conversion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import com.google.cloud.bigquery.Schema;

public class BigQueryToHivePartitionConverter {

  private Partition partition = new Partition();

  public BigQueryToHivePartitionConverter() {
    partition.setDbName("default");
    partition.setTableName("default");
    partition.setValues(new ArrayList<String>());
    partition.setLastAccessTime(0);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("");
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());
    sd.setNumBuckets(-1);
    sd.setBucketCols(new ArrayList<String>());
    sd.setCols(new ArrayList<FieldSchema>());
    sd.setParameters(new HashMap<String, String>());
    sd.setSortCols(new ArrayList<Order>());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    sd.setCompressed(false);
    sd.setStoredAsSubDirectories(false);
    sd.setNumBuckets(-1);
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    Map<String, String> serDeParameters = new HashMap<>();
    serDeParameters.put("serialization.format", ",");
    serDeParameters.put("field.delim", ",");
    serDeInfo.setParameters(serDeParameters);
    sd.setSerdeInfo(serDeInfo);
    SkewedInfo si = new SkewedInfo();
    si.setSkewedColNames(new ArrayList<String>());
    si.setSkewedColValueLocationMaps(new HashMap<List<String>, String>());
    si.setSkewedColValues(new ArrayList<List<String>>());
    sd.setSkewedInfo(new SkewedInfo());
    partition.setSd(sd);
  }

  public Partition convert() {
    return new Partition(partition);
  }

  public BigQueryToHivePartitionConverter withDatabaseName(String dbName) {
    partition.setDbName(dbName);
    return this;
  }

  public BigQueryToHivePartitionConverter withTableName(String tableName) {
    partition.setTableName(tableName);
    return this;
  }

  public BigQueryToHivePartitionConverter withLocation(String location) {
    partition.getSd().setLocation(location);
    return this;
  }

  public BigQueryToHivePartitionConverter withValues(List<String> values) {
    partition.setValues(values);
    return this;
  }

  public BigQueryToHivePartitionConverter withCols(Schema schema) {
    return this.withCols(BigQueryToHiveConversionUtils.getCols(schema));
  }

  public BigQueryToHivePartitionConverter withCols(List<FieldSchema> cols) {
    partition.getSd().setCols(cols);
    return this;
  }

  public BigQueryToHivePartitionConverter withValue(String value) {
    partition.addToValues(value);
    return this;
  }
}
