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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import com.google.cloud.storage.Blob;

public class BigQueryToHivePartitionConverter {

  private final Partition partition = new Partition();

  public BigQueryToHivePartitionConverter() {
    partition.setDbName("default");
    partition.setTableName("default");
    partition.setValues(new ArrayList<String>());
    partition.setLastAccessTime(0);
    partition.setParameters(new HashMap<String, String>());
    mockStats();
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("");
    sd.setNumBuckets(-1);
    sd.setBucketCols(Collections.<String> emptyList());
    sd.setSortCols(Collections.<Order> emptyList());
    sd.setInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
    sd.setCompressed(false);
    sd.setStoredAsSubDirectories(false);
    sd.setNumBuckets(-1);
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.avro.AvroSerDe");
    SkewedInfo si = new SkewedInfo();
    si.setSkewedColNames(Collections.<String> emptyList());
    si.setSkewedColValueLocationMaps(Collections.<List<String>, String> emptyMap());
    si.setSkewedColValues(Collections.<List<String>> emptyList());
    sd.setSkewedInfo(new SkewedInfo());
    sd.setSerdeInfo(serDeInfo);
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

  public BigQueryToHivePartitionConverter withCols(Blob file) {
    String schema = BigQueryToHiveTableConverter.getSchemaFromFile(file);
    partition.getSd().getSerdeInfo().putToParameters("avro.schema.literal", schema);
    return this;
  }

  public BigQueryToHivePartitionConverter withValue(String value) {
    partition.addToValues(value);
    return this;
  }

  // Work around for issue: https://issues.apache.org/jira/browse/HIVE-18767
  // Delete this code once the fix
  // (https://github.com/apache/hive/commit/2fe5186a337141b6fd80b40abbc8bc4226bee962#diff-2a1b7c6ec7a77f1ca9ad84225d192e36)
  // has been released in Hive 2.3.x
  private void mockStats() {
    for (String key : StatsSetupConst.fastStats) {
      partition.getParameters().put(key, "1");
    }
  }
}
