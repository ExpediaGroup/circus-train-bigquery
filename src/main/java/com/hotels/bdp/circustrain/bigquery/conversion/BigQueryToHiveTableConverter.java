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
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;

//TODO Rename?
public class BigQueryToHiveTableConverter {

  private Table table = new Table();

  private Schema schema = null;

  public BigQueryToHiveTableConverter() {
    table.setDbName("default");
    table.setTableName("default");
    table.setOwner("");
    table.setLastAccessTime(0);
    table.setRetention(0);
    table.setPartitionKeys(new ArrayList<FieldSchema>());
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
    table.setSd(sd);
  }

  public Table convert() {
    return new Table(table);
  }

  public BigQueryToHiveTableConverter withDatabaseName(String dbName) {
    table.setDbName(dbName);
    return this;
  }

  public BigQueryToHiveTableConverter withTableName(String tableName) {
    table.setTableName(tableName);
    return this;
  }

  public BigQueryToHiveTableConverter withLocation(String location) {
    table.getSd().setLocation(location);
    return this;
  }

  public BigQueryToHiveTableConverter withSchema(Schema schema) {
    BigQueryToHiveTypeConverter typeConverter = new BigQueryToHiveTypeConverter();
    for (Field field : schema.getFields()) {
      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setName(field.getName().toLowerCase());
      fieldSchema.setType(typeConverter.convert(field.getType().toString()).toLowerCase());
      table.getSd().addToCols(fieldSchema);
    }
    this.schema = schema;
    return this;
  }

  public BigQueryToHiveTableConverter withValues(Iterable<FieldValueList> fieldValues) {
    if (schema == null) {
      return this;
    }

    BigQueryToHiveTypeConverter typeConverter = new BigQueryToHiveTypeConverter();
    List<FieldSchema> partitionKeys = new ArrayList<>();
    for (Field field : schema.getFields()) {
      String key = field.getName().toLowerCase();
      for (FieldValueList row : fieldValues) {
        try {
          // Verify key exists if exception isnt thrown
          row.get(key);
          FieldSchema fieldSchema = new FieldSchema();
          fieldSchema.setName(row.get(key).toString());
          fieldSchema.setType(typeConverter.convert(field.getType().toString()));
          partitionKeys.add(fieldSchema);
        } catch (IllegalArgumentException e) {
          // Do nothing
        }

      }
    }
    table.setPartitionKeys(partitionKeys);
    return this;
  }

}
