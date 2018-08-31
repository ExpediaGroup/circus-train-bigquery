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
package com.hotels.bdp.circustrain.bigquery.partition;

import static jodd.util.StringUtil.isBlank;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;

import com.hotels.bdp.circustrain.bigquery.conversion.BigQueryToHiveTypeConverter;

public class HivePartitionKeyAdder {

  private static final Logger log = LoggerFactory.getLogger(HivePartitionKeyAdder.class);

  private final Table table;

  public HivePartitionKeyAdder(Table table) {
    this.table = table;
  }

  public Table add(String partitionKey, Schema filteredTableSchema) {
    if (isBlank(partitionKey) || filteredTableSchema == null) {
      log.info("PARTITION KEY ({}) IS BLANK OR FILTERED TABLE SCHEMA IS NULL ({})", partitionKey,
          filteredTableSchema == null);
      return new Table(table);
    }

    Table newTable = new Table(table);

    BigQueryToHiveTypeConverter typeConverter = new BigQueryToHiveTypeConverter();
    for (Field field : filteredTableSchema.getFields()) {
      String fieldName = field.getName().toLowerCase().trim();
      log.info("field name is {}", fieldName);
      if (partitionKey.equals(fieldName)) {
        log.info("field name is equal to partition key ({})", partitionKey);
        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.setName(fieldName);
        String type = typeConverter.convert(field.getType().toString()).toLowerCase();
        log.info("type is set to {}", type);
        fieldSchema.setType(typeConverter.convert(field.getType().toString()).toLowerCase());
        newTable.addToPartitionKeys(fieldSchema);
      }
    }
    return newTable;
  }
}
