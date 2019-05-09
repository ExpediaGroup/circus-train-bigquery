/**
 * Copyright (C) 2018-2019 Expedia, Inc.
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

import java.util.Locale;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;

public class HivePartitionKeyAdder {

  private final static String FIELD_SCHEMA_TYPE = "string";
  private final Table table;

  public HivePartitionKeyAdder(Table table) {
    this.table = table;
  }

  public Table add(String partitionKey, Schema filteredTableSchema) {
    if (isBlank(partitionKey) || filteredTableSchema == null) {
      return new Table(table);
    }

    Table newTable = new Table(table);

    for (Field field : filteredTableSchema.getFields()) {
      String fieldName = field.getName().toLowerCase(Locale.ROOT).trim();
      if (partitionKey.equals(fieldName)) {
        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.setName(fieldName);
        // all partition types are string
        fieldSchema.setType(FIELD_SCHEMA_TYPE);
        newTable.addToPartitionKeys(fieldSchema);
      }
    }
    return newTable;
  }
}
