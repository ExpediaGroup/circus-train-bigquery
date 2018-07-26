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

import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;

import com.hotels.bdp.circustrain.bigquery.conversion.BigQueryToHivePartitionConverter;

public class HivePartitionFactory {

  private final Partition partition;

  HivePartitionFactory(
      String databaseName,
      String tableName,
      String partitionValue,
      List<FieldSchema> cols,
      String location) {
    this.partition = new BigQueryToHivePartitionConverter()
        .withDatabaseName(databaseName)
        .withTableName(tableName)
        .withValue(partitionValue)
        .withCols(cols)
        .withLocation(location)
        .convert();
  }

  public Partition get() {
    return partition;
  }
}
