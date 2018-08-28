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
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

class PartitionValueFormatter {
  private static final Logger log = LoggerFactory.getLogger(BigQueryPartitionGenerator.class);

  private static final Set<String> NEEDS_FORMATTING = ImmutableSet.of("string");
  private boolean format = false;
  private final String partitionKeyType;

  PartitionValueFormatter(String partitionKey, String partitionKeyType, List<FieldSchema> cols) {
    this.partitionKeyType = partitionKeyType.toLowerCase();
    partitionKey = partitionKey.toLowerCase().trim();
    for (FieldSchema col : cols) {
      String name = col.getName().toLowerCase().trim();
      String type = col.getType().toLowerCase().trim();

      System.out.println("HIVE COLUMN NAME: " + name);
      System.out.println("HIVE COLUMN TYPE: " + type);
      System.out.println("BIG QUERY COLUMN TYPE: " + this.partitionKeyType);
      System.out.println("BIG QUERY COLUMN CONTAINS HIVE TYPE: " + this.partitionKeyType.contains(type));

      if (name.equals(partitionKey) && this.partitionKeyType.contains(type)) {
        format = true;
        break;
      }
    }
  }

  String format(String partitionValue) {
    partitionValue = partitionValue.toLowerCase().trim();
    if (!format) {
      return partitionValue;
    } else if (partitionKeyType.equals("timestamp") && isNumber(partitionValue)) {
      return getTimestampFromNumber(partitionValue);
    }
    return "\"" + partitionValue + "\"";
  }

  private String getTimestampFromNumber(String partitionValue) {
    return String.format("TIMESTAMP(%s)", Double.valueOf(partitionValue).intValue());
  }

  private boolean isNumber(String partitionValue) {
    try {
      Double.valueOf(partitionValue);
      log.info("PARTITION VALUE IS NUMBER");
      return true;
    } catch (Exception e) {
      log.info("PARTITION VALUE IS NOT A NUMBER: {}; exception: {}", partitionValue, e.getMessage());
      return false;
    }
  }
}
