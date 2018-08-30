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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionValueFormatter {
  private static final Logger log = LoggerFactory.getLogger(BigQueryPartitionGenerator.class);

  private String partitionValue;
  private String hiveColumnType;
  private final String partitionKeyType;

  PartitionValueFormatter(String partitionKey, String partitionKeyType, List<FieldSchema> cols) {
    this.partitionKeyType = partitionKeyType.toLowerCase();
    partitionKey = partitionKey.toLowerCase().trim();
    for (FieldSchema col : cols) {
      String name = col.getName().toLowerCase().trim();
      String type = col.getType().toLowerCase().trim();

      if (name.equals(partitionKey)) {
        hiveColumnType = type;
        break;
      }
    }
  }

  String format(String partitionValue) {
    this.partitionValue = partitionValue.trim();

    switch (hiveColumnType) {
    case "string": {
      return getStringValue();
    }
    case "date": {
      return getDateValue();
    }
    case "timestamp": {
      return getTimestampValue();
    }

    default: {
      return this.partitionValue;
    }
    }

  }

  private String getStringValue() {
    return String.format("\"%s\"", partitionValue);
  }

  private String getDateValue() {
    return String.format("cast(\"%s\" as date)", partitionValue);
  }

  private String getTimestampValue() {
    if (isNumber()) {
      return getTimestampFromNumber();
    } else {
      return String.format("cast(\"%s\" as timestamp)", partitionValue);
    }
  }

  private String getTimestampFromNumber() {
    return String.format("timestamp_seconds(%s)", Double.valueOf(partitionValue).intValue());
  }

  private boolean isNumber() {
    try {
      Double.valueOf(partitionValue);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

}
