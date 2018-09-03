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

import java.sql.Timestamp;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

class PartitionValueFormatter {

  private String partitionValue;
  private String hiveColumnType;

  PartitionValueFormatter(String partitionKey, List<FieldSchema> cols) {
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
    case "string":
    case "date":
      return getStringValue();
    case "timestamp":
      return getTimestampValue();
    default:
      return this.partitionValue;
    }

  }

  private String getStringValue() {
    return String.format("\"%s\"", partitionValue);
  }

  private String getTimestampValue() {
    if (isNumber()) {
      return getTimestampFromNumber();
    } else {
      return String.format("\"%s\"", partitionValue.split(" UTC")[0]);
    }
  }

  private String getTimestampFromNumber() {
    Long unixTimestamp = Double.valueOf(partitionValue).longValue();
    String timestamp = new Timestamp(unixTimestamp * 1000L).toString();
    return String.format("\"%s\"", timestamp);
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
