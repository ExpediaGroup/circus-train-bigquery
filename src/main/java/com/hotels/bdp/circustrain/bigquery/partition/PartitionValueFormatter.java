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

import com.google.cloud.bigquery.FieldValue;

class PartitionValueFormatter {

  private final String partitionKeyType;
  private final FieldValue partitionFieldValue;

  PartitionValueFormatter(FieldValue partitionFieldValue, String partitionKeyType) {
    this.partitionFieldValue = partitionFieldValue;
    this.partitionKeyType = partitionKeyType.toLowerCase();
  }

  String formatValue() {
    switch (partitionKeyType) {
    case "string":
    case "date":
      return getValueWithQuotes();
    case "timestamp":
      return getTimestampValue();
    default:
      return partitionFieldValue.getStringValue();
    }
  }

  private String getValueWithQuotes() {
    return String.format("\"%s\"", partitionFieldValue.getStringValue());
  }

  private String getTimestampValue() {
    if (isNumber()) {
      return getTimestampFromNumber();
    } else {
      throw new IllegalStateException(
          "Expected to get number from BigQuery for timestamp column but got " + partitionFieldValue);
    }
  }

  private String getTimestampFromNumber() {
    return String.format("TIMESTAMP_MICROS(%s)", partitionFieldValue.getTimestampValue());
  }

  private boolean isNumber() {
    try {
      Double.valueOf(partitionFieldValue.getStringValue());
      return true;
    } catch (Exception e) {
      return false;
    }
  }

}
