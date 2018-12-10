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

final class PartitionValueFormatter {

  private PartitionValueFormatter() {}

  static String formatValue(FieldValue partitionFieldValue, String partitionKeyType) {
    partitionKeyType = partitionKeyType.toLowerCase();

    switch (partitionKeyType) {
    case "string":
    case "date":
      return String.format("\"%s\"", partitionFieldValue.getStringValue());
    case "timestamp":
      return getTimestampValue(partitionFieldValue);
    default:
      return partitionFieldValue.getStringValue();
    }
  }

  private static String getTimestampValue(FieldValue partitionFieldValue) {
    if (isDouble(partitionFieldValue)) {
      return String.format("TIMESTAMP_MICROS(%s)", partitionFieldValue.getTimestampValue());
    } else {
      throw new IllegalStateException(
          "Expected to get a double from BigQuery for timestamp column but got " + partitionFieldValue);
    }
  }

  private static boolean isDouble(FieldValue partitionFieldValue) {
    try {
      Double.valueOf(partitionFieldValue.getStringValue());
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

}
