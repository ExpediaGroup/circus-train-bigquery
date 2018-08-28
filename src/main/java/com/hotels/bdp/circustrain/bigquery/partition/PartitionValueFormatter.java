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

import com.google.common.collect.ImmutableSet;

class PartitionValueFormatter {

  private static final Set<String> NEEDS_FORMATTING = ImmutableSet.of("string");
  private boolean format = false;

  PartitionValueFormatter(String partitionKey, List<FieldSchema> cols) {
    partitionKey = partitionKey.toLowerCase().trim();
    for (FieldSchema col : cols) {
      String name = col.getName().toLowerCase().trim();
      String type = col.getType().toLowerCase().trim();

      if (name.equals(partitionKey) && NEEDS_FORMATTING.contains(type)) {
        format = true;
        break;
      }
    }
  }

  String format(String partitionValue) {
    partitionValue = partitionValue.trim();
    if (!format) {
      return partitionValue;
    }
    return "\"" + partitionValue + "\"";
  }
}
