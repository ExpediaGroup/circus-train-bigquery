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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.google.api.client.util.Joiner;

public class PartitionColumnFormatter {

  private static Joiner joiner = Joiner.on(',');

  private PartitionColumnFormatter() {}

  public static String formatColumns(List<FieldSchema> columns) {
    List<String> formattedColumns = new ArrayList<>();
    for (FieldSchema col : columns) {
      String name = col.getName().trim();
      String type = col.getType().toLowerCase().trim();

      if (type.equals("timestamp")) {
        formattedColumns.add(formatTimestampColumn(name));
      } else {
        formattedColumns.add(name);
      }
    }

    return joiner.join(formattedColumns);
  }

  // format the timestamp returned by BigQuery to include the timestamp without timezone
  // if timestamp contains timezone, Hive converts that value to null
  private static String formatTimestampColumn(String name) {
    return "FORMAT_TIMESTAMP(\"%F %H:%M:%E*S\", " + name + ") as " + name;
  }

}
