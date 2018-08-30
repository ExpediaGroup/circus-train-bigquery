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
package com.hotels.bdp.circustrain.bigquery.conversion;

public class BigQueryToHiveTypeConverter {
  public String convert(String type) {
    type = type.toUpperCase();
    switch (type) {
    case "STRING":
    case "BYTES":
    case "DATETIME":
    case "TIME": {
      return "STRING";
    }

    case "DATE": {
      return "DATE";
    }

    case "TIMESTAMP": {
      return "TIMESTAMP";
    }

    case "INTEGER":
    case "INT64": {
      // BigQuery ints are 8 bytes
      return "BIGINT";
    }

    case "NUMERIC": {
      return "DECIMAL(38,9)";
    }

    case "FLOAT":
    case "FLOAT64": {
      return "DOUBLE";
    }

    case "BOOL":
    case "BOOLEAN": {
      return "BOOLEAN";
    }

    default: {
      throw new UnsupportedOperationException("BigQuery type " + type + " cannot be converted to Hive");
    }
    }
  }
}
