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
    if ("STRING".equals(type)) {
      return "STRING";
    }
    if ("INTEGER".equals(type)) {
      return "INT";
    }
    if ("INT64".equals(type)) {
      return "BIGINT";
    }
    if ("FLOAT".equals(type)) {
      return "DOUBLE";
    }
    if ("FLOAT64".equals(type)) {
      return "DOUBLE";
    }
    if ("BOOL".equals(type)) {
      return "BOOLEAN";
    }
    if ("BOOLEAN".equals(type)) {
      return "BOOLEAN";
    }
    if ("BYTES".equals(type)) {
      return "STRING";
    }
    if ("DATETIME".equals(type)) {
      return "STRING";
    }
    if ("DATE".equals(type)) {
      return "STRING";
    }
    if ("TIME".equals(type)) {
      return "STRING";
    }
    if ("TIMESTAMP".equals(type)) {
      return "STRING";
    }
    throw new UnsupportedOperationException("BigQuery type " + type + " cannot be converted to Hive");
  }
}
