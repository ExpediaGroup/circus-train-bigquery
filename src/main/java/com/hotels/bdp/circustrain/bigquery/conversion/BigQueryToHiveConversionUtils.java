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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;

class BigQueryToHiveConversionUtils {

  static List<FieldSchema> getCols(Schema schema) {
    BigQueryToHiveTypeConverter typeConverter = new BigQueryToHiveTypeConverter();
    Set<FieldSchema> partitionKeys = new LinkedHashSet<>();

    for (Field field : schema.getFields()) {
      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setName(field.getName().toLowerCase().trim());
      fieldSchema.setType(typeConverter.convert(field.getType().toString()).toLowerCase());
      partitionKeys.add(fieldSchema);
    }
    return new ArrayList<>(partitionKeys);
  }
}
