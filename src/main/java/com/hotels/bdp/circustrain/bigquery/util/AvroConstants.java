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
package com.hotels.bdp.circustrain.bigquery.util;

public final class AvroConstants {
  public static final String SCHEMA_PARAMETER = "avro.schema.literal";
  public static final String INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
  public static final String OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
  public static final String SERIALIZATION_LIB = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";

  private AvroConstants() {}

}
