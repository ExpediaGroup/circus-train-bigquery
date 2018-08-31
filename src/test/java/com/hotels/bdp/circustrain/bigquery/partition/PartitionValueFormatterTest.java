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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Test;

public class PartitionValueFormatterTest {

  private final List<FieldSchema> cols = new ArrayList<>();
  private final FieldSchema fieldSchema = new FieldSchema();

  private final String partitionKey = "key";
  private String type;
  private String value = "value";

  @Test
  public void formatNotNeeded() {
    type = "INTEGER";
    fieldSchema.setName(partitionKey);
    fieldSchema.setType(type);
    cols.add(fieldSchema);

    PartitionValueFormatter formatter = new PartitionValueFormatter(partitionKey, type, cols);
    assertEquals(value, formatter.format(value));
  }

  @Test
  public void formatStringValue() {
    type = "STRING";
    fieldSchema.setName(partitionKey);
    fieldSchema.setType(type);
    cols.add(fieldSchema);

    String expected = "\"" + value + "\"";
    PartitionValueFormatter formatter = new PartitionValueFormatter(partitionKey, type, cols);
    assertThat(formatter.format(value), is(expected));
  }

  @Test
  public void formatTimestampValue() {
    type = "TIMESTAMP";
    fieldSchema.setName(partitionKey);
    fieldSchema.setType(type);
    cols.add(fieldSchema);

    String expected = "\"" + value + "\"";
    PartitionValueFormatter formatter = new PartitionValueFormatter(partitionKey, type, cols);
    assertEquals(expected, formatter.format(value));
  }

  @Test
  public void formatDateValue() {
    type = "DATE";
    fieldSchema.setName(partitionKey);
    fieldSchema.setType(type);
    cols.add(fieldSchema);

    String expected = "cast(\"" + value + "\" as date)";
    PartitionValueFormatter formatter = new PartitionValueFormatter(partitionKey, type, cols);
    assertEquals(expected, formatter.format(value));
  }

  @Test
  public void formatTimestampFromUnixTimestampValue() {
    type = "TIMESTAMP";
    value = "1483228800.0";
    fieldSchema.setName(partitionKey);
    fieldSchema.setType(type);
    cols.add(fieldSchema);

    String expected = "timestamp_seconds(1483228800)";
    String timestampExpected = "\"2017-01-01 00:00:00.0\"";
    PartitionValueFormatter formatter = new PartitionValueFormatter(partitionKey, type, cols);
    assertEquals(timestampExpected, formatter.format(value));
  }

}
