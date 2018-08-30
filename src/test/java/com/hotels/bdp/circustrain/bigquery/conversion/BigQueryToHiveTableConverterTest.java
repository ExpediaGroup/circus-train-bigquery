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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;

public class BigQueryToHiveTableConverterTest {

  @Test
  public void withDatabaseName() {
    String dbName = "database";
    Table table = new BigQueryToHiveTableConverter().withDatabaseName(dbName).convert();
    assertEquals(table.getDbName(), dbName);
  }

  @Test
  public void withTableName() {
    String tableName = "database";
    Table table = new BigQueryToHiveTableConverter().withTableName(tableName).convert();
    assertEquals(table.getTableName(), tableName);
  }

  @Test
  public void withLocation() {
    String location = "getExtractedDataBaseLocation";
    Table table = new BigQueryToHiveTableConverter().withLocation(location).convert();
    assertEquals(table.getSd().getLocation(), location);
  }

  @Test
  public void withSchema() {
    Field integerField = Field.of("integer", LegacySQLTypeName.INTEGER);
    Field stringField = Field.of("string", LegacySQLTypeName.STRING);
    Field booleanField = Field.of("boolean", LegacySQLTypeName.BOOLEAN);
    Field floatField = Field.of("float", LegacySQLTypeName.FLOAT);
    Field dateField = Field.of("date", LegacySQLTypeName.DATE);
    Field dateTimeField = Field.of("datetime", LegacySQLTypeName.DATETIME);
    Field bytesField = Field.of("bytes", LegacySQLTypeName.BYTES);
    Field timeStampField = Field.of("timestamp", LegacySQLTypeName.TIMESTAMP);
    Field timeField = Field.of("time", LegacySQLTypeName.TIME);

    Schema schema = Schema.of(integerField, stringField, booleanField, floatField, dateField, dateTimeField, bytesField,
        timeStampField, timeField);
    Table table = new BigQueryToHiveTableConverter().withSchema(schema).convert();
    List<FieldSchema> fields = table.getSd().getCols();
    assertEquals("integer", fields.get(0).getName());
    assertEquals("bigint", fields.get(0).getType());
    assertEquals("string", fields.get(1).getName());
    assertEquals("string", fields.get(1).getType());
    assertEquals("boolean", fields.get(2).getName());
    assertEquals("boolean", fields.get(2).getType());
    assertEquals("float", fields.get(3).getName());
    assertEquals("double", fields.get(3).getType());
    assertEquals("date", fields.get(4).getName());
    assertEquals("date", fields.get(4).getType());
    assertEquals("datetime", fields.get(5).getName());
    assertEquals("string", fields.get(5).getType());
    assertEquals("bytes", fields.get(6).getName());
    assertEquals("string", fields.get(6).getType());
    assertEquals("timestamp", fields.get(7).getName());
    assertEquals("timestamp", fields.get(7).getType());
    assertEquals("time", fields.get(8).getName());
    assertEquals("string", fields.get(8).getType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void unsupportedTypeThrowsException() {
    Field unsupportedField = Field.of("record", LegacySQLTypeName.RECORD);
    Schema schema = Schema.of(unsupportedField);
    new BigQueryToHiveTableConverter().withSchema(schema).convert();
  }

}
