package com.hotels.bdp.circustrain.bigquery.conversion;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;

public class BigQueryToHiveFieldConverterTest {

  private final Field stringField = Field.of("name", LegacySQLTypeName.STRING);
  private final Field intField = Field.of("age", LegacySQLTypeName.INTEGER);
  private Schema schema;

  @Test
  public void convertSchemaWithOneField() {
    schema = Schema.of(Collections.singletonList(stringField));
    List<FieldSchema> fieldSchema = BigQueryToHiveFieldConverter.convert(schema);
    assertThat(fieldSchema.size(), is(1));
  }

  @Test
  public void convertSchemaWithMultipleFields() {
    schema = Schema.of(Arrays.asList(stringField, intField));
    List<FieldSchema> fieldSchema = BigQueryToHiveFieldConverter.convert(schema);
    assertThat(fieldSchema.size(), is(2));
  }
}
