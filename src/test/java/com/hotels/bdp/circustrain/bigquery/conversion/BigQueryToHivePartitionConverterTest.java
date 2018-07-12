package com.hotels.bdp.circustrain.bigquery.conversion;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryToHivePartitionConverterTest {

  @Test
  public void withDatabaseName() {
    String dbName = "database";
    Partition partition = new BigQueryToHivePartitionConverter().withDatabaseName(dbName).convert();
    assertEquals(partition.getDbName(), dbName);
  }

  @Test
  public void withpartitionName() {
    String partitionName = "database";
    Partition partition = new BigQueryToHivePartitionConverter().withTableName(partitionName).convert();
    assertEquals(partition.getTableName(), partitionName);
  }

  @Test
  public void withLocation() {
    String location = "getDataLocation";
    Partition partition = new BigQueryToHivePartitionConverter().withLocation(location).convert();
    assertEquals(partition.getSd().getLocation(), location);
  }

  @Test
  public void withValues() {
    Field stringField = Field.of("foo", LegacySQLTypeName.STRING);

    Schema schema = Schema.of(stringField);
    FieldValueList fieldValues = mock(FieldValueList.class);
    FieldValue field = mock(FieldValue.class);
    when(fieldValues.get("foo")).thenReturn(field);
    when(field.getValue()).thenReturn("baz");
    fieldValues.add(field);

    Partition partition = new BigQueryToHivePartitionConverter()
        .withSchema(schema)
        .withValues(Collections.singletonList(fieldValues))
        .convert();
    assertEquals(partition.getValues().get(0), "foo=baz");
  }

  @Test(expected = IllegalArgumentException.class)
  public void unsupportedTypeThrowsException() {
    Field unsupportedField = Field.of("record", LegacySQLTypeName.RECORD);
    Schema schema = Schema.of(unsupportedField);
    new BigQueryToHivePartitionConverter().withSchema(schema).convert();
  }
}
