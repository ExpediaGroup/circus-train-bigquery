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
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;

import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionContainerFactory;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;
import com.hotels.bdp.circustrain.bigquery.util.SchemaExtractor;

@RunWith(MockitoJUnitRunner.class)
public class HivePartitionGeneratorTest {

  private @Mock BigQueryMetastore metastore;
  private @Mock ExtractionService service;
  private @Mock ExtractionContainerFactory factory;
  private @Mock ExtractionContainer container;
  private @Mock FieldValue fieldValue;
  private @Mock FieldValueList row;
  private @Mock SchemaExtractor schemaExtractor;

  private HivePartitionGenerator hivePartitionGenerator;
  private final Table table = new Table();
  private final FieldSchema fieldSchema = new FieldSchema();
  private final List<FieldSchema> cols = new ArrayList<>();
  private final List<FieldValueList> rows = new ArrayList<>();
  private final ExtractionUri extractionUri = new ExtractionUri();
  private final StorageDescriptor storageDescriptor = new StorageDescriptor();
  private final String databaseName = "database";
  private final String tableName = "table";
  private final String partitionKey = "foo";
  private final String value = "value";
  private final String type = "string";
  private final String bucket = extractionUri.getBucket();
  private final String folder = extractionUri.getFolder();
  private final String filePartialLocation = String.format("gs://%s/%s/", bucket, folder);

  @Before
  public void init() throws IOException {
    table.setDbName(databaseName);
    table.setTableName(tableName);
    table.setSd(storageDescriptor);
    fieldSchema.setName(partitionKey);

    hivePartitionGenerator = new HivePartitionGenerator(table, metastore, service, factory, schemaExtractor);
    when(factory.newInstance()).thenReturn(container);
    when(container.getExtractionUri()).thenReturn(extractionUri);
    when(row.get(partitionKey)).thenReturn(fieldValue);
    when(fieldValue.getValue()).thenReturn(value);
  }

  @Test
  public void typical() {
    fieldSchema.setType(type);
    cols.add(fieldSchema);
    storageDescriptor.setCols(cols);
    rows.add(row);

    List<Partition> partitions = hivePartitionGenerator.generate(partitionKey, type, rows);

    assertThat(partitions.size(), is(1));
    Partition partition = partitions.get(0);
    assertThat(partition.getDbName(), is(databaseName));
    assertThat(partition.getTableName(), is(tableName));
    assertThat(partition.getSd().getLocation(), startsWith(filePartialLocation));
  }

}
