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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.ExtractionContainerFactory;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.util.CircusTrainBigQueryMetastore;

@RunWith(MockitoJUnitRunner.class)
public class HivePartitionGeneratorTest {

  private @Mock Table table;
  private @Mock
  CircusTrainBigQueryMetastore metastore;
  private @Mock ExtractionService service;
  private @Mock
  ExtractionContainerFactory factory;
  private @Mock ExtractionContainer container;
  private @Mock ExtractionUri uri;

  private HivePartitionGenerator hivePartitionGenerator;

  @Before
  public void init() {
    hivePartitionGenerator = new HivePartitionGenerator(table, metastore, service, factory);
  }

  @Test
  public void generateTest() {
    String dbName = "db";
    String tblName = "tbl";
    when(table.getDbName()).thenReturn(dbName);
    when(table.getTableName()).thenReturn(tblName);
    StorageDescriptor sd = new StorageDescriptor();
    String partitionKey = "foo";
    String type = "string";

    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setName(partitionKey);
    fieldSchema.setType(type);
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(fieldSchema);
    when(table.getSd()).thenReturn(sd);
    sd.setCols(cols);

    List<FieldValueList> rows = new ArrayList<>();
    FieldValueList row = mock(FieldValueList.class);
    rows.add(row);
    Object o = mock(Object.class);
    when(o.toString()).thenReturn(partitionKey);
    FieldValue fieldValue = mock(FieldValue.class);
    when(row.get(partitionKey)).thenReturn(fieldValue);
    when(fieldValue.getValue()).thenReturn(o);

    when(factory.get()).thenReturn(container);
    when(container.getExtractionUri()).thenReturn(uri);
    String bucket = "bucket";
    String folder = "folder";
    when(uri.getBucket()).thenReturn(bucket);
    when(uri.getFolder()).thenReturn(folder);
    List<Partition> partitions = hivePartitionGenerator.generate(partitionKey, rows);
    assertEquals(1, partitions.size());
    Partition partition = partitions.get(0);
    assertEquals(dbName, partition.getDbName());
    assertEquals(tblName, partition.getTableName());
    assertEquals(1, partition.getSd().getCols().size());
    assertEquals(cols.get(0), partition.getSd().getCols().get(0));
    assertTrue(partition.getSd().getLocation().startsWith(String.format("gs://%s/%s/", bucket, folder)));
  }

}
