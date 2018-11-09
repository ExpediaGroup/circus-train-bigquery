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
package com.hotels.bdp.circustrain.bigquery.extraction.container;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;

import com.hotels.bdp.circustrain.bigquery.util.AvroConstants;
import com.hotels.bdp.circustrain.bigquery.util.SchemaUtils;

@RunWith(MockitoJUnitRunner.class)
public class UpdatePartitionSchemaActionTest {

  private @Mock Storage storage;
  private @Mock Blob blob;
  private @Mock Page<Blob> blobs;
  private @Mock ExtractionUri extractionUri;
  private final Partition partition = new Partition();
  private UpdatePartitionSchemaAction updatePartitionSchemaAction;
  private String schema;

  @Before
  public void setUp() throws IOException {
    partition.setSd(new StorageDescriptor());
    partition.getSd().setSerdeInfo(new SerDeInfo());
    SchemaUtils.setUpSchemaMocks(storage, blob, blobs);
    schema = SchemaUtils.getTestSchema();
  }

  @Test
  public void typical() throws IOException {
    updatePartitionSchemaAction = new UpdatePartitionSchemaAction(partition, storage, extractionUri);
    updatePartitionSchemaAction.run();
    assertThat(partition.getSd().getSerdeInfo().getParameters().get(AvroConstants.SCHEMA_PARAMETER), is(schema));
  }

}
