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

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryPartitionGeneratorTest {

  private @Mock BigQueryMetastore bigQueryMetastore;
  private @Mock ExtractionService extractionService;
  private @Mock Partition partition;

  private BigQueryPartitionGenerator generator;

  private final String sourceDBName = "db";
  private final String sourceTableName = "tbl";
  private final String partitionKey = "foo";
  private final String partitionValue = "bar";
  private final String destinationBucket = "bucket";
  private final String destinationFolder = "folder";

  @Before
  public void init() {
    generator = new BigQueryPartitionGenerator(bigQueryMetastore, extractionService, sourceDBName, sourceTableName,
        partitionKey, partitionValue, destinationBucket, destinationFolder);
  }

  @Test
  public void generatePart() {
    ExtractionUri uri = generator.generatePartition(partition);
    assertThat(destinationBucket, is(uri.getBucket()));
    assertThat(uri.getFolder(), startsWith(destinationFolder));
    assertThat(uri.getKey(), endsWith(String.format("%s=%s.%s", partitionKey, partitionValue, uri.getExtension())));
    ArgumentCaptor<ExtractionContainer> extractionContainerCaptor = ArgumentCaptor.forClass(ExtractionContainer.class);
    verify(extractionService).register(extractionContainerCaptor.capture());
    assertThat(extractionContainerCaptor.getValue().getExtractionUri(), is(uri));
  }

}
