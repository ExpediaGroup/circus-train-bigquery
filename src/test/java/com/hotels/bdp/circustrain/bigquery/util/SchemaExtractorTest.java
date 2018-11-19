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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.circustrain.bigquery.util.SchemaUtils.getTestData;
import static com.hotels.bdp.circustrain.bigquery.util.SchemaUtils.getTestSchema;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;

@RunWith(MockitoJUnitRunner.class)
public class SchemaExtractorTest {

  private @Mock Storage storage;
  private @Mock ExtractionUri extractionUri;
  private @Mock Page<Blob> blobs;
  private @Mock Blob blob;

  private String expectedSchema;
  private final SchemaExtractor schemaExtractor = new SchemaExtractor();

  @Before
  public void setUp() throws IOException {
    when(storage.list(anyString(), any(BlobListOption.class), any(BlobListOption.class))).thenReturn(blobs);
    when(blobs.iterateAll()).thenReturn(Arrays.asList(blob));
    when(blob.getContent()).thenReturn(getTestData());
    expectedSchema = getTestSchema();
  }

  @Test
  public void typical() {
    String schema = schemaExtractor.getSchemaFromStorage(storage, extractionUri);
    assertThat(schema, is(expectedSchema));
  }

  @Test(expected = CircusTrainException.class)
  public void getSchemaFromInvalidFile() throws IOException {
    when(blob.getContent()).thenReturn(getTestSchema().getBytes());
    schemaExtractor.getSchemaFromStorage(storage, extractionUri);
  }
}
