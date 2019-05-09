/**
 * Copyright (C) 2018-2019 Expedia, Inc.
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import fm.last.commons.test.file.DataFolder;
import fm.last.commons.test.file.RootDataFolder;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.io.Files;

public class SchemaUtils {

  private static DataFolder dataFolder = new RootDataFolder();

  private SchemaUtils() {}

  public static String getTestSchema() throws IOException {
    File file = dataFolder.getFile("usa_names_schema.avsc");
    String content = Files.asCharSource(file, StandardCharsets.UTF_8).read();
    return content;
  }

  public static ReadChannel getTestData() throws IOException {
    return getChannelFromFile("usa_names.avro");
  }

  public static ReadChannel getInvalidTestData() throws IOException {
    return getChannelFromFile("invalid_avro.avro");
  }

  private static ReadChannel getChannelFromFile(String fileName) throws IOException {
    RandomAccessFile file = new RandomAccessFile(dataFolder.getFile(fileName), "r");
    return new StubReadChannel(file.getChannel());
  }

  public static void setUpSchemaMocks(Storage storage, Blob blob, Page<Blob> blobs) throws IOException {
    when(storage.list(anyString(), any(BlobListOption.class), any(BlobListOption.class))).thenReturn(blobs);
    when(blobs.iterateAll()).thenReturn(Arrays.asList(blob));
    when(blob.reader()).thenReturn(getTestData());
  }

}
