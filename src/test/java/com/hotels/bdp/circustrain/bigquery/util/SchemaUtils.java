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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fm.last.commons.test.file.DataFolder;
import fm.last.commons.test.file.RootDataFolder;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.io.Files;

public class SchemaUtils {

  private static final Logger log = LoggerFactory.getLogger(SchemaUtils.class);

  private @Rule static DataFolder dataFolder = new RootDataFolder();

  private SchemaUtils() {}

  public static String getTestSchema() {
    File file = getFile("usa_names_schema.avsc");
    try {
      String content = Files.asCharSource(file, Charset.forName("UTF-8")).read();
      return content;
    } catch (IOException e) {
      log.error("Could not get string for usa_names_schema.avsc", e);
      return null;
    }
  }

  public static byte[] getTestData() {
    File file = getFile("usa_names.avro");
    try {
      byte[] content = Files.asByteSource(file).read();
      return content;
    } catch (IOException e) {
      log.error("Could not get bytes for test file usa_names.avro", e);
      return null;
    }
  }

  private static File getFile(String name) {
    try {
      File folder = dataFolder.getFolder();
      String path = folder.getPath() + "/" + name;
      File file = new File(path);
      return file;
    } catch (IOException e) {
      log.error("Could not get test folder", e);
      return null;
    }
  }

  public static void setUpSchemaMocks(Storage storage, Blob blob, Page<Blob> blobs) {
    byte[] content = SchemaUtils.getTestData();
    when(storage.list(anyString(), any(BlobListOption.class), any(BlobListOption.class))).thenReturn(blobs);
    when(blobs.iterateAll()).thenReturn(Arrays.asList(blob));
    when(blob.getContent()).thenReturn(content);
  }

}
