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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ExtractionUriTest {

  @Test
  public void constructionTest() {
    ExtractionUri data = new ExtractionUri("bucket", "folder", "file", "avro", "avsc");
    assertEquals("bucket", data.getBucket());
    assertEquals("folder", data.getFolder());
    assertEquals("avro", data.getFormat());
    assertEquals("folder/file.avsc", data.getKey());
    assertEquals("gs://bucket/folder/file.avsc", data.getUri());
  }

}
