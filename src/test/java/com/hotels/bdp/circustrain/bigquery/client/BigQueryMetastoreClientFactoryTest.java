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
package com.hotels.bdp.circustrain.bigquery.client;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import org.junit.Test;

public class BigQueryMetastoreClientFactoryTest {

  @Test
  public void acceptsBigQueryUri() {
    BigQueryMetastoreClientFactory factory = new BigQueryMetastoreClientFactory(null, null, null);
    assertTrue(factory.accepts("bigquery://my-project-id"));
  }

  @Test
  public void rejectsThriftUri() {
    BigQueryMetastoreClientFactory factory = new BigQueryMetastoreClientFactory(null, null, null);
    assertFalse(factory.accepts("thrift://my-thrift-uri"));
  }
}
