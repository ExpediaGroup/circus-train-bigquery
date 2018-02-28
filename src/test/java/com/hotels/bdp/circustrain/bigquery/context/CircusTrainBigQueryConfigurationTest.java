/**
 * Copyright (C) 2015-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.bigquery.context;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.hotels.bdp.circustrain.core.conf.SourceCatalog;

public class CircusTrainBigQueryConfigurationTest {

  @Test
  public void getProjectId() {
    SourceCatalog sourceCatalog = new SourceCatalog();
    sourceCatalog.setHiveMetastoreUris("bigquery://test-project-id");
    CircusTrainBigQueryConfiguration configuration = new CircusTrainBigQueryConfiguration();
    assertEquals("test-project-id", configuration.getProjectId(sourceCatalog));
  }
}