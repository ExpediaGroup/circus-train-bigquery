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
package com.hotels.bdp.circustrain.bigquery.copier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Test;

import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.bigquery.extraction.BigQueryDataExtractionManager;

public class BigQueryCopierTest {

  @Test
  public void copy() {
    BigQueryDataExtractionManager dataExtractionManager = mock(BigQueryDataExtractionManager.class);
    Copier delegate = mock(Copier.class);
    BigQueryCopier bigQueryCopier = new BigQueryCopier(delegate, dataExtractionManager);
    bigQueryCopier.copy();
    verify(dataExtractionManager).extractAll();
    verifyNoMoreInteractions(dataExtractionManager);
    verify(delegate).copy();
  }
}
