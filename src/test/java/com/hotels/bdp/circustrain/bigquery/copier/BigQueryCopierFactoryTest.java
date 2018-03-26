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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.bigquery.extraction.BigQueryDataExtractionManager;

public class BigQueryCopierFactoryTest {

  @Test
  public void supportsScheme() {
    CopierFactory unsupported = mock(CopierFactory.class);
    CopierFactory supported = mock(CopierFactory.class);
    when(unsupported.supportsSchemes(anyString(), anyString())).thenReturn(false);
    when(supported.supportsSchemes(anyString(), anyString())).thenReturn(true);
    List<CopierFactory> copierFactories = Arrays.asList(unsupported, supported);
    BigQueryDataExtractionManager dataExtractionManager = mock(BigQueryDataExtractionManager.class);
    BigQueryCopierFactory factory = new BigQueryCopierFactory(copierFactories, dataExtractionManager);
    assertTrue(factory.supportsSchemes("gs://", "s3://"));
    assertEquals(supported, factory.getSupportedFactory());
  }

  @Test
  public void doesntSupportScheme() {
    CopierFactory unsupported = mock(CopierFactory.class);
    CopierFactory alsoUnsupported = mock(CopierFactory.class);
    when(unsupported.supportsSchemes(anyString(), anyString())).thenReturn(false);
    when(alsoUnsupported.supportsSchemes(anyString(), anyString())).thenReturn(false);
    List<CopierFactory> copierFactories = Arrays.asList(unsupported, alsoUnsupported);
    BigQueryDataExtractionManager dataExtractionManager = mock(BigQueryDataExtractionManager.class);
    BigQueryCopierFactory factory = new BigQueryCopierFactory(copierFactories, dataExtractionManager);
    assertFalse(factory.supportsSchemes("gs://", "s3://"));
    assertNull(factory.getSupportedFactory());
  }
}
