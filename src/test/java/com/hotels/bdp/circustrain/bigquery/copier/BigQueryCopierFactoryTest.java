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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryCopierFactoryTest {

  private @Mock CopierFactory copierFactory;
  private @Mock ExtractionService service;

  private BigQueryCopierFactory factory;

  @Before
  public void setUp() {
    List<CopierFactory> copierFactories = Collections.singletonList(copierFactory);
    factory = new BigQueryCopierFactory(copierFactories, service);
  }

  @Test
  public void supportsSchemes() {
    when(copierFactory.supportsSchemes(anyString(), anyString())).thenReturn(true);
    assertThat(factory.supportsSchemes("gs://", "s3://"), is(true));
    assertEquals(copierFactory, factory.getSupportedFactory());
  }

  @Test
  public void doesntSupportSchemes() {
    when(copierFactory.supportsSchemes(anyString(), anyString())).thenReturn(false);
    assertThat(factory.supportsSchemes("gs://", "s3://"), is(false));
    assertNull(factory.getSupportedFactory());
  }

}
