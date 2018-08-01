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
package com.hotels.bdp.circustrain.bigquery.extraction.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.Table;

import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.service.DataCleaner;
import com.hotels.bdp.circustrain.bigquery.extraction.service.DataExtractor;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;

@RunWith(MockitoJUnitRunner.class)
public class ExtractionServiceTest {

  private ExtractionService service;
  private @Mock
  DataExtractor extractor;
  private @Mock
  DataCleaner cleaner;
  private Map<Table, ExtractionContainer> registry = new HashMap<>();

  @Before
  public void init() {
    service = new ExtractionService(extractor, cleaner, registry);
  }

  @Test
  public void register() {
    ExtractionContainer container = mock(ExtractionContainer.class);
    Table table = mock(Table.class);
    when(container.getTable()).thenReturn(table);
    service.register(container);
    Mockito.verify(extractor).add(container);
    Mockito.verify(cleaner).add(container);
    Mockito.verify(extractor).add(container);
    assertEquals(1, registry.size());
  }

  @Test
  public void extract() {
    service.extract();
    Mockito.verify(extractor).extract();
  }

  @Test
  public void cleanup() {
    service.cleanup();
    Mockito.verify(cleaner).cleanup();
  }

  @Test
  public void retrieve() {
    ExtractionContainer container = mock(ExtractionContainer.class);
    Table table = mock(Table.class);
    when(container.getTable()).thenReturn(table);
    service.register(container);
    assertEquals(container, service.retrieve(table));
  }

}
