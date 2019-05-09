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
package com.hotels.bdp.circustrain.bigquery.extraction.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.Table;
import com.google.cloud.storage.Storage;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.extraction.container.PostExtractionAction;

@RunWith(MockitoJUnitRunner.class)
public class ExtractionServiceTest {

  private @Mock DataExtractor extractor;
  private @Mock DataCleaner cleaner;
  private @Mock Table table;
  private @Mock ExtractionContainer container;
  private @Mock Partition partition;
  private @Mock ExtractionUri extractionUri;
  private @Mock Storage storage;

  private ExtractionService service;
  private final Map<Table, ExtractionContainer> registry = new HashMap<>();
  private final List<PostExtractionAction> actions = new ArrayList<>();

  @Before
  public void init() {
    when(container.getPostExtractionActions()).thenReturn(actions);
    when(extractor.extract()).thenReturn(Arrays.asList(container));
    when(container.getTable()).thenReturn(table);
    service = new ExtractionService(extractor, cleaner, registry);
  }

  @Test
  public void register() {
    service.register(container);
    verify(extractor).add(container);
    verify(cleaner).add(container);
    assertThat(registry.size(), is(1));
    assertThat(registry.get(table), is(container));
  }

  @Test
  public void extract() {
    service.extract();
    verify(extractor).extract();
  }

  @Test
  public void cleanup() {
    service.cleanup();
    verify(cleaner).cleanup();
  }

  @Test
  public void retrieve() {
    service.register(container);
    assertThat(service.retrieve(table), is(container));
  }

  @Test
  public void runActions() {
    PostExtractionAction action1 = mock(PostExtractionAction.class);
    PostExtractionAction action2 = mock(PostExtractionAction.class);
    actions.add(action1);
    actions.add(action2);
    service.register(container);
    service.extract();
    verify(action1).run();
    verify(action2).run();
  }

  @Test
  public void runActionsWithException() {
    PostExtractionAction action1 = mock(PostExtractionAction.class);
    PostExtractionAction action2 = mock(PostExtractionAction.class);
    doThrow(new CircusTrainException("error")).when(action1).run();
    actions.add(action1);
    actions.add(action2);
    service.register(container);
    try {
      service.extract();
      fail("Exception should be thrown");
    } catch (CircusTrainException e) {
      verifyZeroInteractions(action2);
    }
  }

}
