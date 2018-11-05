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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.cloud.bigquery.Table;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.container.PostExtractionAction;

@Component
public class ExtractionService {

  private final DataExtractor extractor;
  private final DataCleaner cleaner;
  private final Map<Table, ExtractionContainer> registry;
  private Storage storage;

  @Autowired
  ExtractionService(Storage storage) {
    this(new DataExtractor(storage), new DataCleaner(storage), new HashMap<Table, ExtractionContainer>());
    this.storage = storage;
  }

  @VisibleForTesting
  ExtractionService(DataExtractor extractor, DataCleaner cleaner, Map<Table, ExtractionContainer> registry) {
    this.extractor = extractor;
    this.cleaner = cleaner;
    this.registry = registry;
  }

  public void register(ExtractionContainer container) {
    extractor.add(container);
    cleaner.add(container);
    registry.put(container.getTable(), container);
  }

  public void extract() {
    List<ExtractionContainer> extracted = extractor.extract();
    for (ExtractionContainer container : extracted) {
      List<PostExtractionAction> actions = container.getPostExtractionActions();
      for (PostExtractionAction action : actions) {
        action.run();
      }
    }
  }

  public void cleanup() {
    cleaner.cleanup();
  }

  public ExtractionContainer retrieve(Table table) {
    return registry.get(table);
  }

  public Storage getStorage() {
    return storage;
  }
}
