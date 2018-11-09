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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.cloud.bigquery.Table;

public class ExtractionContainer {

  private final Table table;
  private final ExtractionUri extractionUri;
  private List<PostExtractionAction> postExtractionActions = Collections.emptyList();

  public ExtractionContainer(
      Table table,
      ExtractionUri extractionUri,
      List<PostExtractionAction> postExtractionActions) {
    this.table = table;
    this.extractionUri = extractionUri;
    this.postExtractionActions = Collections.unmodifiableList(postExtractionActions);
  }

  public ExtractionContainer(Table table, ExtractionUri extractionUri, PostExtractionAction postExtractionAction) {
    this(table, extractionUri, Arrays.asList(postExtractionAction));
  }

  public ExtractionContainer(Table table, ExtractionUri extractionUri) {
    this.table = table;
    this.extractionUri = extractionUri;
  }

  public Table getTable() {
    return table;
  }

  public ExtractionUri getExtractionUri() {
    return extractionUri;
  }

  public List<PostExtractionAction> getPostExtractionActions() {
    return postExtractionActions;
  }

}
