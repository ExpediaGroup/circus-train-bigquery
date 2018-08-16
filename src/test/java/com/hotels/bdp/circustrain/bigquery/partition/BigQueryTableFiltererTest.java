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
package com.hotels.bdp.circustrain.bigquery.partition;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryTableFiltererTest {

  private final String databaseName = "db";
  private final String tableName = "tbl";
  private final String filterQuery = "foo < 5";

  private @Mock
  BigQueryMetastore bigQueryMetastore;
  private @Mock ExtractionService service;

  private BigQueryTableFilterer filterer;

  @Before
  public void init() {
    filterer = new BigQueryTableFilterer(bigQueryMetastore, service, databaseName, tableName, filterQuery);
  }

  @Test
  public void filterTable() {
    filterer.filterTable();
    verify(bigQueryMetastore).executeIntoDestinationTable(databaseName, tableName, filterQuery);
  }

  @Test
  public void getFilteredTable() {
    filterer.getFilteredTable();
    verify(bigQueryMetastore).getTable(databaseName, tableName);
    verify(service).register(any(ExtractionContainer.class));
  }
}
