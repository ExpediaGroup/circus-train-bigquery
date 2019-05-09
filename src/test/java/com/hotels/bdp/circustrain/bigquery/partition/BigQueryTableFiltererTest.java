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
package com.hotels.bdp.circustrain.bigquery.partition;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;

import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.util.BigQueryMetastore;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryTableFiltererTest {

  private @Mock BigQueryMetastore bigQueryMetastore;
  private @Mock ExtractionService service;
  private @Mock TableResult tableResult;
  private @Mock Table table;

  private BigQueryTableFilterer filterer;

  private final String databaseName = "db";
  private final String tableName = "tbl";
  private final String filterQuery = "foo < 5";

  @Before
  public void init() {
    filterer = new BigQueryTableFilterer(bigQueryMetastore, service, databaseName, tableName, filterQuery);
  }

  @Test
  public void filterTable() {
    when(bigQueryMetastore.executeIntoDestinationTable(databaseName, tableName, filterQuery)).thenReturn(tableResult);
    TableResult result = filterer.filterTable();
    assertThat(result, is(tableResult));
  }

  @Test
  public void getFilteredTable() {
    when(bigQueryMetastore.getTable(databaseName, tableName)).thenReturn(table);
    Table result = filterer.getFilteredTable();
    assertThat(result, is(table));
    verify(service).register(any(ExtractionContainer.class));
  }
}
