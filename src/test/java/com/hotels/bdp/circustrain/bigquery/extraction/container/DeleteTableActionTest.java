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
package com.hotels.bdp.circustrain.bigquery.extraction.container;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Table;

@RunWith(MockitoJUnitRunner.class)
public class DeleteTableActionTest {

  private @Mock Table table;
  private DeleteTableAction deleteTableAction;

  @Before
  public void setUp() {
    deleteTableAction = new DeleteTableAction(table);
  }

  @Test
  public void typical() {
    deleteTableAction.run();
    verify(table).delete();
  }

  @Test
  public void runActionsWithTableDeletionError() {
    doThrow(BigQueryException.class).when(table).delete();
    try {
      deleteTableAction.run();
    } catch (Exception e) {
      fail("Exception should not occur");
    }

  }

}
