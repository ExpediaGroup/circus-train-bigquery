/**
 * Copyright (C) 2015-2018 Expedia Inc.
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
package com.hotels.bdp.circustrain.bigquery.context;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;

@RunWith(MockitoJUnitRunner.class)
public class CircusTrainBigQueryConditionTest {

  private @Mock ConditionContext context;
  private @Mock Environment environment;

  @Test
  public void matches() {
    when(context.getEnvironment()).thenReturn(environment);
    when(environment.getProperty(eq("source-catalog.hive-metastore-uris"), eq(String.class)))
        .thenReturn("bigquery://foobaz");
    when(environment.containsProperty(eq("source-catalog.hive-metastore-uris"))).thenReturn(true);
    CircusTrainBigQueryCondition condition = new CircusTrainBigQueryCondition();
    assertTrue(condition.matches(context, null));
  }

  @Test
  public void doesntMatch() {
    when(context.getEnvironment()).thenReturn(environment);
    when(environment.getProperty(eq("source-catalog.hive-metastore-uris"), eq(String.class)))
        .thenReturn("thrift://foo-baz-1234");
    when(environment.containsProperty(eq("source-catalog.hive-metastore-uris"))).thenReturn(true);
    CircusTrainBigQueryCondition condition = new CircusTrainBigQueryCondition();
    assertFalse(condition.matches(context, null));
  }
}
