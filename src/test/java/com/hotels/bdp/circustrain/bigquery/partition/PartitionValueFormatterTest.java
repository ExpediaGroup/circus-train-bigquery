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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.FieldValue;

@RunWith(MockitoJUnitRunner.class)
public class PartitionValueFormatterTest {

  private @Mock FieldValue fieldValue;

  private String value = "value";
  private String partitionKeyType;
  private String expected;

  @Before
  public void setUp() {
    when(fieldValue.getStringValue()).thenReturn(value);
  }

  @Test
  public void formatNotNeeded() {
    partitionKeyType = "INTEGER";
    PartitionValueFormatter formatter = new PartitionValueFormatter(fieldValue, partitionKeyType);
    assertEquals(value, formatter.formatValue());
  }

  @Test
  public void formatStringValue() {
    partitionKeyType = "STRING";
    expected = "\"" + value + "\"";
    PartitionValueFormatter formatter = new PartitionValueFormatter(fieldValue, partitionKeyType);
    assertThat(formatter.formatValue(), is(expected));
  }

  @Test(expected = IllegalStateException.class)
  public void formatTimestampValue() {
    partitionKeyType = "TIMESTAMP";
    new PartitionValueFormatter(fieldValue, partitionKeyType).formatValue();
  }

  @Test
  public void formatDateValue() {
    partitionKeyType = "DATE";
    expected = "\"" + value + "\"";
    PartitionValueFormatter formatter = new PartitionValueFormatter(fieldValue, partitionKeyType);
    assertEquals(expected, formatter.formatValue());
  }

  @Test
  public void formatTimestampFromUnixTimestampValue() {
    partitionKeyType = "TIMESTAMP";
    value = "1483228800.0";
    expected = "TIMESTAMP_MICROS(1483228800000000)";
    setTimestampCondition(value);

    PartitionValueFormatter formatter = new PartitionValueFormatter(fieldValue, partitionKeyType);
    assertEquals(expected, formatter.formatValue());
  }

  @Test
  public void formatTimestampWithMilliseconds() {
    partitionKeyType = "TIMESTAMP";
    value = "1408452095.22";
    expected = "TIMESTAMP_MICROS(1408452095220000)";
    setTimestampCondition(value);

    PartitionValueFormatter formatter = new PartitionValueFormatter(fieldValue, partitionKeyType);
    assertEquals(expected, formatter.formatValue());
  }

  private void setTimestampCondition(String value) {
    when(fieldValue.getStringValue()).thenReturn(value);
    when(fieldValue.getTimestampValue()).thenReturn(getTimestamp(value));
  }

  private long getTimestamp(String value) {
    int microseconds = 1000000;
    return new Double(Double.valueOf(value) * microseconds).longValue();
  }

}
