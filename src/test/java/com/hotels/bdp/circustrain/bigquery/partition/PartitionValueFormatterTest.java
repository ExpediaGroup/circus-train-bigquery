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
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.bigquery.FieldValue;

@RunWith(MockitoJUnitRunner.class)
public class PartitionValueFormatterTest {

  private @Mock FieldValue fieldValue;

  private final String value = "value";

  @Before
  public void setUp() {
    when(fieldValue.getStringValue()).thenReturn(value);
  }

  @Test
  public void formatNotNeeded() {
    String partitionKeyType = "INTEGER";
    String formatted = PartitionValueFormatter.formatValue(fieldValue, partitionKeyType);
    assertThat(formatted, is(value));
  }

  @Test
  public void formatStringValue() {
    String partitionKeyType = "STRING";
    String expected = "\"" + value + "\"";
    String formatted = PartitionValueFormatter.formatValue(fieldValue, partitionKeyType);
    assertThat(formatted, is(expected));
  }

  @Test(expected = IllegalStateException.class)
  public void formatTimestampValue() {
    String partitionKeyType = "TIMESTAMP";
    PartitionValueFormatter.formatValue(fieldValue, partitionKeyType);
  }

  @Test
  public void formatDateValue() {
    String partitionKeyType = "DATE";
    String expected = "\"" + value + "\"";
    String formatted = PartitionValueFormatter.formatValue(fieldValue, partitionKeyType);
    assertThat(formatted, is(expected));
  }

  @Test
  public void formatTimestampFromUnixTimestampValue() {
    String partitionKeyType = "TIMESTAMP";
    String timestampValue = "1483228800.0";
    String expected = "TIMESTAMP_MICROS(1483228800000000)";
    setTimestampCondition(timestampValue);

    String formatted = PartitionValueFormatter.formatValue(fieldValue, partitionKeyType);
    assertThat(formatted, is(expected));
  }

  @Test
  public void formatTimestampWithMilliseconds() {
    String partitionKeyType = "TIMESTAMP";
    String timestampValue = "1408452095.22";
    String expected = "TIMESTAMP_MICROS(1408452095220000)";
    setTimestampCondition(timestampValue);

    String formatted = PartitionValueFormatter.formatValue(fieldValue, partitionKeyType);
    assertThat(formatted, is(expected));
  }

  private void setTimestampCondition(String value) {
    when(fieldValue.getStringValue()).thenReturn(value);
    when(fieldValue.getTimestampValue()).thenReturn(getTimestamp(value));
  }

  private long getTimestamp(String value) {
    long microseconds = TimeUnit.SECONDS.toMicros(1L);
    return new Double(Double.valueOf(value) * microseconds).longValue();
  }

}
