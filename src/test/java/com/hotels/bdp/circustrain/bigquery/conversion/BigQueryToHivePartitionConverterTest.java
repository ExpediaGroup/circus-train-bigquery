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
package com.hotels.bdp.circustrain.bigquery.conversion;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.bigquery.util.AvroConstants;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryToHivePartitionConverterTest {

  @Test
  public void typical() {
    Partition partition = new BigQueryToHivePartitionConverter().convert();
    assertThat(partition.getSd().getLocation(), is(""));
    assertThat(partition.getSd().getInputFormat(), is(AvroConstants.INPUT_FORMAT));
    assertThat(partition.getSd().getOutputFormat(), is(AvroConstants.OUTPUT_FORMAT));
    assertThat(partition.getSd().getSerdeInfo().getSerializationLib(), is(AvroConstants.SERIALIZATION_LIB));
  }

}
