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
package com.hotels.bdp.circustrain.bigquery.copier;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryCopierFactoryTest {

  private @Mock CopierFactory copierFactory;
  private @Mock ExtractionService service;
  private @Mock Copier copier;

  private BigQueryCopierFactory factory;

  @Before
  public void setUp() {
    List<CopierFactory> copierFactories = Collections.singletonList(copierFactory);
    factory = new BigQueryCopierFactory(copierFactories, service);
    when(copierFactory.supportsSchemes(anyString(), anyString())).thenReturn(true);
  }

  @Test
  public void supportsSchemes() {
    assertThat(factory.supportsSchemes("gs://", "s3://"), is(true));
    assertThat(factory.getSupportedFactory(), is(copierFactory));
  }

  @Test
  public void doesntSupportSchemes() {
    when(copierFactory.supportsSchemes(anyString(), anyString())).thenReturn(false);
    assertThat(factory.supportsSchemes("gs://", "s3://"), is(false));
    assertNull(factory.getSupportedFactory());
  }

  @Test
  public void newInstance() {
    String eventId = "eventId";
    Path sourceBaseLocation = new Path("sourceBaseLocation");
    Path replicaLocation = new Path("replicaLocation");
    List<Path> sourceSubLocations = Collections.singletonList(new Path("subLocations"));
    Map<String, Object> copierOptions = new HashMap<>();
    when(copierFactory.newInstance(eventId, sourceBaseLocation, sourceSubLocations, replicaLocation, copierOptions))
        .thenReturn(copier);
    factory.supportsSchemes("gs://", "s3://");
    Copier bigQueryCopier = factory
        .newInstance(eventId, sourceBaseLocation, sourceSubLocations, replicaLocation, copierOptions);
    bigQueryCopier.copy();
    verify(service).extract();
    verify(copier).copy();
  }

}
