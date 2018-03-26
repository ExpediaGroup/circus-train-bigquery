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
package com.hotels.bdp.circustrain.bigquery.copier;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.bigquery.context.CircusTrainBigQueryCondition;
import com.hotels.bdp.circustrain.bigquery.extraction.BigQueryDataExtractionManager;

@Component
@Profile({ Modules.REPLICATION })
@Order(Ordered.HIGHEST_PRECEDENCE)
@Conditional(CircusTrainBigQueryCondition.class)
public class BigQueryCopierFactory implements CopierFactory {

  private static final Logger log = LoggerFactory.getLogger(BigQueryCopierFactory.class);

  private final List<CopierFactory> delegates;
  private final BigQueryDataExtractionManager dataExtractionManager;

  private CopierFactory supportedFactory;

  @Autowired
  BigQueryCopierFactory(List<CopierFactory> delegates, BigQueryDataExtractionManager dataExtractionManager) {
    this.delegates = ImmutableList.copyOf(delegates);
    this.dataExtractionManager = dataExtractionManager;
  }

  @Override
  public boolean supportsSchemes(String sourceScheme, String replicaScheme) {
    for (CopierFactory factory : delegates) {
      if (factory.supportsSchemes(sourceScheme, replicaScheme)) {
        supportedFactory = factory;
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  public CopierFactory getSupportedFactory() {
    return supportedFactory;
  }

  @Override
  public Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    Copier copier = supportedFactory.newInstance(eventId, sourceBaseLocation, sourceSubLocations, replicaLocation,
        copierOptions);
    Copier bigQueryCopier = new BigQueryCopier(copier, dataExtractionManager);
    log.info("{} created copier which delegates to the copier produced by {}", this.getClass().getName(),
        supportedFactory.getClass().getName());
    return bigQueryCopier;
  }

  @Override
  public Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    return newInstance(eventId, sourceBaseLocation, Collections.<Path> emptyList(), replicaLocation, copierOptions);
  }
}
