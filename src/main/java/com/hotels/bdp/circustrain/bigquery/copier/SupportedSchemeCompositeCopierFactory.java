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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.api.copier.CopierPathGenerator;
import com.hotels.bdp.circustrain.api.copier.CopierPathGeneratorParams;
import com.hotels.bdp.circustrain.api.copier.MetricsMerger;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.bigquery.extraction.BigQueryDataExtractionService;

public class SupportedSchemeCompositeCopierFactory implements CopierFactory {

  private class CompositeCopier implements Copier {

    private final List<Copier> copiers;
    private final MetricsMerger metricsMerger;

    CompositeCopier(List<Copier> copiers, MetricsMerger metricsMerger) {
      this.copiers = copiers;
      this.metricsMerger = metricsMerger;
    }

    @Override
    public Metrics copy() throws CircusTrainException {
      Metrics metrics = Metrics.NULL_VALUE;
      for (Copier copier : copiers) {
        Metrics copierMetrics = copier.copy();
        if (copierMetrics == null) {
          continue;
        }
        metrics = metricsMerger.merge(metrics, copierMetrics);
      }
      return metrics;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(BigQueryDataExtractionService.class);

  private final List<CopierFactory> delegates;
  private final CopierPathGenerator pathGenerator;
  private final MetricsMerger metricsMerger;

  public SupportedSchemeCompositeCopierFactory(
      List<CopierFactory> delegates,
      CopierPathGenerator pathGenerator,
      MetricsMerger metricsMerger) {
    this.delegates = new ArrayList<>(delegates);
    this.pathGenerator = pathGenerator;
    this.metricsMerger = metricsMerger;
  }

  @Override
  public boolean supportsSchemes(String sourceScheme, String replicaScheme) {
    List<CopierFactory> supportedFactories = new ArrayList<>();
    for (CopierFactory factory : delegates) {
      if (factory.supportsSchemes(sourceScheme, replicaScheme)) {
        log.info("Adding {} as a delegate", factory.getClass().getName());
        supportedFactories.add(factory);
      }
    }
    delegates.clear();
    delegates.addAll(supportedFactories);
    return !delegates.isEmpty();
  }

  @Override
  public Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    List<Copier> copiers = new ArrayList<>(delegates.size());
    int i = 0;
    for (CopierFactory delegate : delegates) {
      CopierPathGeneratorParams copierPathGeneratorParams = CopierPathGeneratorParams.newParams(i++, eventId,
          sourceBaseLocation, sourceSubLocations, replicaLocation, copierOptions);
      Path newSourceBaseLocation = pathGenerator.generateSourceBaseLocation(copierPathGeneratorParams);
      Path newReplicaLocation = pathGenerator.generateReplicaLocation(copierPathGeneratorParams);
      Copier copier = delegate.newInstance(eventId, newSourceBaseLocation, sourceSubLocations, newReplicaLocation,
          copierOptions);
      copiers.add(copier);
    }
    return new CompositeCopier(copiers, metricsMerger);
  }

  @Override
  public Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    List<Copier> copiers = new ArrayList<>(delegates.size());
    int i = 0;
    for (CopierFactory delegatee : delegates) {
      CopierPathGeneratorParams copierPathGeneratorParams = CopierPathGeneratorParams.newParams(i++, eventId,
          sourceBaseLocation, null, replicaLocation, copierOptions);
      Path newReplicaLocation = pathGenerator.generateReplicaLocation(copierPathGeneratorParams);
      Path newSourceBaseLocation = pathGenerator.generateSourceBaseLocation(copierPathGeneratorParams);
      Copier copier = delegatee.newInstance(eventId, newSourceBaseLocation, newReplicaLocation, copierOptions);
      copiers.add(copier);
    }
    return new CompositeCopier(copiers, metricsMerger);
  }
}
