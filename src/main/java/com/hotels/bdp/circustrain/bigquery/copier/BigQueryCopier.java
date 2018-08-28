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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.metrics.Metrics;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;

public class BigQueryCopier implements Copier {

  private static final Logger log = LoggerFactory.getLogger(BigQueryCopier.class);

  private final Copier copier;
  private final ExtractionService service;

  BigQueryCopier(Copier copier, ExtractionService service) {
    this.copier = copier;
    this.service = service;
  }

  @Override
  public Metrics copy() throws CircusTrainException {
    log.info("Extracting table data for copying by {}", copier.getClass().getName());
    service.extract();
    log.info("Delegating to {} to execute copying of the data", copier.getClass().getName());
    return copier.copy();
  }
}
