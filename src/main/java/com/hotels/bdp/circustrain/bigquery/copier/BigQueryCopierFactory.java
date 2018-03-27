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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;

import com.hotels.bdp.circustrain.api.copier.Copier;
import com.hotels.bdp.circustrain.api.copier.CopierFactory;
import com.hotels.bdp.circustrain.bigquery.extraction.BigQueryDataExtractionManager;

public class BigQueryCopierFactory implements CopierFactory {

  private final BigQueryDataExtractionManager dataExtractionManager;

  @Autowired
  public BigQueryCopierFactory(BigQueryDataExtractionManager dataExtractionManager) {
    this.dataExtractionManager = dataExtractionManager;
  }

  @Override
  public boolean supportsSchemes(String sourceScheme, String replicaScheme) {
    return true;
  }

  @Override
  public Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      List<Path> sourceSubLocations,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    return new BigQueryCopier(dataExtractionManager);
  }

  @Override
  public Copier newInstance(
      String eventId,
      Path sourceBaseLocation,
      Path replicaLocation,
      Map<String, Object> copierOptions) {
    return new BigQueryCopier(dataExtractionManager);
  }
}
