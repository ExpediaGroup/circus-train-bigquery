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
package com.hotels.bdp.circustrain.bigquery.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;

import com.hotels.bdp.circustrain.api.metastore.CloseableMetaStoreClient;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientException;
import com.hotels.bdp.circustrain.api.metastore.MetaStoreClientFactory;
import com.hotels.bdp.circustrain.bigquery.extraction.BigQueryDataExtractionManager;

public class BigQueryMetastoreClientFactory implements MetaStoreClientFactory {

  public static final String ACCEPT_PREFIX = "bigquery://";

  private static final Logger log = LoggerFactory.getLogger(BigQueryMetastoreClientFactory.class);

  private final BigQuery bigQuery;
  private final BigQueryDataExtractionManager dataExtractionManager;

  public BigQueryMetastoreClientFactory(BigQuery bigQuery, BigQueryDataExtractionManager dataExtractionManager) {
    this.bigQuery = bigQuery;
    this.dataExtractionManager = dataExtractionManager;
  }

  @Override
  public boolean accepts(String uri) {
    return uri.startsWith(ACCEPT_PREFIX);
  }

  @Override
  public CloseableMetaStoreClient newInstance(HiveConf conf, String name) throws MetaStoreClientException {
    log.info("Creating new instance of BigQueryMetastoreClient");
    return new BigQueryMetastoreClient(bigQuery, dataExtractionManager);
  }
}
