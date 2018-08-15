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
package com.hotels.bdp.circustrain.bigquery.client;

import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.bigquery.CircusTrainBigQueryConstants;
import com.hotels.bdp.circustrain.bigquery.extraction.service.ExtractionService;
import com.hotels.bdp.circustrain.bigquery.table.service.TableServiceFactory;
import com.hotels.bdp.circustrain.bigquery.util.CircusTrainBigQueryMetastore;
import com.hotels.bdp.circustrain.core.metastore.ConditionalMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.exception.MetaStoreClientException;

@Component
public class BigQueryMetastoreClientFactory implements ConditionalMetaStoreClientFactory {

  private final BigQueryMetastoreClient metastoreClient;

  @Autowired
  BigQueryMetastoreClientFactory(
      CircusTrainBigQueryMetastore bigQueryMetastore,
      ExtractionService service,
      TableServiceFactory tableServiceFactory) {
    this.metastoreClient = new BigQueryMetastoreClient(bigQueryMetastore, service, new HiveTableCache(),
        tableServiceFactory);
  }

  @Override
  public boolean accepts(String uri) {
    return uri.startsWith(CircusTrainBigQueryConstants.ACCEPT_PREFIX);
  }

  @Override
  public CloseableMetaStoreClient newInstance(HiveConf conf, String name) throws MetaStoreClientException {
    return metastoreClient;
  }
}
