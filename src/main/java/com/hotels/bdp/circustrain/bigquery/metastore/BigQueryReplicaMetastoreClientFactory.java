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
package com.hotels.bdp.circustrain.bigquery.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.hotels.bdp.circustrain.core.metastore.ConditionalMetaStoreClientFactory;
import com.hotels.bdp.circustrain.core.metastore.ThriftHiveMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.exception.MetaStoreClientException;

@Component
@Order(Ordered.LOWEST_PRECEDENCE - 20)
public class BigQueryReplicaMetastoreClientFactory implements ConditionalMetaStoreClientFactory {

  private static final Logger log = LoggerFactory.getLogger(BigQuerySourceMetastoreClientFactory.class);

  private ConditionalMetaStoreClientFactory factory;

  BigQueryReplicaMetastoreClientFactory() {
    factory = new ThriftHiveMetaStoreClientFactory();
  }

  @Override
  public boolean accepts(String uri) {
    return factory.accepts(uri);
  }

  @Override
  public CloseableMetaStoreClient newInstance(HiveConf conf, String name) throws MetaStoreClientException {
    log.info("Creating DEBUG replica metastore client");
    try {
      return new BigQueryReplicaMetastoreClient(factory.newInstance(conf, name));
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }
}
