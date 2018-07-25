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
package com.hotels.bdp.circustrain.bigquery.context;

import org.springframework.boot.bind.RelaxedPropertyResolver;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.PropertyResolver;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class CircusTrainBigQueryCondition implements Condition {

  private static final String SOURCE_CATALOG_HIVE_METASTORE_URIS = "source-catalog.hive-metastore-uris";

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata annotatedTypeMetadata) {
    PropertyResolver resolver = new RelaxedPropertyResolver(context.getEnvironment());
    String hiveMetastoreUris = resolver.getProperty(SOURCE_CATALOG_HIVE_METASTORE_URIS);
    if (hiveMetastoreUris != null && hiveMetastoreUris.startsWith(CircusTrainBigQueryConstants.ACCEPT_PREFIX)) {
      return true;
    }
    return false;
  }
}
