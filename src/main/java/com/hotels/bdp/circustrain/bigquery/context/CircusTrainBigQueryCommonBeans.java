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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.google.api.services.bigquery.BigqueryScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.api.Modules;
import com.hotels.bdp.circustrain.api.conf.SourceCatalog;
import com.hotels.bdp.circustrain.gcp.context.GCPSecurity;

@Profile({ Modules.REPLICATION })
@Conditional(CircusTrainBigQueryCondition.class)
@Configuration
class CircusTrainBigQueryCommonBeans {

  @Bean
  BigQuery bigQuery(GCPSecurity gcpSecurity, SourceCatalog sourceCatalog) throws IOException {
    return BigQueryOptions
        .newBuilder()
        .setCredentials(getCredential(gcpSecurity))
        .setProjectId(getProjectId(sourceCatalog))
        .build()
        .getService();
  }

  @Bean
  Storage googleStorage(GCPSecurity gcpSecurity, SourceCatalog sourceCatalog) throws IOException {
    return StorageOptions
        .newBuilder()
        .setCredentials(getCredential(gcpSecurity))
        .setProjectId(getProjectId(sourceCatalog))
        .build()
        .getService();
  }

  private GoogleCredentials getCredential(GCPSecurity gcpSecurity) throws IOException {
    File credentialFile = new File(gcpSecurity.getCredentialProvider());
    try (InputStream credentialStream = new FileInputStream(credentialFile)) {
      GoogleCredentials credential = GoogleCredentials.fromStream(credentialStream);
      if (credential.createScopedRequired()) {
        credential = credential.createScoped(BigqueryScopes.all());
      }
      return credential;
    }
  }

  @VisibleForTesting
  String getProjectId(SourceCatalog sourceCatalog) {
    return sourceCatalog.getHiveMetastoreUris().split("(bigquery:\\/\\/.*?)")[1];
  }
}
