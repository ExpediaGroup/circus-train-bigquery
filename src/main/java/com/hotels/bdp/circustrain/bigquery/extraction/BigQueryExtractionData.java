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
package com.hotels.bdp.circustrain.bigquery.extraction;

import java.util.UUID;

class BigQueryExtractionData {

  private final String format = "csv";
  private final String dataBucket = "circus-train-bigquery-tmp-" + UUID.randomUUID().toString().toLowerCase();
  // Wildcard key enables sharding of exported data > 1GB - https://cloud.google.com/bigquery/docs/exporting-data
  private final String dataKey = UUID.randomUUID().toString().toLowerCase() + "-*";
  private final String dataUri = "gs://" + dataBucket + "/" + dataKey + "." + format;

  String getDataKey() {
    return dataKey;
  }

  String getDataUri() {
    return dataUri;
  }

  String getFormat() {
    return format;
  }

  String getDataBucket() {
    return dataBucket;
  }
}
