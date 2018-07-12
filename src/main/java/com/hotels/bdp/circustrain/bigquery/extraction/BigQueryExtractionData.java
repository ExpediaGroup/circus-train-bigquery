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

import org.apache.commons.lang.builder.HashCodeBuilder;

class BigQueryExtractionData {

  private final String format = "csv";
  private final String dataBucket = "circus-train-bigquery-tmp-" + UUID.randomUUID().toString().toLowerCase();
  private final String dataKey = UUID.randomUUID().toString().toLowerCase();
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

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(format).append(dataBucket).append(dataKey).append(dataUri).toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    BigQueryExtractionData other = (BigQueryExtractionData) o;
    if (other.dataBucket != this.dataBucket) {
      return false;
    }
    if (other.dataKey != this.dataKey) {
      return false;
    }
    if (other.dataUri != this.dataUri) {
      return false;
    }
    if (other.format != this.format) {
      return false;
    }
    return true;
  }
}
