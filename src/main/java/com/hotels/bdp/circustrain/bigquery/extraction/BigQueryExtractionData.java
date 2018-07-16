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

import static jodd.util.StringUtil.isEmpty;

import java.util.UUID;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class BigQueryExtractionData {

  private static final String DEFAULT_BUCKET_PREFIX = "circus-train-bigquery-tmp-";
  private static final String DEFAULT_FORMAT = "csv";

  private final String dataFormat;
  private final String dataBucket;
  private final String dataKey;
  private final String dataFolder;
  private final String dataUri;

  public BigQueryExtractionData() {
    this(DEFAULT_BUCKET_PREFIX + randomUri(), null, randomUri(), DEFAULT_FORMAT);
  }

  public BigQueryExtractionData(String bucket) {
    this(bucket, null, randomUri(), DEFAULT_FORMAT);
  }

  public BigQueryExtractionData(String bucket, String folder) {
    this(bucket, folder, randomUri(), DEFAULT_FORMAT);
  }

  public BigQueryExtractionData(String bucket, String folder, String file) {
    this(bucket, folder, file, DEFAULT_FORMAT);
  }

  public BigQueryExtractionData(String bucket, String folder, String fileName, String fileFormat) {
    this.dataBucket = bucket;
    this.dataFolder = folder;
    this.dataFormat = fileFormat;
    if (isEmpty(dataFolder)) {
      this.dataKey = fileName + "." + fileFormat;
    } else {
      this.dataKey = folder + "/" + fileName + "." + fileFormat;
    }
    this.dataUri = "gs://" + dataBucket + "/" + dataKey;
  }

  public static String randomUri() {
    return UUID.randomUUID().toString().toLowerCase();
  }

  String getFolder() {
    return dataFolder;
  }

  String getKey() {
    return dataKey;
  }

  String getUri() {
    return dataUri;
  }

  String getFormat() {
    return dataFormat;
  }

  String getBucket() {
    return dataBucket;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(dataFormat).append(dataBucket).append(dataKey).append(dataUri).toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof BigQueryExtractionData) {
      BigQueryExtractionData other = (BigQueryExtractionData) o;
      if (!other.dataBucket.equals(this.dataBucket)) {
        return false;
      }
      if (!other.dataKey.equals(this.dataKey)) {
        return false;
      }
      if (!other.dataUri.equals(this.dataUri)) {
        return false;
      }
      if (!other.dataFormat.equals(this.dataFormat)) {
        return false;
      }
      return true;
    } else {
      return false;
    }
  }
}
