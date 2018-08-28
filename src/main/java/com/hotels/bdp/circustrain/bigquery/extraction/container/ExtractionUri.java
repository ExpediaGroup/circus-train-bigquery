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
package com.hotels.bdp.circustrain.bigquery.extraction.container;

import static com.hotels.bdp.circustrain.bigquery.util.RandomStringGenerationUtils.randomUri;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class ExtractionUri {

  private static final String DEFAULT_BUCKET_PREFIX = "circus-train-bigquery-tmp-";
  // Wildcard key enables sharding of exported data > 1GB - https://cloud.google.com/bigquery/docs/exporting-data
  private static final String SHARD_DATA_POSTFIX = "-*";
  private static final String DEFAULT_FORMAT = "csv";

  private final String dataFormat;
  private final String dataBucket;
  private final String dataKey;
  private final String dataFolder;
  private final String dataUri;

  public ExtractionUri() {
    this(DEFAULT_BUCKET_PREFIX + randomUri());
  }

  public ExtractionUri(String bucket) {
    this(bucket, randomUri());
  }

  public ExtractionUri(String bucket, String folder) {
    this(bucket, folder, randomUri() + SHARD_DATA_POSTFIX, DEFAULT_FORMAT);
  }

  public ExtractionUri(String bucket, String folder, String fileName) {
    this(bucket, folder, fileName, DEFAULT_FORMAT);
  }

  public ExtractionUri(String bucket, String folder, String fileName, String fileFormat) {
    dataBucket = bucket;
    dataFolder = folder;
    dataFormat = fileFormat;
    dataKey = folder + "/" + fileName + "." + fileFormat;
    dataUri = "gs://" + dataBucket + "/" + dataKey;
  }

  public String getFolder() {
    return dataFolder;
  }

  public String getKey() {
    return dataKey;
  }

  public String getUri() {
    return dataUri;
  }

  public String getFormat() {
    return dataFormat;
  }

  public String getBucket() {
    return dataBucket;
  }

  public String getTableLocation() {
    return "gs://" + dataBucket + "/" + dataFolder + "/";
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(dataFormat).append(dataBucket).append(dataKey).append(dataUri).toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ExtractionUri) {
      ExtractionUri other = (ExtractionUri) o;
      if (!other.dataBucket.equals(dataBucket)) {
        return false;
      }
      if (!other.dataKey.equals(dataKey)) {
        return false;
      }
      if (!other.dataUri.equals(dataUri)) {
        return false;
      }
      if (!other.dataFormat.equals(dataFormat)) {
        return false;
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return getUri();
  }
}
