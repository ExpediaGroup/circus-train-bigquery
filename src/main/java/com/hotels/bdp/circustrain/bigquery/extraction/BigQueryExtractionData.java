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

import static com.hotels.bdp.circustrain.bigquery.extraction.BigQueryDataExtractionKey.makeKey;

import java.util.UUID;

import com.google.cloud.bigquery.Table;

class BigQueryExtractionData {

  private final String format = "csv";
  private final String dataBucket = "circus-train-bigquery-tmp-" + UUID.randomUUID().toString().toLowerCase();
  private final String dataKey = UUID.randomUUID().toString().toLowerCase();
  private final String dataUri = "gs://" + dataBucket + "/" + dataKey + "." + format;
  private final Table table;

  private boolean extracted = false;
  private boolean cleaned = false;

  BigQueryExtractionData(Table table) {
    this.table = table;
  }

  boolean isExtracted() {
    return extracted;
  }

  void setExtracted(boolean flag) {
    this.extracted = flag;
  }

  boolean isCleaned() {
    return cleaned;
  }

  void setCleaned(boolean flag) {
    this.cleaned = flag;
  }

  String getTableName() {
    return table.getTableId().getTable();
  }

  String getDatasetName() {
    return table.getTableId().getDataset();
  }

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

  Table getTable() {
    return table;
  }

  @Override
  public String toString() {
    return "Table: " + getDatasetName() + getTableName() + ". Location: " + getDataUri() + ".";
  }

  @Override
  public int hashCode() {
    return (makeKey(getDatasetName(), getTableName())).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    BigQueryExtractionData other = (BigQueryExtractionData) obj;

    String thisDatasetName = this.getDatasetName();
    String otherDatasetName = other.getDatasetName();
    if (!thisDatasetName.equals(otherDatasetName)) {
      return false;
    }

    String thisTableName = this.getTableName();
    String otherTableName = other.getTableName();
    if (!thisTableName.equals(otherTableName)) {
      return false;
    }

    return true;
  }
}
