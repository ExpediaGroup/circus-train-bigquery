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

final class ExtractionContainer {

  private BigQueryExtractionData extractionData;
  private Boolean deleteTable;
  private Boolean extracted;

  ExtractionContainer(BigQueryExtractionData extractionData, Boolean deleteTable, Boolean extracted) {
    this.extractionData = extractionData;
    this.deleteTable = deleteTable;
    this.extracted = extracted;
  }

  BigQueryExtractionData getExtractionData() {
    return extractionData;
  }

  void setExtractionData(BigQueryExtractionData extractionData) {
    this.extractionData = extractionData;
  }

  Boolean getDeleteTable() {
    return deleteTable;
  }

  void setDeleteTable(Boolean deleteTable) {
    this.deleteTable = deleteTable;
  }

  Boolean getExtracted() {
    return extracted;
  }

  void setExtracted(Boolean extracted) {
    this.extracted = extracted;
  }
}
