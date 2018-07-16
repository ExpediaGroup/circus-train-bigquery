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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;

import com.hotels.bdp.circustrain.api.CircusTrainException;

public class BigQueryDataExtractionService {

  private static final Logger log = LoggerFactory.getLogger(BigQueryDataExtractionService.class);

  private final Storage storage;

  public BigQueryDataExtractionService(Storage storage) {
    this.storage = storage;
  }

  void extract(Table table, BigQueryExtractionData extractionData) {
    createBucket(extractionData);
    extractDataFromBigQuery(extractionData, table);
  }

  void cleanup(BigQueryExtractionData extractionData) {
    String dataBucket = extractionData.getBucket();
    String dataKey = extractionData.getKey();
    String dataUri = extractionData.getUri();

    BlobId blobId = BlobId.of(dataBucket, dataKey);
    boolean suceeded = storage.delete(blobId);
    if (!suceeded) {
      log.warn("Could not delete object {}", dataUri);
    }

    // TODO: Handle better
    Bucket bucket = storage.get(dataBucket);
    try {
      suceeded = bucket.delete();
      if (!suceeded) {
        log.warn("Could not delete bucket {}", dataBucket);
      } else {
        log.info("Deleted {}", dataBucket);
      }
    } catch (Exception e) {
      log.warn(e.toString());
    }
  }

  private void createBucket(BigQueryExtractionData extractionData) {
    String dataBucket = extractionData.getBucket();
    if (bucketExists(dataBucket)) {
      log.info("Bucket {} already exists. Skipped creation", dataBucket);
      return;
    }
    log.info("Creating bucket {}", dataBucket);
    BucketInfo bucketInfo = BucketInfo.of(dataBucket);
    storage.create(bucketInfo);
  }

  private boolean bucketExists(String bucketName) {
    return storage.get(bucketName, Storage.BucketGetOption.fields()) != null;
  }

  private void extractDataFromBigQuery(BigQueryExtractionData extractionData, Table table) {
    String dataset = table.getTableId().getDataset();
    String tableName = table.getTableId().getTable();
    String format = extractionData.getFormat();
    String dataUri = extractionData.getUri();

    log.info("Extracting {}.{} to temporary location {}", dataset, tableName, dataUri);
    try {
      Job job = table.extract(format, dataUri);
      Job completedJob = job.waitFor();
      if (completedJob == null) {
        throw new CircusTrainException("Error extracting BigQuery table data to Google storage, job no longer exists");
      } else if (completedJob.getStatus().getError() != null) {
        BigQueryError error = completedJob.getStatus().getError();
        throw new CircusTrainException("Error extracting BigQuery table data to Google storage: "
            + error.getMessage()
            + ", reason="
            + error.getReason()
            + ", location="
            + error.getLocation());
      } else {
        log.info("Job completed successfully");
      }
    } catch (InterruptedException e) {
      throw new CircusTrainException(e);
    }
  }
}
