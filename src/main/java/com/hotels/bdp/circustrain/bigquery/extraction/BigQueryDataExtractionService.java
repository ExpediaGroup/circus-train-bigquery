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

import com.google.cloud.bigquery.Job;
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

  void extract(BigQueryExtractionData extractionData) {
    createBucket(extractionData);
    extractDataFromBigQuery(extractionData);
  }

  void cleanup(BigQueryExtractionData extractionData) {
    String dataBucket = extractionData.getDataBucket();
    String dataKey = extractionData.getDataKey();
    String format = extractionData.getFormat();
    String dataUri = extractionData.getDataUri();

    BlobId blobId = BlobId.of(dataBucket, dataKey + "." + format);
    boolean suceeded = storage.delete(blobId);
    if (!suceeded) {
      log.warn("Could not delete object {}", dataUri);
    }
    Bucket bucket = storage.get(dataBucket);
    suceeded = bucket.delete();
    if (!suceeded) {
      log.warn("Could not delete bucket {}", dataBucket);
    }
  }

  private void createBucket(BigQueryExtractionData extractionData) {
    String dataBucket = extractionData.getDataBucket();
    log.info("Creating bucket {}", dataBucket);
    BucketInfo bucketInfo = BucketInfo.of(dataBucket);
    storage.create(bucketInfo);
  }

  private void extractDataFromBigQuery(BigQueryExtractionData extractionData) {
    String dataset = extractionData.getDatasetName();
    String tableName = extractionData.getTableName();
    String format = extractionData.getFormat();
    String dataUri = extractionData.getDataUri();

    log.info("Extracting {}.{} to temporary location {}", dataset, tableName, dataUri);
    try {
      Job job = extractionData.getTable().extract(format, dataUri);
      Job completedJob = job.waitFor();
      if (completedJob != null && completedJob.getStatus().getError() == null) {
        // Job completed successfully
      } else {
        throw new CircusTrainException("Could not extract BigQuery table data to Google storage");
      }
    } catch (InterruptedException e) {
      throw new CircusTrainException(e);
    }
  }
}
