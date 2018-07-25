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

import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;

import com.hotels.bdp.circustrain.api.CircusTrainException;

public class DataExtractor {

  private static final Logger log = LoggerFactory.getLogger(DataExtractor.class);

  private final Storage storage;
  private final Queue<ExtractionContainer> extractionQueue;

  public DataExtractor(Storage storage) {
    this(storage, new LinkedList<ExtractionContainer>());
  }

  public DataExtractor(Storage storage, Queue<ExtractionContainer> extractionQueue) {
    this.storage = storage;
    this.extractionQueue = extractionQueue;
  }

  public void add(ExtractionContainer container) {
    extractionQueue.add(container);
  }

  public void extract() {
    while (!extractionQueue.isEmpty()) {
      ExtractionContainer container = extractionQueue.poll();
      Table table = container.getTable();
      ExtractionUri extractionUri = container.getExtractionUri();
      createBucket(extractionUri);
      extractDataFromBigQuery(extractionUri, table);
    }
  }

  private void createBucket(ExtractionUri extractionUri) {
    String dataBucket = extractionUri.getBucket();
    if (bucketExists(dataBucket)) {
      log.debug("Bucket {} already exists. Skipped creation", dataBucket);
      return;
    }
    log.info("Creating bucket {}", dataBucket);
    BucketInfo bucketInfo = BucketInfo.of(dataBucket);
    storage.create(bucketInfo);
  }

  private boolean bucketExists(String bucketName) {
    return storage.get(bucketName, Storage.BucketGetOption.fields()) != null;
  }

  private void extractDataFromBigQuery(ExtractionUri extractionUri, Table table) {
    String format = extractionUri.getFormat();
    String dataUri = extractionUri.getUri();
    String baseLocation = extractionUri.getBucket() + "/" + extractionUri.getFolder();

    log.info("Extracting table data to temporary location gs://{}/", baseLocation);
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
