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

import static com.hotels.bdp.circustrain.bigquery.RuntimeConstants.DEFAULT_THREADPOOL_SIZE;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.circustrain.api.CircusTrainException;

public class DataExtractor {

  private static final Logger log = LoggerFactory.getLogger(DataExtractor.class);

  private final Storage storage;
  private final Queue<ExtractionContainer> extractionQueue;

  DataExtractor(Storage storage) {
    this(storage, new LinkedList<ExtractionContainer>());
  }

  @VisibleForTesting
  DataExtractor(Storage storage, Queue<ExtractionContainer> extractionQueue) {
    this.storage = storage;
    this.extractionQueue = extractionQueue;
  }

  public void add(ExtractionContainer container) {
    extractionQueue.add(container);
  }

  List<ExtractionContainer> extract(ExecutorService executorService) {
    List<Future<ExtractionContainer>> futures = new ArrayList<>();
    while (!extractionQueue.isEmpty()) {
      futures.add(executorService.submit(new ExtractionTask(extractionQueue.poll())));
    }

    List<ExtractionContainer> extracted = new ArrayList<>();
    for (Future<ExtractionContainer> future : futures) {
      try {
        extracted.add(future.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new CircusTrainException("Couldn't extract table data", e);
      }
    }
    return extracted;
  }

  List<ExtractionContainer> extract() {
    ExecutorService executorService = Executors.newFixedThreadPool(DEFAULT_THREADPOOL_SIZE);
    List<ExtractionContainer> extracted = extract(executorService);
    executorService.shutdownNow();
    return extracted;
  }

  private synchronized void createBucket(ExtractionUri extractionUri) {
    String dataBucket = extractionUri.getBucket();
    if (bucketExists(dataBucket)) {
      log.debug("Bucket {} already exists. Skipped creation", dataBucket);
      return;
    }
    log.debug("Creating bucket {}", dataBucket);
    BucketInfo bucketInfo = BucketInfo.of(dataBucket);
    storage.create(bucketInfo);
  }

  private synchronized boolean bucketExists(String bucketName) {
    try {
      return storage.get(bucketName) != null;
    } catch (StorageException e) {
      log.warn("Cannot verify whether bucket {} exists.", bucketName, e);
      return false;
    }
  }

  private void extractDataFromBigQuery(ExtractionUri extractionUri, Table table) {
    String format = extractionUri.getFormat();
    String dataUri = extractionUri.getUri();

    try {
      Job job = table.extract(format, dataUri);
      Job completedJob = job.waitFor();
      if (completedJob == null) {
        throw new CircusTrainException("Error extracting BigQuery table data to Google storage, job no longer exists");
      }
      if (completedJob.getStatus().getError() != null) {
        BigQueryError error = completedJob.getStatus().getError();
        throw new CircusTrainException("Error extracting BigQuery table data to Google storage: "
            + error.getMessage()
            + ", reason="
            + error.getReason()
            + ", location="
            + error.getLocation());
      }
    } catch (InterruptedException e) {
      throw new CircusTrainException(e);
    }
  }

  private class ExtractionTask implements Callable<ExtractionContainer> {

    private final ExtractionContainer container;

    private ExtractionTask(ExtractionContainer container) {
      this.container = container;
    }

    @Override
    public ExtractionContainer call() throws Exception {
      return extract();
    }

    private ExtractionContainer extract() {
      log.info("Extracting table data to {}", container.getExtractionUri());
      Table table = container.getTable();
      ExtractionUri extractionUri = container.getExtractionUri();
      createBucket(extractionUri);
      extractDataFromBigQuery(extractionUri, table);
      return container;
    }
  }
}
