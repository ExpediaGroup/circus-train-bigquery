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
package com.hotels.bdp.circustrain.bigquery.extraction.service;

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

import com.google.cloud.bigquery.Table;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;

public class DataCleaner {

  private static final Logger log = LoggerFactory.getLogger(DataCleaner.class);

  private final Storage storage;
  private final Queue<ExtractionContainer> cleanupQueue;

  DataCleaner(Storage storage) {
    this(storage, new LinkedList<ExtractionContainer>());
  }

  DataCleaner(Storage storage, Queue<ExtractionContainer> cleanupQueue) {
    this.storage = storage;
    this.cleanupQueue = cleanupQueue;
  }

  public void add(ExtractionContainer container) {
    cleanupQueue.add(container);
  }

  List<ExtractionContainer> cleanup() {
    ExecutorService executorService = Executors.newFixedThreadPool(DEFAULT_THREADPOOL_SIZE);
    List<ExtractionContainer> deleted = cleanup(executorService);
    executorService.shutdown();
    return deleted;
  }

  List<ExtractionContainer> cleanup(ExecutorService executorService) {
    List<ExtractionContainer> deleted = new ArrayList<>(cleanupQueue);
    while (!cleanupQueue.isEmpty()) {
      ExtractionContainer container = cleanupQueue.poll();
      log.info("Cleaning data at location {}", container.getExtractionUri());

      Table table = container.getTable();
      boolean deleteTable = container.getDeleteTable();
      if (deleteTable) {
        log.debug("Deleted table. {}", container.getTable().getTableId());
        table.delete();
      }
      deleteBucketAndContents(executorService, container);
    }
    return deleted;
  }

  private void deleteBucketAndContents(ExecutorService executorService, ExtractionContainer container) {
    if (bucketExists(container)) {
      deleteObjectsInBucket(executorService, container);
      deleteBucket(container);
      log.debug("Deleted temporary bucket {} and its contents", container.getExtractionUri().getBucket());
    }
  }

  private void deleteObjectsInBucket(ExecutorService exec, final ExtractionContainer container) {
    try {
      Iterable<Blob> blobs = storage.list(container.getExtractionUri().getBucket()).iterateAll();
      List<Future<Void>> futures = new ArrayList<>();
      try {
        for (final Blob blob : blobs) {
          Future<Void> future = exec.submit(new Callable<Void>() {
            @Override
            public Void call() {
              deleteObject(blob);
              return null;
            }
          });
          futures.add(future);
        }
        for (Future future : futures) {
          future.get();
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new CircusTrainException(e);
      }
    } catch (StorageException e) {
      log.warn("Error fetching objects in bucket {} for deletion", container.getExtractionUri().getBucket(), e);
    }
  }

  private void deleteObject(Blob blob) {
    try {
      if (blob.exists()) {
        boolean suceeded = storage.delete(blob.getBlobId());
        if (suceeded) {
          log.debug("Deleted object {}", blob);
        } else {
          log.warn("Could not delete object {}", blob);
        }
      }
    } catch (StorageException e) {
      log.warn("Error deleting object {}", blob, e);
    }
  }

  private void deleteBucket(ExtractionContainer container) {
    try {
      Bucket bucket = storage.get(container.getExtractionUri().getBucket());
      boolean suceeded = bucket.delete();
      if (suceeded) {
        log.info("Deleted bucket {}", container.getExtractionUri().getBucket());
      } else {
        log.warn("Could not delete bucket {}", container.getExtractionUri().getBucket());
      }
    } catch (StorageException e) {
      log.warn("Error deleting bucket {}", container.getExtractionUri().getBucket(), e);
    }
  }

  private boolean bucketExists(ExtractionContainer container) {
    try {
      return storage.get(container.getExtractionUri().getBucket()) != null;
    } catch (StorageException e) {
      log.warn("Cannot verify whether bucket {} exists.", container.getExtractionUri().getBucket(), e);
      return false;
    }
  }
}
