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

import com.google.cloud.bigquery.Table;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;

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

  void cleanup() {
    while (!cleanupQueue.isEmpty()) {
      ExtractionContainer container = cleanupQueue.poll();
      Table table = container.getTable();
      ExtractionUri extractionUri = container.getExtractionUri();
      boolean deleteTable = container.getDeleteTable();
      if (deleteTable) {
        table.delete();
      }
      deleteBucketAndContents(extractionUri.getBucket());
    }
  }

  private void deleteBucketAndContents(String dataBucket) {
    if (bucketExists(dataBucket)) {
      deleteObjectsInBucket(dataBucket);
      deleteBucket(dataBucket);
      log.info("Deleted temporary bucket {} and its contents", dataBucket);
    }
  }

  private void deleteObjectsInBucket(String dataBucket) {
    try {
      Iterable<Blob> blobs = storage.list(dataBucket).iterateAll();
      for (Blob blob : blobs) {
        try {
          if (blob.exists()) {
            boolean suceeded = storage.delete(blob.getBlobId());
            if (suceeded) {
              log.info("Deleted object {}", blob);
            } else {
              log.warn("Could not delete object {}", blob);
            }
          }
        } catch (StorageException e) {
          log.warn("Error deleting object {} in bucket {}", blob, dataBucket, e);
        }
      }
    } catch (StorageException e) {
      log.warn("Error fetching objects in bucket {} for deletion", dataBucket, e);
    }
  }

  private void deleteBucket(String dataBucket) {
    try {
      Bucket bucket = storage.get(dataBucket);
      boolean suceeded = bucket.delete();
      if (suceeded) {
        log.info("Deleted bucket {}", dataBucket);
      } else {
        log.warn("Could not delete bucket {}", dataBucket);
      }
    } catch (StorageException e) {
      log.warn("Error deleting bucket {}", dataBucket, e);
    }
  }

  private boolean bucketExists(String bucketName) {
    try {
      return storage.get(bucketName) != null;
    } catch (StorageException e) {
      log.warn("Cannot verify whether bucket {} exists.", bucketName, e);
      return false;
    }
  }
}
