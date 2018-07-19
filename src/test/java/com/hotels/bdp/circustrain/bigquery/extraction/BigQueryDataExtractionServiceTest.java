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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.api.gax.paging.Page;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.api.CircusTrainException;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryDataExtractionServiceTest {

  public @Rule ExpectedException expectedException = ExpectedException.none();

  private @Mock Storage storage;
  private @Mock Table table;
  private @Mock Job job;
  private @Mock JobStatus jobStatus;

  private BigQueryDataExtractionService service;

  @Before
  public void init() {
    service = new BigQueryDataExtractionService(storage);
  }

  @Test
  public void extract() throws InterruptedException {
    BigQueryExtractionData data = new BigQueryExtractionData();
    TableId tableId = TableId.of("dataset", "table");
    when(table.extract(anyString(), anyString())).thenReturn(job);
    when(table.getTableId()).thenReturn(tableId);
    when(job.waitFor(Matchers.<RetryOption> anyVararg())).thenReturn(job);
    when(job.getStatus()).thenReturn(jobStatus);
    when(jobStatus.getError()).thenReturn(null);
    service.extract(table, data);
    verify(storage).create(any(BucketInfo.class));
    verify(table).extract(eq("csv"), eq(data.getDataUri()));
  }

  @Test
  public void cleanup() {
    BigQueryExtractionData data = new BigQueryExtractionData();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(storage.delete(any(BlobId.class))).thenReturn(true);
    Bucket bucket = mock(Bucket.class);
    when(bucket.delete()).thenReturn(true);
    when(storage.get(anyString())).thenReturn(bucket);

    List<Blob> blobs = new ArrayList<>();
    final int numBlobs = 10;
    for (int i = 0; i < numBlobs; ++i) {
      Blob blob = mock(Blob.class);
      BlobId blobId = BlobId.of(data.getDataBucket(), data.getDataKey() + i);
      when(blob.getBlobId()).thenReturn(blobId);
      blobs.add(blob);
    }

    Page pages = mock(Page.class);
    when(storage.list(anyString())).thenReturn(pages);
    when(pages.iterateAll()).thenReturn(blobs);

    service.cleanup(data);

    for (int i = 0; i < numBlobs; ++i) {
      verify(storage).delete(blobs.get(i).getBlobId());
    }

    verify(bucket).delete();
  }

  @Test
  public void cleanupWhenDeletionFailsOnBlobDoesntThrowException() {
    BigQueryExtractionData data = new BigQueryExtractionData();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(storage.delete(any(BlobId.class))).thenReturn(false);
    Bucket bucket = mock(Bucket.class);
    when(bucket.delete()).thenReturn(true);
    when(storage.get(anyString())).thenReturn(bucket);
    Blob blob = mock(Blob.class);
    List<Blob> blobs = ImmutableList.of(blob);
    Page pages = mock(Page.class);
    when(storage.list(anyString())).thenReturn(pages);
    when(pages.iterateAll()).thenReturn(blobs);

    service.cleanup(data);
    verify(storage).delete(any(BlobId.class));
    verify(bucket).delete();
  }

  @Test
  public void cleanupWhenDeletionThrowsExceptionOnBlobDoesntThrowException() {
    BigQueryExtractionData data = new BigQueryExtractionData();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(storage.delete(any(BlobId.class))).thenThrow(new StorageException(new IOException()));
    Bucket bucket = mock(Bucket.class);
    when(bucket.delete()).thenReturn(true);
    when(storage.get(anyString())).thenReturn(bucket);
    Blob blob = mock(Blob.class);
    List<Blob> blobs = ImmutableList.of(blob);
    Page pages = mock(Page.class);
    when(storage.list(anyString())).thenReturn(pages);
    when(pages.iterateAll()).thenReturn(blobs);

    service.cleanup(data);
    verify(storage).delete(any(BlobId.class));
    verify(bucket).delete();
  }

  @Test
  public void cleanupWhenDeletionThrowsExceptionOnBlobDoesntStillCleansRemainingBlobs() {
    BigQueryExtractionData data = new BigQueryExtractionData();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(storage.delete(any(BlobId.class))).thenReturn(true);
    Bucket bucket = mock(Bucket.class);
    when(bucket.delete()).thenReturn(true);
    when(storage.get(anyString())).thenReturn(bucket);

    BlobId firstCall = BlobId.of(data.getDataBucket(), data.getDataKey() + 1);
    BlobId thirdCall = BlobId.of(data.getDataBucket(), data.getDataKey() + 3);

    Blob blob = mock(Blob.class);
    when(blob.getBlobId()).thenReturn(firstCall).thenThrow(new StorageException(new IOException())).thenReturn(thirdCall);

    Page pages = mock(Page.class);
    when(storage.list(anyString())).thenReturn(pages);
    when(pages.iterateAll()).thenReturn(ImmutableList.of(blob, blob, blob));

    service.cleanup(data);

    verify(storage).delete(firstCall);
    verify(storage).delete(thirdCall);
    verify(storage, times(2)).delete(any(BlobId.class));
    verify(bucket).delete();
  }

  @Test
  public void cleanupWhenBucketDeletionThrowExceptionDoesntFailJob() {
    BigQueryExtractionData data = new BigQueryExtractionData();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(storage.delete(any(BlobId.class))).thenReturn(true);
    Bucket bucket = mock(Bucket.class);
    when(bucket.delete()).thenReturn(true);
    when(storage.get(anyString())).thenReturn(bucket);
    Blob blob = mock(Blob.class);
    List<Blob> blobs = ImmutableList.of(blob);
    Page pages = mock(Page.class);
    when(storage.list(anyString())).thenReturn(pages);
    when(pages.iterateAll()).thenReturn(blobs);
    when(storage.delete(any(BlobId.class))).thenThrow(new StorageException(new IOException()));

    service.cleanup(data);
    verify(storage).delete(any(BlobId.class));
    verify(bucket).delete();
  }

  @Test
  public void exceptionNotThrownWhenListFails() {
    BigQueryExtractionData data = new BigQueryExtractionData();
    when(storage.list(anyString())).thenThrow(new StorageException(new IOException()));
    service.cleanup(data);
    verify(storage, times(0)).delete(any(BlobId.class));
  }

  @Test
  public void jobNoLongerExists() throws InterruptedException {
    expectedException.expect(CircusTrainException.class);
    expectedException.expectMessage("job no longer exists");

    BigQueryExtractionData data = new BigQueryExtractionData();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(table.extract(anyString(), anyString())).thenReturn(job);
    when(job.waitFor(Matchers.<RetryOption> anyVararg())).thenReturn(null);
    service.extract(table, data);
  }

  @Test
  public void jobError() throws InterruptedException {
    BigQueryError bigQueryError = new BigQueryError("reason", "location", "message");
    expectedException.expect(CircusTrainException.class);
    expectedException.expectMessage(bigQueryError.getReason());
    expectedException.expectMessage(bigQueryError.getLocation());
    expectedException.expectMessage(bigQueryError.getMessage());

    BigQueryExtractionData data = new BigQueryExtractionData();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(table.extract(anyString(), anyString())).thenReturn(job);
    when(job.waitFor(Matchers.<RetryOption> anyVararg())).thenReturn(job);
    when(job.getStatus()).thenReturn(jobStatus);
    when(jobStatus.getError()).thenReturn(new BigQueryError("reason", "location", "message"));
    service.extract(table, data);
  }
}
