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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableList;

import com.hotels.bdp.circustrain.api.CircusTrainException;

@RunWith(MockitoJUnitRunner.class)
public class DataCleanerTest {

  private DataCleaner cleaner;
  private @Mock Storage storage;
  private @Mock Table table;
  private @Mock Future future;
  private @Mock ExecutorService executorService;

  @Before
  public void init() throws ExecutionException, InterruptedException {
    initExecutor();
    cleaner = new DataCleaner(storage);
  }

  private void initExecutor() {
    Future future = mock(Future.class);
    try {
      when(future.get()).thenReturn(null);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    when(executorService.submit(any(Callable.class))).thenReturn(future);
    Mockito.when(executorService.submit(Mockito.argThat(new ArgumentMatcher<Callable>() {
      @Override
      public boolean matches(Object argument) {
        Callable callable = (Callable) argument;
        try {
          callable.call();
        } catch (Exception e) {
          throw new CircusTrainException(e);
        }
        return true;
      }

    }))).thenReturn(future);
  }

  @Test
  public void cleanup() {
    ExtractionUri data = new ExtractionUri();
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
      when(blob.exists()).thenReturn(true);
      BlobId blobId = BlobId.of(data.getBucket(), data.getKey() + i);
      when(blob.getBlobId()).thenReturn(blobId);
      blobs.add(blob);
    }

    Page pages = mock(Page.class);
    when(storage.list(anyString())).thenReturn(pages);
    when(pages.iterateAll()).thenReturn(blobs);

    cleaner.add(new ExtractionContainer(table, data, false));
    cleaner.cleanup(executorService);

    for (int i = 0; i < numBlobs; ++i) {
      verify(storage).delete(blobs.get(i).getBlobId());
    }

    verify(bucket).delete();
  }

  @Test
  public void cleanupWhenDeletionFailsOnBlobDoesntThrowException() {
    ExtractionUri data = new ExtractionUri();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(storage.delete(any(BlobId.class))).thenReturn(false);
    Bucket bucket = mock(Bucket.class);
    when(bucket.delete()).thenReturn(true);
    when(storage.get(anyString())).thenReturn(bucket);
    Blob blob = mock(Blob.class);
    when(blob.exists()).thenReturn(true);
    List<Blob> blobs = ImmutableList.of(blob);
    Page pages = mock(Page.class);
    when(storage.list(anyString())).thenReturn(pages);
    when(pages.iterateAll()).thenReturn(blobs);

    cleaner.add(new ExtractionContainer(table, data, false));
    cleaner.cleanup(executorService);

    verify(storage).delete(any(BlobId.class));
    verify(bucket).delete();
  }

  @Test
  public void cleanupWhenDeletionThrowsExceptionOnBlobDoesntThrowException() {
    ExtractionUri data = new ExtractionUri();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(storage.delete(any(BlobId.class))).thenThrow(new StorageException(new IOException()));
    Bucket bucket = mock(Bucket.class);
    when(bucket.delete()).thenReturn(true);
    when(storage.get(anyString())).thenReturn(bucket);
    Blob blob = mock(Blob.class);
    when(blob.exists()).thenReturn(true);
    List<Blob> blobs = ImmutableList.of(blob);
    Page pages = mock(Page.class);
    when(storage.list(anyString())).thenReturn(pages);
    when(pages.iterateAll()).thenReturn(blobs);

    cleaner.add(new ExtractionContainer(table, data, false));
    cleaner.cleanup(executorService);

    verify(storage).delete(any(BlobId.class));
    verify(bucket).delete();
  }

  @Test
  public void cleanupWhenDeletionThrowsExceptionOnBlobDoesntStillCleansRemainingBlobs() {
    ExtractionUri data = new ExtractionUri();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(storage.delete(any(BlobId.class))).thenReturn(true);
    Bucket bucket = mock(Bucket.class);
    when(bucket.delete()).thenReturn(true);
    when(storage.get(anyString())).thenReturn(bucket);

    BlobId firstCall = BlobId.of(data.getBucket(), data.getKey() + 1);
    BlobId thirdCall = BlobId.of(data.getBucket(), data.getKey() + 3);

    Blob blob = mock(Blob.class);
    when(blob.exists()).thenReturn(true);
    when(blob.getBlobId()).thenReturn(firstCall).thenThrow(new StorageException(new IOException())).thenReturn(
        thirdCall);

    Page pages = mock(Page.class);
    when(storage.list(anyString())).thenReturn(pages);
    when(pages.iterateAll()).thenReturn(ImmutableList.of(blob, blob, blob));

    cleaner.add(new ExtractionContainer(table, data, false));
    cleaner.cleanup(executorService);

    verify(storage).delete(firstCall);
    verify(storage).delete(thirdCall);
    verify(storage, times(2)).delete(any(BlobId.class));
    verify(bucket).delete();
  }

  @Test
  public void cleanupWhenBucketDeletionThrowExceptionDoesntFailJob() {
    ExtractionUri data = new ExtractionUri();
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    when(storage.delete(any(BlobId.class))).thenReturn(true);
    Bucket bucket = mock(Bucket.class);
    when(bucket.delete()).thenReturn(true);
    when(storage.get(anyString())).thenReturn(bucket);
    Blob blob = mock(Blob.class);
    when(blob.exists()).thenReturn(true);
    List<Blob> blobs = ImmutableList.of(blob);
    Page pages = mock(Page.class);
    when(storage.list(anyString())).thenReturn(pages);
    when(pages.iterateAll()).thenReturn(blobs);
    when(storage.delete(any(BlobId.class))).thenThrow(new StorageException(new IOException()));

    cleaner.add(new ExtractionContainer(table, data, false));
    cleaner.cleanup(executorService);

    verify(storage).delete(any(BlobId.class));
    verify(bucket).delete();
  }

  @Test
  public void exceptionNotThrownWhenListFails() {
    ExtractionUri data = new ExtractionUri();
    when(storage.list(anyString())).thenThrow(new StorageException(new IOException()));
    TableId tableId = TableId.of("dataset", "table");
    when(table.getTableId()).thenReturn(tableId);
    Bucket bucket = mock(Bucket.class);
    when(bucket.delete()).thenThrow(new StorageException(new IOException()));
    when(storage.get(anyString())).thenReturn(bucket);

    cleaner.add(new ExtractionContainer(table, data, false));
    cleaner.cleanup(executorService);

    verify(storage, times(0)).delete(any(BlobId.class));
  }
}
