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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
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

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;

@RunWith(MockitoJUnitRunner.class)
public class DataCleanerTest {

  private @Mock Bucket bucket;
  private @Mock Blob blob;
  private @Mock ExecutorService executorService;
  private @Mock Future future;
  private @Mock Page pages;
  private @Mock Storage storage;
  private @Mock Table table;
  private DataCleaner cleaner;
  private final ExtractionUri extractionUri = new ExtractionUri();
  private final TableId tableId = TableId.of("dataset", "table");
  private ExtractionContainer extractionContainer;

  @Before
  public void init() throws ExecutionException, InterruptedException {
    initExecutor();

    cleaner = new DataCleaner(storage);
    extractionContainer = new ExtractionContainer(table, extractionUri);

    when(table.getTableId()).thenReturn(tableId);
    when(future.get()).thenReturn(null);
    when(storage.get(anyString())).thenReturn(bucket);
    when(bucket.delete()).thenReturn(true);
    when(storage.list(anyString())).thenReturn(pages);
    when(blob.exists()).thenReturn(true);
    when(pages.iterateAll()).thenReturn(Collections.singletonList(blob));
  }

  private void initExecutor() throws InterruptedException, ExecutionException {
    when(executorService.submit(any(Callable.class))).thenReturn(future);
    Mockito.when(executorService.submit(Matchers.argThat(new ArgumentMatcher<Callable>() {
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
    List<Blob> blobs = new ArrayList<>();
    final int numBlobs = 10;
    for (int i = 0; i < numBlobs; ++i) {
      Blob blob = mock(Blob.class);
      when(blob.exists()).thenReturn(true);
      BlobId blobId = BlobId.of(extractionUri.getBucket(), extractionUri.getKey() + i);
      when(blob.getBlobId()).thenReturn(blobId);
      blobs.add(blob);
    }

    when(pages.iterateAll()).thenReturn(blobs);
    when(storage.delete(any(BlobId.class))).thenReturn(true);

    cleaner.add(extractionContainer);
    List<ExtractionContainer> cleaned = cleaner.cleanup(executorService);

    for (int i = 0; i < numBlobs; ++i) {
      verify(storage).delete(blobs.get(i).getBlobId());
    }

    verify(bucket).delete();
    assertThat(cleaned.get(0), is(extractionContainer));
  }

  @Test
  public void cleanupWhenDeletionFailsOnBlobDoesntThrowException() {
    when(storage.delete(any(BlobId.class))).thenReturn(false);

    cleaner.add(extractionContainer);
    List<ExtractionContainer> cleaned = cleaner.cleanup(executorService);

    verify(storage).delete(any(BlobId.class));
    verify(bucket).delete();
    assertThat(cleaned.get(0), is(extractionContainer));
  }

  @Test
  public void cleanupWhenDeletionThrowsExceptionOnBlobDoesntThrowException() {
    when(storage.delete(any(BlobId.class))).thenThrow(new StorageException(new IOException()));

    cleaner.add(extractionContainer);
    List<ExtractionContainer> cleaned = cleaner.cleanup(executorService);

    verify(storage).delete(any(BlobId.class));
    verify(bucket).delete();
    assertThat(cleaned.get(0), is(extractionContainer));
  }

  @Test
  public void cleanupWhenDeletionThrowsExceptionOnBlobDoesntStillCleansRemainingBlobs() {
    BlobId firstCall = BlobId.of(extractionUri.getBucket(), extractionUri.getKey() + 1);
    BlobId thirdCall = BlobId.of(extractionUri.getBucket(), extractionUri.getKey() + 3);
    when(storage.delete(any(BlobId.class))).thenReturn(true);
    when(blob.getBlobId()).thenReturn(firstCall).thenThrow(new StorageException(new IOException()))
    .thenReturn(thirdCall);
    when(pages.iterateAll()).thenReturn(Arrays.asList(blob, blob, blob));

    cleaner.add(extractionContainer);
    List<ExtractionContainer> cleaned = cleaner.cleanup(executorService);

    verify(storage).delete(firstCall);
    verify(storage).delete(thirdCall);
    verify(storage, times(2)).delete(any(BlobId.class));
    verify(bucket).delete();
    assertThat(cleaned.get(0), is(extractionContainer));
  }

  @Test
  public void cleanupWhenBucketDeletionThrowExceptionDoesntFailJob() {
    when(storage.delete(any(BlobId.class))).thenThrow(new StorageException(new IOException()));
    cleaner.add(extractionContainer);
    List<ExtractionContainer> cleaned = cleaner.cleanup(executorService);
    verify(storage).delete(any(BlobId.class));
    verify(bucket).delete();
    assertThat(cleaned.get(0), is(extractionContainer));
  }

  @Test
  public void exceptionNotThrownWhenListFails() {
    when(storage.list(anyString())).thenThrow(new StorageException(new IOException()));
    cleaner.add(extractionContainer);
    List<ExtractionContainer> cleaned = cleaner.cleanup(executorService);
    verify(storage, times(0)).delete(any(BlobId.class));
    assertThat(cleaned.get(0), is(extractionContainer));
  }

  @Test
  public void cleanupNoArguments() {
    cleaner.add(extractionContainer);
    List<ExtractionContainer> cleaned = cleaner.cleanup();
    assertThat(cleaned.get(0), is(extractionContainer));
  }

  @Test
  public void cleanupTable() {
    ExtractionContainer container = new ExtractionContainer(table, extractionUri);
    cleaner.add(container);
    List<ExtractionContainer> cleaned = cleaner.cleanup();
    verify(table).delete();
    assertThat(cleaned.get(0), is(container));
  }

  @Test
  public void cleanupAbsentBucket() {
    when(storage.get(anyString())).thenReturn(null);
    cleaner.add(extractionContainer);
    List<ExtractionContainer> cleaned = cleaner.cleanup();
    verify(storage, times(0)).delete(any(BlobId.class));
    assertThat(cleaned.get(0), is(extractionContainer));
  }

  @Test
  public void cleanupAbsentBlob() {
    when(blob.exists()).thenReturn(false);
    cleaner.add(extractionContainer);
    List<ExtractionContainer> cleaned = cleaner.cleanup();
    verify(storage, times(0)).delete(any(BlobId.class));
    assertThat(cleaned.get(0), is(extractionContainer));
  }
}
