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
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionContainer;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;
import com.hotels.bdp.circustrain.bigquery.extraction.container.PostExtractionAction;

@RunWith(MockitoJUnitRunner.class)
public class DataExtractorTest {

  public @Rule ExpectedException expectedException = ExpectedException.none();

  private @Mock Storage storage;
  private @Mock Table table;
  private @Mock Job job;
  private @Mock JobStatus jobStatus;
  private @Mock Future future;
  private @Mock ExecutorService executorService;
  private DataExtractor extractor;
  private final ExtractionUri data = new ExtractionUri();
  private final TableId tableId = TableId.of("dataset", "table");
  private ExtractionContainer extractionContainer;

  @Before
  public void init() throws ExecutionException, InterruptedException {
    initExecutor();

    extractor = new DataExtractor(storage);
    extractionContainer = new ExtractionContainer(table, data, PostExtractionAction.RETAIN);

    when(table.getTableId()).thenReturn(tableId);
    when(table.extract(anyString(), anyString())).thenReturn(job);
    when(job.waitFor(Matchers.<RetryOption> anyVararg())).thenReturn(job);
    when(job.getStatus()).thenReturn(jobStatus);
  }

  private void initExecutor() throws InterruptedException, ExecutionException {
    when(future.get()).thenReturn(null);
    when(executorService.submit((Callable) anyObject())).thenReturn(future);
    when(executorService.submit(Matchers.argThat(new ArgumentMatcher<Callable>() {
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
  public void extract() throws InterruptedException {
    when(jobStatus.getError()).thenReturn(null);

    extractor.add(extractionContainer);
    List<ExtractionContainer> extracted = extractor.extract(executorService);

    verify(storage).create(any(BucketInfo.class));
    verify(table).extract(eq("csv"), eq(data.getUri()));
    assertThat(extracted.size(), is(1));
  }

  @Test
  public void jobNoLongerExists() throws InterruptedException {
    expectedException.expect(CircusTrainException.class);
    expectedException.expectMessage("job no longer exists");

    when(job.waitFor(Matchers.<RetryOption> anyVararg())).thenReturn(null);

    extractor.add(extractionContainer);
    extractor.extract(executorService);
  }

  @Test
  public void jobError() throws InterruptedException {
    BigQueryError bigQueryError = new BigQueryError("reason", "getExtractedDataBaseLocation", "message");
    expectedException.expect(CircusTrainException.class);
    expectedException.expectMessage(bigQueryError.getReason());
    expectedException.expectMessage(bigQueryError.getLocation());
    expectedException.expectMessage(bigQueryError.getMessage());

    when(jobStatus.getError()).thenReturn(bigQueryError);

    extractor.add(extractionContainer);
    extractor.extract(executorService);
  }

  @Test(expected = CircusTrainException.class)
  public void extractInterruptedFuture() throws InterruptedException, ExecutionException {
    when(future.get()).thenThrow(new InterruptedException());
    extractor.add(extractionContainer);
    extractor.extract(executorService);
  }

  @Test(expected = CircusTrainException.class)
  public void extractFuturewithExecutionException() throws InterruptedException, ExecutionException {
    when(future.get()).thenThrow(new ExecutionException(null));
    extractor.add(extractionContainer);
    extractor.extract(executorService);
  }

  @Test
  public void extractWithNoArguments() {
    List<ExtractionContainer> extracted = extractor.extract();
    assertThat(extracted.size(), is(0));
  }

  @Test
  public void extractWhenBucketAlreadyExists() {
    when(storage.get(anyString())).thenReturn(mock(Bucket.class));
    extractor.add(extractionContainer);
    List<ExtractionContainer> extracted = extractor.extract(executorService);
    verify(storage, never()).create((BucketInfo) anyObject());
    assertThat(extracted.size(), is(1));
  }

  @Test
  public void errorWhenCheckingIfBucketExists() {
    when(storage.get(anyString())).thenThrow(new StorageException(0, "message"));
    extractor.add(extractionContainer);
    List<ExtractionContainer> extracted = extractor.extract(executorService);
    verify(storage).create((BucketInfo) anyObject());
    assertThat(extracted.size(), is(1));
  }

}
