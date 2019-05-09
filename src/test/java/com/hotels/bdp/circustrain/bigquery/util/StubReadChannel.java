/**
 * Copyright (C) 2018-2019 Expedia, Inc.
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
package com.hotels.bdp.circustrain.bigquery.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;

public class StubReadChannel implements ReadChannel {

  private ReadableByteChannel delegate;

  public StubReadChannel(ReadableByteChannel delegate) {
    this.delegate = delegate;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return delegate.read(dst);
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public void close() {
    try {
      delegate.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void seek(long position) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setChunkSize(int chunkSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RestorableState<ReadChannel> capture() {
    throw new UnsupportedOperationException();
  }

}
