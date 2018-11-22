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
package com.hotels.bdp.circustrain.bigquery.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.springframework.stereotype.Component;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;

@Component
public class SchemaExtractor {

  public String getSchemaFromStorage(Storage storage, ExtractionUri extractionUri) {
    Page<Blob> blobs = storage
        .list(extractionUri.getBucket(), BlobListOption.currentDirectory(),
            BlobListOption.prefix(extractionUri.getFolder() + "/"));
    Blob blob = blobs.iterateAll().iterator().next();
    return extractSchemaFromFile(blob);
  }

  private String extractSchemaFromFile(Blob blob) {
    try (ReadableByteChannel reader = blob.reader();
      InputStream input = Channels.newInputStream(reader)) {
      Schema schema = extractAvroSchema(input);
      return schema.toString();
    } catch (IOException e) {
      throw new CircusTrainException("Error extracting schema from blob", e);
    }
  }

  private Schema extractAvroSchema(InputStream input) {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(input, datumReader)) {
      return dataFileReader.getSchema();
    } catch (IOException e) {
      throw new CircusTrainException("Error extracting schema from inputstream", e);
    }
  }
}
