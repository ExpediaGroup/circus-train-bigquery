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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;

public class SchemaExtractor {

  private SchemaExtractor() {}

  public static String getSchemaFromStorage(Storage storage, ExtractionUri extractionUri) {
    Page<Blob> blobs = storage
        .list(extractionUri.getBucket(), BlobListOption.currentDirectory(),
            BlobListOption.prefix(extractionUri.getFolder() + "/"));
    Blob file = blobs.iterateAll().iterator().next();
    return getSchemaFromFile(file);
  }

  private static String getSchemaFromFile(Blob file) {
    try {
      SeekableByteArrayInput input = new SeekableByteArrayInput(file.getContent());
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
      DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input, datumReader);
      Schema schema = dataFileReader.getSchema();
      dataFileReader.close();
      return schema.toString();
    } catch (IOException e) {
      throw new CircusTrainException("Error getting schema from table", e);
    }
  }
}
