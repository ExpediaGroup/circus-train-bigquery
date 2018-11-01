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
import com.google.common.annotations.VisibleForTesting;

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

  @VisibleForTesting
  static String getSchemaFromFile(Blob file) {
    try {
      SeekableByteArrayInput input = new SeekableByteArrayInput(file.getContent());
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
      DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input, datumReader);
      Schema schema = dataFileReader.getSchema();
      dataFileReader.close();
      return schema.toString();
    } catch (IOException e) {
      throw new CircusTrainException("Error getting schema from table: " + e.getMessage());
    }
  }
}
