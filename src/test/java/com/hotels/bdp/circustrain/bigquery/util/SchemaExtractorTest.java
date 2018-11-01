package com.hotels.bdp.circustrain.bigquery.util;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;

import com.hotels.bdp.circustrain.api.CircusTrainException;
import com.hotels.bdp.circustrain.bigquery.extraction.container.ExtractionUri;

@RunWith(MockitoJUnitRunner.class)
public class SchemaExtractorTest {

  private @Mock Storage storage;
  private @Mock ExtractionUri extractionUri;
  private @Mock Page<Blob> blobs;
  private @Mock Blob blob;

  private String expectedSchema;

  @Before
  public void setUp() throws IOException {
    when(storage.list(anyString(), any(BlobListOption.class), any(BlobListOption.class))).thenReturn(blobs);
    when(blobs.iterateAll()).thenReturn(Arrays.asList(blob));
    when(blob.getContent()).thenReturn(getContentFromFileName("usa_names.avro"));
    expectedSchema = new String(getContentFromFileName("usa_names_schema.avsc"));
  }

  @Test
  public void typical() {
    String schema = SchemaExtractor.getSchemaFromStorage(storage, extractionUri);
    assertThat(schema, is(expectedSchema));
  }

  @Test(expected = CircusTrainException.class)
  public void getSchemaFromInvalidFile() throws IOException {
    when(blob.getContent()).thenReturn(getContentFromFileName("usa_names_schema.avsc"));
    SchemaExtractor.getSchemaFromStorage(storage, extractionUri);
  }

  public static byte[] getContentFromFileName(String name) throws IOException {
    File file = new File("src/test/resources/" + name);
    FileInputStream fStream = new FileInputStream(file);
    byte[] content = IOUtils.toByteArray(fStream);
    return content;
  }
}
