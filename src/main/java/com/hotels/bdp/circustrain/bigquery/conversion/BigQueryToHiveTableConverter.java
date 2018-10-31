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
package com.hotels.bdp.circustrain.bigquery.conversion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.storage.Blob;

public class BigQueryToHiveTableConverter {

  private final Table table = new Table();
  private static final Logger log = LoggerFactory.getLogger(BigQueryToHiveTableConverter.class);

  public BigQueryToHiveTableConverter() {
    table.setDbName("default");
    table.setTableName("default");
    table.setOwner("");
    table.setLastAccessTime(0);
    table.setRetention(0);
    table.setParameters(Collections.<String, String> emptyMap());
    table.setPartitionKeys(Collections.<FieldSchema> emptyList());
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("");
    sd.setNumBuckets(-1);
    sd.setParameters(Collections.<String, String> emptyMap());
    sd.setBucketCols(Collections.<String> emptyList());
    sd.setSortCols(Collections.<Order> emptyList());
    sd.setCols(new ArrayList<FieldSchema>());
    sd.setInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
    sd.setCompressed(false);
    sd.setStoredAsSubDirectories(false);
    sd.setNumBuckets(-1);

    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.avro.AvroSerDe");
    // log.info("SERDE INFO ========" + serDeInfo);
    // Map<String, String> serDeParameters = new HashMap<>();
    // serDeParameters.put("avro.schema.literal", "<<get schema>>");
    // serDeParameters.put("serialization.format", "1");
    // serDeParameters.put("field.delim", ",");
    // serDeParameters.put("skip.header.line.count", "1");
    // serDeInfo.setParameters(serDeParameters);

    SkewedInfo si = new SkewedInfo();
    si.setSkewedColNames(Collections.<String> emptyList());
    si.setSkewedColValueLocationMaps(Collections.<List<String>, String> emptyMap());
    si.setSkewedColValues(Collections.<List<String>> emptyList());
    sd.setSkewedInfo(new SkewedInfo());
    sd.setSerdeInfo(serDeInfo);
    table.setSd(sd);
  }

  public Table convert() {
    return new Table(table);
  }

  public BigQueryToHiveTableConverter withDatabaseName(String dbName) {
    table.setDbName(dbName);
    return this;
  }

  public BigQueryToHiveTableConverter withTableName(String tableName) {
    table.setTableName(tableName);
    return this;
  }

  public BigQueryToHiveTableConverter withCols(List<FieldSchema> cols) {
    table.getSd().setCols(cols);
    return this;
  }

  public BigQueryToHiveTableConverter withLocation(String location) {
    table.getSd().setLocation(location);
    return this;
  }

  public BigQueryToHiveTableConverter withSchema(Blob file) {
    String schema = getSchemaFromFile(file);
    table.getSd().getSerdeInfo().putToParameters("avro.schema.literal", schema);
    log.info("SERDEINFO PARAMETERS ============== {}", table.getSd().getSerdeInfo().getParameters());
    return this;
  }

  public static String getSchemaFromFile(Blob file) {
    try {
      SeekableByteArrayInput input = new SeekableByteArrayInput(file.getContent());
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
      DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input, datumReader);
      org.apache.avro.Schema schema = dataFileReader.getSchema();
      dataFileReader.close();
      return schema.toString();
    } catch (IOException e) {
      log.info("ERROOOOOOOOOOOOR {}", e.getMessage());
      return null;
    }
  }

}
