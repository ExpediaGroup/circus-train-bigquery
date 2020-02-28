# Circus Train BigQuery To Hive Replication

##  Overview
This [Circus Train](https://github.com/HotelsDotCom/circus-train) plugin enables the conversion of BigQuery tables to Hive.

## Start using
You can obtain Circus Train BigQuery from Maven Central:

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.hotels/circus-train-bigquery/badge.svg?subject=com.hotels:circus-train-bigquery)](https://maven-badges.herokuapp.com/maven-central/com.hotels/circus-train-bigquery) [![Build Status](https://travis-ci.org/HotelsDotCom/circus-train-bigquery.svg?branch=master)](https://travis-ci.org/HotelsDotCom/circus-train-bigquery) ![build](https://github.com/HotelsDotCom/circus-train/workflows/build/badge.svg?event=push) [![Coverage Status](https://coveralls.io/repos/github/HotelsDotCom/circus-train-bigquery/badge.svg?branch=master)](https://coveralls.io/github/HotelsDotCom/circus-train-bigquery?branch=master) ![GitHub license](https://img.shields.io/github/license/HotelsDotCom/circus-train.svg)

## Installation
In order to be used by Circus Train the above `circus-train-bigquery` jar file must be added to Circus Train's classpath.  It is highly recommended that the version of this library and the version of Circus Train are identical. The recommended way to make this extension available on the classpath is to store it in a standard location
and then add this to the `CIRCUS_TRAIN_CLASSPATH` environment variable (e.g. via a startup script):

    export CIRCUS_TRAIN_CLASSPATH=$CIRCUS_TRAIN_CLASSPATH:/opt/circus-train-big-query/lib/*

Another option is to place the jar file in the Circus Train `lib` folder which will then automatically load it but risks interfering with any Circus Train jobs that do not require the extension's functionality.

## Configuration
* Add the following to the Circus Train YAML configuration in order to load the BigQuery extension via Circus Train's [extension loading mechanism](https://github.com/HotelsDotCom/circus-train#loading-extensions):

```
     extension-packages: com.hotels.bdp.circustrain.bigquery
```
     
* Configure Circus Train as you would for a copy job from Google Cloud [Configuration](https://github.com/HotelsDotCom/circus-train/tree/master/circus-train-gcp)
* Provide the Google Cloud project ID that your BigQuery instance resides in as your `source-catalog` `hive-metastore-uris` parameter using the format `hive-metastore-uris: bigquery://<project-id>`
* To enable copying to Google Storage provide a path to your Google Credentials in the configuration under the gcp-security parameter.
* Provide your BigQuery dataset as `source-table` `database-name` and your BigQuery table name as `source-table` `table-name`

## Partition Generation
Circus Train BigQuery allows you to add partitions for one column to your Hive destination table upon replication from BigQuery to Hive. The partition must be a field present on your source BigQuery table. The user can configure the partition field by setting the `table-replications[n].copier-option : circus-train-bigquery-partition-by` property on a specific table replication within the Circus Train configuration file. The destination data will be repartitioned on the specified field.

If your destination data is partitioned you can also specify a partition filter using the `table-replications[n].copier-option : circus-train-bigquery-partition-filter` property. The partition filter is a BigQuery SQL query which will be used to filter the data being replicated - i.e. so one doesn't have to replicate the entire table on each run. You can think of the partition filter being executed as `select * from bq_db.table where ${filter-condition}`, with the `${filter-condition}` being substituted by your partition filter. Only the rows returned by this query will be replicated. See [partition filters](https://github.com/HotelsDotCom/circus-train#partition-filters) in the main Circus Train documentation for more information.

### Examples:

#### Simple Configuration
    extension-packages: com.hotels.bdp.circustrain.bigquery
    source-catalog:
      name: my-google-source-catalog
      hive-metastore-uris: bigquery://my-gcp-project-id
    replica-catalog:
      name: my-replica-catalog
      hive-metastore-uris: thrift://internal-shared-hive-metastore-elb-123456789.us-west-2.elb.foobaz.com:9083

    gcp-security:
      credential-provider: /home/hadoop/.gcp/my-gcp-project-01c26fd71db7.json 

    table-replications:
    - source-table:
        database-name: mysourcedb
        table-name: google_ads_data
      replica-table:
        database-name: myreplicadb
        table-name: bigquery_google_ads_data
        table-location: s3://mybucket/foo/baz/

#### Configuration with partition generation configured
    extension-packages: com.hotels.bdp.circustrain.bigquery
    source-catalog:
      ... see above ...

    table-replications:
      -
        source-table:
          database-name: bdp
          table-name: bigquery_source
        replica-table:
          database-name: bdp
          table-name: hive_replica
          table-location: s3://bucket/foo/bar/
        copier-options:
          circustrain-bigquery-partition-by: date

#### Configuration with partition filter configured
    extension-packages: com.hotels.bdp.circustrain.bigquery
    source-catalog:
      ... see above ...

    table-replications:
      -
        source-table:
          database-name: bdp
          table-name: bigquery_source
        replica-table:
          database-name: bdp
          table-name: hive_replica
          table-location: s3://bucket/foo/bar/
        copier-options:
          circustrain-bigquery-partition-by: date
          circustrain-bigquery-partition-filter: date BETWEEN TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -150 DAY) AND CURRENT_TIMESTAMP()


#### Technical Overview
The BigQuery plugin works by extracting the BigQuery table data into Google Storage using Google APIs, and then using Circus Train
listeners to convert the BigQuery metadata into a Hive table object. The data is then replicated from source to replica using 
the metadata from this mocked Hive table.

# Contact

## Mailing List
If you would like to ask any questions about or discuss Circus Train BigQuery please join the main Circus Train mailing list at 

  [https://groups.google.com/forum/#!forum/circus-train-user](https://groups.google.com/forum/#!forum/circus-train-user)

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2018-2020 Expedia, Inc.
