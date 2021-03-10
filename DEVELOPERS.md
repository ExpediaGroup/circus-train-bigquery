![Circus Train BigQuery.](circus-train-bigquery.png "Moving data from Google BigQuery to Hive.")

## Overview 
Circus Train BigQuery is an extension of Circus Train which allows tables in Google BigQuery to be replicated to Hive.

This document contains a collection of notes put together by developers who have worked on the project to provide some explanations of the code and how it works. This is not completely exhaustive of all the inner workings of the project, so do feel free to add more information or detail. 


## README.md

First and foremost, it's worth having a read through the Circus Train [README.md](https://github.com/HotelsDotCom/circus-train/blob/main/README.md) file and the Circus Train BigQuery [README.md](https://github.com/HotelsDotCom/circus-train-bigquery/blob/main/README.md). These are pretty extensive guides containing a lot of info on the projects, including how to run each of them and the different configurations which can be used. 

It may also be useful to read through the [DEVELOPERS.md](https://github.com/HotelsDotCom/circus-train/blob/main/DEVELOPERS.md) file for Circus Train. 


## Basic description

At a high level, CTBQ uses the Replication and S3-MapReduce Copier integration points provided by CT to replicate data out of BigQuery and into Hive. It first runs a query to extract the data out of BQ into a temporary BQ table (applying any necessary partition filters) and then uses Google's BigQuery and Storage APIs to create a Job which extracts that data out of this temporary table onto Google Cloud Storage (GCS) as Avro files. A Hive table object (i.e. not a real Hive table) is then put "on top" of this data and Circus Train is instructed to replicate the data from this GCS source to the target as normal.


### Partitioned Tables

* A temporary table is created by applying a partition filter to the source table. 
* The data from this temporary table extracted to GCS. 
* Circus Train then uses this GCS location to perform the replication to Hive.


### Unpartitioned Tables

* The data from the entire source table is extracted to GCS.
* This GCS location is used by Circus Train to perform the replication. 


## Class Explanations
As mentioned, BigQuery is an extension of Circus Train so processing will begin in the Circus Train code. Control is given to BigQuery which will set up the table to be replicated, and then Circus Train will perform the actual replication. 


### Key

**name** - Title for the class being described

(CT) - Indicates that the class is a part of the Circus Train code 

`<Name>` -  Capitalised names indicate classes

`<name>` - Lowercase names indicate methods


### Classes

**Locomotive** (CT)

* Creates a new `Replication` (CT) object using the `ReplicationFactory` (CT) and calls `replicate` on it.

**ReplicationFactory* (CT)

* Returns a `Replication` (CT) object, the type depends on whether the source table is partitioned or not.
* Creates a `Source` (CT) object which refers back to `HiveEndpoint` (CT) which then calls `getTable` on the `BigQueryMetastoreClient` object.  

**Replication** (CT)

* Either `PartitionedTableReplication` (CT) or `UnpartitionedTableReplication` (CT).
* Uses the `BigQueryCopierFactory` to generate the `BigQueryCopier`, then calls `copy` to copy over the table data. 
* After that will update the metadata of the table. 


**BigQueryMetastoreClientFactory**

* Checks the given URI is acceptable for BigQuery.
* Returns a new instance of the `BigQueryMetastoreClient`.


**BigQueryMetastoreClient**

* Registers a container with the `ExtractionService`.
* Container has an `UpdateTableSchemaAction` to be executed by the `ExtractionService`.
* Returns a Hive table - based on the BigQuery table. Creates the cached table object.


**BigQueryMetastore**

* Configures and runs a Query Job in GBQ which will execute the partition filter query on the source table, loads the result to a temporary GBQ table.


**TableServiceFactory**

* Will create either a `PartitionedTableService` or an `UnpartitionedTableService`, based on whether the Hive table being replicated is partitioned or not.
* Created inside `BigQueryMetastoreClient`.


**UnpartitionedTableService**

* Takes the hive table in the constructor. 
* Returns an empty list of partitions. 


**PartitionedTableService**

* Pass in `BigQueryTableFilterer` in the constructor. This executes the filter query onto the table.
* Calls generate on the `HivePartitionGenerator`.


**HivePartitionGenerator** 

* Calls the `BigQueryToHivePartitionConverter` to generate a new basic partition.
* Uses `BigQueryPartitionGenerator` to add data from BQ table to the partition.
* Sets partition parameters.


**BigQueryPartitionGenerator**

* Generates the partition in BigQuery using the query:
> select * except <partition column> ....

* Schedules this partition for extraction with the `ExtractionService`.


**BigQueryTableFilterer** 

* Creates the GBQ tables using the filtered query.
* Has a `DeleteTableAction` so they are deleted after.


**Composite Copier Factory** (CT)

* Creates the `BigQueryCopierFactory` instance.
* Adds it to a `CompositeCopier` object.
* Later in `PartitionedTableReplication` or `UnpartitionedTableReplication`, copy will be called on the `CompositeCopier` - which will call copy on the `BQCopier`.


**BigQueryCopierFactory**

* Creates the `BigQueryCopier`.


**BigQueryCopier**

* Runs extract on `ExtractionService`.
* Delegates to the Circus Train copier, e.g. `S3MapReduce`, which performs the copy using the location of the data in GCS. 


**ExtractionService**

* Runs the actions on the `PostExtractionActions` inside the `ExtractionContainers`.
* Calls the `DataExtractor` class which gets the data from the temp tables.
* Also has a cleanup method.


**DataExtractor** 

* Extracts the data from the temporary `BigQueryTable` and puts it into GCS.


**ExtractionContainer** 

* Takes a `PostExtractionAction` to run after the extraction of data.
* In the `BigQueryTableFilterer` it passes in an action to delete the table.
* `PartitionedTableService` calls the method that does this.
* The `ExtractionContainer` object is passed to `ExtractionService` via the `register(_)` method.


The copying of the data is then carried out by Circus Train copiers.

# Contact

## Mailing List
If you would like to ask any questions about or discuss Circus Train or Circus Train BigQuery please join our mailing list at 

  [https://groups.google.com/forum/#!forum/circus-train-user](https://groups.google.com/forum/#!forum/circus-train-user)
  
# Credits
The Circus Train BigQuery logo is licensed under the [Creative Commons Attribution-Share Alike 4.0](https://creativecommons.org/licenses/by-sa/4.0/deed.en) International license. It includes an adaption of the [Google BigQuery logo](https://commons.wikimedia.org/wiki/File:Google-BigQuery-Logo.svg) that is similarly licensed under the [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/deed.en) International license. The Circus Train logo uses the [Ewert font](http://www.1001fonts.com/ewert-font.html) by [Johan Kallas](http://www.1001fonts.com/users/kallasjohan/) under the [SIL Open Font License (OFL)](http://scripts.sil.org/cms/scripts/page.php?site_id=nrsi&id=OFL). 

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2016-2020 Expedia, Inc.
