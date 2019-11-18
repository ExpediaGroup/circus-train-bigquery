# [5.1.2] - TBD
### Fixed
* Removed duplicated partition column from the schema. See [#37](https://github.com/HotelsDotCom/circus-train-bigquery/issues/37).

# [5.1.1] - 2019-09-23
### Changed
* Updated `hotels-oss-parent` to 4.2.0 (was 4.0.1).
* Excluded `org.pentaho.pentaho-aggdesigner-algorithm` dependency as it's not available in Maven Central.

# [5.1.0] - 2019-05-09
### Changed
* Refactored project to remove checkstyle and findbugs warnings, which does not impact functionality.
* Upgraded `circus-train-version` to 14.0.1 (was 13.0.0).
* Upgraded `hotels-oss-parent` to 4.0.1 (was 2.3.3).
* Upgraded `google-cloud-bom` to 0.73.0-alpha (was 0.34.0-alpha).
* Shade and relocate all `com.google` packages (to fix Guava version issue).

# [5.0.4] - 2018-12-17
### Changed
* Upgraded `hotels-oss-parent` pom to 2.3.3 (was 2.0.6). See [#23](https://github.com/HotelsDotCom/circus-train-bigquery/issues/23).

# [5.0.3] - 2018-12-05
### Fixed
* Add Spring annotations to all the components that do not need to load during the housekeeping module. See [#28](https://github.com/HotelsDotCom/circus-train-bigquery/issues/28).

# [5.0.2] - 2018-11-22
### Fixed
* Read schema from Avro file using a stream instead of entire file to avoid OOM. See [#26](https://github.com/HotelsDotCom/circus-train-bigquery/issues/26).

# [5.0.1] - 2018-11-19
### Changed
* Partition columns are set to type `string`, regardless of the type of the column that is used to partition the table.
* If an error occurs while deleting an intermediate table during cleanup, this will no longer fail the replication. Instead, it will log the error and continue.

# [5.0.0] - 2018-11-16
### Changed
* Circus Train version upgraded to 13.0.0 (was 12.0.0). Note that this change is _not_ backwards compatible as this BigQuery extension now needs to be explicitly added to the Circus Train classpath using Circus Train's [standard extension loading mechanism](https://github.com/HotelsDotCom/circus-train#loading-extensions). See [#20](https://github.com/HotelsDotCom/circus-train-bigquery/issues/20).

# [4.0.0] - 2018-11-09
### Changed
* Replicated tables are now exported as AVRO files instead of CSV files. This allows BigQuery tables to be replicated without any schema or data change. See [#18](https://github.com/HotelsDotCom/circus-train-bigquery/issues/17). 
* Please note that this version is _not_ backwards compatible for partitioned tables that have been replicated using an earlier version - in this case all the previously replicated partitions will need to be replicated again from scratch.

# [3.0.0] - 2018-09-07
### Added
* Support for partition generation upon replication.
* Support for partition filters.
* Support for filters in Standard SQL.

### Fixed
* Integers from BigQuery are treated as 64 bit (BigInt Hive type) values rather than 32 bit (Int Hive type) values.

# [2.0.2] - 2018-07-19
### Fixed
* Jobs no longer fail when parts of the best effort temporary data cleanup fail.

# [2.0.1] - 2018-07-18
### Changed
* Support sharded exports to enable replications of larger tables See [#10](https://github.com/HotelsDotCom/circus-train-bigquery/issues/10).

### Fixed
* Set Hive replica table metadata to ignore header in replicated CSV files. See [#12](https://github.com/HotelsDotCom/circus-train-bigquery/issues/12).

# [2.0.0] - 2018-07-13
### Changed
* Updated project to be compatible with Circus Train 12.0.0 and up.
* Improved error logging when extraction job fails or is no longer present.
* Extract table during replication rather than in listener to prevent silent failure. See [#5](https://github.com/HotelsDotCom/circus-train-bigquery/issues/5).

# [1.0.1] - 2018-02-28
### Changed
* Project moved into Github.
