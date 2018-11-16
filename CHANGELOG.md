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
