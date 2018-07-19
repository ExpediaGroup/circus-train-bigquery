# TBD
### Changed
* Jobs no longer fail when parts of the best effort temporary data cleanup fail.
* Support sharded exports to enable replications of larger tables See [#10](https://github.com/HotelsDotCom/circus-train-bigquery/issues/10).
### Fixed
* Set Hive replica table metadata to ignore header in replicated CSV files. See [#12](https://github.com/HotelsDotCom/circus-train-bigquery/issues/12)

# 2.0.0 - 2018-07-13
### Changed
* Updated project to be compatible with Circus Train 12.0.0 and up
* Improved error logging when extraction job fails or is no longer present.
* Extract table during replication rather than in listener to prevent silent failure. See [#5](https://github.com/HotelsDotCom/circus-train-bigquery/issues/5).

# 1.0.1 - 2018-02-28
### Changed
* Project moved into Github.
