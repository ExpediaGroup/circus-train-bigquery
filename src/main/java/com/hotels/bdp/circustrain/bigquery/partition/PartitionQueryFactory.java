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
package com.hotels.bdp.circustrain.bigquery.partition;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PartitionQueryFactory {

  private static final Logger log = LoggerFactory.getLogger(PartitionQueryFactory.class);

  public String newInstance(Table hiveTable, String partitionBy, String partitionFilter) {
    if (isNotBlank(partitionBy) && isNotBlank(partitionFilter)) {
      String query = String
          .format("select distinct(%s) from %s.%s where %s group by %s order by %s", partitionBy, hiveTable.getDbName(),
              hiveTable.getTableName(), partitionFilter, partitionBy, partitionBy);
      log.debug("Query for partitioning and filtering table: {}", query);
      return query;
    } else if (isNotBlank(partitionBy) && isBlank(partitionFilter)) {
      String query = String
          .format("select distinct(%s) from %s.%s group by %s order by %s", partitionBy, hiveTable.getDbName(),
              hiveTable.getTableName(), partitionBy, partitionBy);
      log.debug("Query for partitioning table: {}", query);
      return query;
    } else {
      throw new IllegalStateException(
          "Cannot create a partition filter query if neither partitionBy nor partitionFilter are provided");
    }
  }
}
