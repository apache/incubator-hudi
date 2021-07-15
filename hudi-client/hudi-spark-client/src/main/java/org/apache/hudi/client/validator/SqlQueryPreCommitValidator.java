/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client.validator;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Validator framework to run sql queries and compare table state at different locations.
 */
public abstract class SqlQueryPreCommitValidator<T extends HoodieRecordPayload, I, K, O extends JavaRDD<WriteStatus>> extends SparkPreCommitValidator<T, I, K, O> {
  private static final Logger LOG = LogManager.getLogger(SqlQueryPreCommitValidator.class);
  private static final AtomicInteger TABLE_COUNTER = new AtomicInteger(0);

  public SqlQueryPreCommitValidator(HoodieSparkTable<T> table, HoodieEngineContext engineContext, HoodieWriteConfig config) {
    super(table, engineContext, config);
  }

  /**
   * Takes input datasets 1) before commit started and 2) with inflight commit. Perform required validation 
   * and throw error if validation fails
   */
  @Override
  public void validateRecordsBeforeAndAfter(Dataset<Row> before, Dataset<Row> after, final Set<String> partitionsAffected) {
    String hoodieTableName = "staged_table_" + TABLE_COUNTER.incrementAndGet();
    String hoodieTableBeforeCurrentCommit = hoodieTableName + "_before";
    String hoodieTableWithInflightCommit = hoodieTableName + "_after";
    before.registerTempTable(hoodieTableBeforeCurrentCommit);
    after.registerTempTable(hoodieTableWithInflightCommit);
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(getEngineContext());
    SQLContext sqlContext = new SQLContext(jsc);

    String[] queries = getQueriesToRun();
    //TODO run this in a thread pool to improve parallelism
    Arrays.stream(queries).forEach(query -> 
        validateUsingQuery(query, hoodieTableBeforeCurrentCommit, hoodieTableWithInflightCommit, sqlContext));
  }

  protected String[] getQueriesToRun() {
    String sqlQueriesConfigured = getWriteConfig().getProps().getProperty(getQueryConfigName());
    if (StringUtils.isNullOrEmpty(sqlQueriesConfigured)) {
      throw new HoodieValidationException("Sql validator configured incorrectly. expecting at least one query. Found 0 queries in "
          + sqlQueriesConfigured);
    }
    return sqlQueriesConfigured.trim().split(";");
  }
  
  protected void printAllRowsIfDebugEnabled(Dataset<Row> dataset) {
    if (LOG.isDebugEnabled()) {
      dataset = dataset.cache();
      LOG.debug("Printing all rows from query validation:");
      dataset.show(Integer.MAX_VALUE,false);
    }
  }
  
  protected abstract String getQueryConfigName();
  
  protected abstract void validateUsingQuery(String query, String prevTableSnapshot, String newTableSnapshot, SQLContext sqlContext);
}
