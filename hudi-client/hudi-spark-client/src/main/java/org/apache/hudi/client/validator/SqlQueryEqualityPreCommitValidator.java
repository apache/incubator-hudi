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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Validator to run sql query and compare table state 
 * 1) before new commit started.
 * 2) current inflight commit (if successful).
 * 
 * Expects both queries to return same result.
 */
public class SqlQueryEqualityPreCommitValidator<T extends HoodieRecordPayload, I, K, O extends JavaRDD<WriteStatus>> extends SqlQueryPreCommitValidator<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(SqlQueryEqualityPreCommitValidator.class);

  public SqlQueryEqualityPreCommitValidator(HoodieSparkTable<T> table, HoodieEngineContext engineContext, HoodieWriteConfig config) {
    super(table, engineContext, config);
  }

  @Override
  protected String getQueryConfigName() {
    return HoodiePreCommitValidatorConfig.PRE_COMMIT_VALIDATORS_EQUALITY_SQL_QUERIES.key();
  }

  @Override
  protected void validateUsingQuery(String query, String prevTableSnapshot, String newTableSnapshot, SQLContext sqlContext) {
    String queryWithPrevSnapshot = query.replaceAll(HoodiePreCommitValidatorConfig.VALIDATOR_TABLE_VARIABLE, prevTableSnapshot);
    String queryWithNewSnapshot = query.replaceAll(HoodiePreCommitValidatorConfig.VALIDATOR_TABLE_VARIABLE, newTableSnapshot);
    LOG.info("Running query on previous state: " + queryWithPrevSnapshot);
    Dataset<Row> prevRows = sqlContext.sql(queryWithPrevSnapshot);
    LOG.info("Running query on new state: " + queryWithNewSnapshot);
    Dataset<Row> newRows  = sqlContext.sql(queryWithNewSnapshot);
    printAllRowsIfDebugEnabled(prevRows);
    printAllRowsIfDebugEnabled(newRows);
    boolean areDatasetsEqual = prevRows.intersect(newRows).count() == prevRows.count();
    LOG.info("Completed Equality Validation, datasets equal? " + areDatasetsEqual);
    if (!areDatasetsEqual) {
      LOG.error("query validation failed. See stdout for sample query results. Query: " + query);
      System.out.println("Expected result (sample records only):");
      prevRows.show();
      System.out.println("Actual result (sample records only):");
      newRows.show();
      throw new HoodieValidationException("Query validation failed for '" + query + "'. See stdout for expected vs actual records");
    }
  }
}
