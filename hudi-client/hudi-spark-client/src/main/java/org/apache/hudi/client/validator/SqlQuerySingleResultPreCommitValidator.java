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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Validator to run sql queries on new table state and expects a single result. If the result doesnt match expected result,
 * throw validation error. 
 * 
 * Example configuration: "query1#expectedResult1;query2#expectedResult2;"
 */
public class SqlQuerySingleResultPreCommitValidator<T extends HoodieRecordPayload, I, K, O extends JavaRDD<WriteStatus>> extends SqlQueryPreCommitValidator<T, I, K, O> {
  private static final Logger LOG = LogManager.getLogger(SqlQueryInequalityPreCommitValidator.class);

  public SqlQuerySingleResultPreCommitValidator(HoodieSparkTable<T> table, HoodieEngineContext engineContext, HoodieWriteConfig config) {
    super(table, engineContext, config);
  }

  @Override
  protected String getQueryConfigName() {
    return HoodiePreCommitValidatorConfig.PRE_COMMIT_VALIDATORS_SINGLE_VALUE_SQL_QUERIES.key();
  }

  @Override
  protected void validateUsingQuery(String query, String prevTableSnapshot, String newTableSnapshot, SQLContext sqlContext) {
    String[] queryWithExpectedResult = query.split("#");
    if (queryWithExpectedResult.length != 2) {
      throw new HoodieValidationException("Invalid query format " + query);
    }
    
    String queryToRun = queryWithExpectedResult[0];
    String expectedResult = queryWithExpectedResult[1];
    LOG.info("Running query on new state: " + queryToRun);
    String queryWithNewSnapshot = queryToRun.replaceAll(HoodiePreCommitValidatorConfig.VALIDATOR_TABLE_VARIABLE, newTableSnapshot);
    List<Row> newRows  = sqlContext.sql(queryWithNewSnapshot).collectAsList();
    if (newRows.size() != 1 && newRows.get(0).size() != 1) {
      throw new HoodieValidationException("Invalid query result. expect single value for '" + query + "'");
    }
    Object result = newRows.get(0).apply(0);
    if (result == null || !expectedResult.equals(result.toString())) {
      LOG.error("Mismatch query result. Expected: " + expectedResult + " got " + result + "Query: " + query);
      throw new HoodieValidationException("Query validation failed for '" + query
          + "'. Expected " + expectedResult + " rows, Found " + result);
    } else {
      LOG.info("Query validation successful. Expected: " + expectedResult + " got " + result);
    }
  }
}
