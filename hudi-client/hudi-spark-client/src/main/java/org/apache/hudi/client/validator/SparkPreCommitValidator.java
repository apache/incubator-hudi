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
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validator can be configured pre-commit. 
 */
public abstract class SparkPreCommitValidator<T extends HoodieRecordPayload, I, K, O extends JavaRDD<WriteStatus>> {
  private static final Logger LOG = LogManager.getLogger(SparkPreCommitValidator.class);

  private HoodieSparkTable<T> table;
  private HoodieEngineContext engineContext;
  private HoodieWriteConfig writeConfig;

  protected SparkPreCommitValidator(HoodieSparkTable<T> table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    this.table = table;
    this.engineContext = engineContext;
    this.writeConfig = writeConfig;
  }
  
  protected Set<String> getPartitionsModified(HoodieWriteMetadata<O> writeResult) {
    Set<String> partitionsModified;
    if (writeResult.getWriteStats().isPresent()) {
      partitionsModified = writeResult.getWriteStats().get().stream().map(HoodieWriteStat::getPartitionPath).collect(Collectors.toSet());
    } else {
      partitionsModified = new HashSet<>(writeResult.getWriteStatuses().map(WriteStatus::getPartitionPath).collect());
    }
    return partitionsModified;
  }

  /**
   * Verify the data written as part of specified instant. 
   * Throw HoodieValidationException if any unexpected data is written (Example: data files are not readable for some reason).
   */
  public void validate(String instantTime, HoodieWriteMetadata<O> writeResult, Dataset<Row> before, Dataset<Row> after) throws HoodieValidationException {
    HoodieTimer timer = new HoodieTimer().startTimer();
    try {
      validateRecordsBeforeAndAfter(before, after, getPartitionsModified(writeResult));
    } finally {
      LOG.info(getClass() + " validator took " + timer.endTimer() + " ms");
    }
  }

  /**
   * Takes input of RDD 1) before clustering and 2) after clustering. Perform required validation 
   * and throw error if validation fails
   */
  protected abstract void validateRecordsBeforeAndAfter(Dataset<Row> before,
                                                        Dataset<Row> after,
                                                        Set<String> partitionsAffected);

  public HoodieTable getHoodieTable() {
    return this.table;
  }

  public HoodieEngineContext getEngineContext() {
    return this.engineContext;
  }

  public HoodieWriteConfig getWriteConfig() {
    return this.writeConfig;
  }
}
