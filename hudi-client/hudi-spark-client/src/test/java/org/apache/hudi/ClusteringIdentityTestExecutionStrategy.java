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

package org.apache.hudi;

import org.apache.avro.Schema;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.execution.SparkLazyInsertIterable;
import org.apache.hudi.io.CreateFixedFileHandleFactory;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkMergeOnReadTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Sample clustering strategy for testing. This actually doesnt transform data, but simply rewrites the same data 
 * in a new file.
 */
public class ClusteringIdentityTestExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends ClusteringExecutionStrategy<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LogManager.getLogger(ClusteringIdentityTestExecutionStrategy.class);

  public ClusteringIdentityTestExecutionStrategy(HoodieSparkCopyOnWriteTable<T> table,
                                                 HoodieSparkEngineContext engineContext,
                                                 HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  public ClusteringIdentityTestExecutionStrategy(HoodieSparkMergeOnReadTable<T> table,
                                                 HoodieSparkEngineContext engineContext,
                                                 HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public JavaRDD<WriteStatus> performClustering(
      final JavaRDD<HoodieRecord<T>> inputRecords,
      final int numOutputGroups,
      final String instantTime,
      final Map<String, String> strategyParams,
      final Schema schema,
      final List<HoodieFileGroupId> inputFileIds) {
    if (inputRecords.getNumPartitions() != 1 || inputFileIds.size() != 1) {
      throw new HoodieClusteringException("Expect only one partition for test strategy: " + getClass().getName());
    }

    Properties props = getWriteConfig().getProps();
    HoodieWriteConfig newConfig =  HoodieWriteConfig.newBuilder().withProps(props).build();
    TaskContextSupplier taskContextSupplier = getEngineContext().getTaskContextSupplier();
    final HoodieTable hoodieTable = getHoodieTable();
    final String schemaString = schema.toString();

    JavaRDD<WriteStatus> writeStatus = inputRecords.mapPartitions(recordItr -> 
          insertRecords(recordItr, newConfig, instantTime, hoodieTable, schemaString, inputFileIds.get(0).getFileId(), taskContextSupplier))
        .flatMap(List::iterator);
    return writeStatus;
  }

  private static <T extends HoodieRecordPayload<T>> Iterator<List<WriteStatus>> insertRecords(final Iterator<HoodieRecord<T>> recordItr,
                                                    final HoodieWriteConfig newConfig,
                                                    final String instantTime,
                                                    final HoodieTable hoodieTable,
                                                    final String schema,
                                                    final String fileId,
                                                    final TaskContextSupplier taskContextSupplier) {
   
    return new SparkLazyInsertIterable(recordItr, true, newConfig, instantTime, hoodieTable,
        fileId, taskContextSupplier, new CreateFixedFileHandleFactory(fileId), true);
  }
}
