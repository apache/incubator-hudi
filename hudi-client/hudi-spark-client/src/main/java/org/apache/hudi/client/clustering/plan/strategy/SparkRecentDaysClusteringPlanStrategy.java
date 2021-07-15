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

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkMergeOnReadTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Clustering Strategy that only looks at latest 'daybased.lookback.partitions' partitions.
 */
public class SparkRecentDaysClusteringPlanStrategy<T extends HoodieRecordPayload<T>>
    extends SparkSizeBasedClusteringPlanStrategy<T> {
  private static final Logger LOG = LogManager.getLogger(SparkRecentDaysClusteringPlanStrategy.class);

  public SparkRecentDaysClusteringPlanStrategy(HoodieSparkCopyOnWriteTable<T> table,
                                               HoodieSparkEngineContext engineContext,
                                               HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  public SparkRecentDaysClusteringPlanStrategy(HoodieSparkMergeOnReadTable<T> table,
                                               HoodieSparkEngineContext engineContext,
                                               HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  protected List<String> filterPartitionPaths(List<String> partitionPaths) {
    int targetPartitionsForClustering = getWriteConfig().getTargetPartitionsForClustering();
    return partitionPaths.stream()
        .sorted(Comparator.reverseOrder())
        .limit(targetPartitionsForClustering > 0 ? targetPartitionsForClustering : partitionPaths.size())
        .collect(Collectors.toList());
  }
}
