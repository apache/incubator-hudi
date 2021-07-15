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

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieClusteringConfig.CLUSTERING_STRATEGY_PARAM_PREFIX;

/**
 * Clustering Strategy to filter just specified partitions from [begin, end]. Note both begin and end are inclusive.
 */
public class SparkSelectedPartitionsClusteringPlanStrategy<T extends HoodieRecordPayload<T>>
    extends SparkSizeBasedClusteringPlanStrategy<T> {
  private static final Logger LOG = LogManager.getLogger(SparkSelectedPartitionsClusteringPlanStrategy.class);

  public static final String CONF_BEGIN_PARTITION = CLUSTERING_STRATEGY_PARAM_PREFIX + "cluster.begin.partition";
  public static final String CONF_END_PARTITION = CLUSTERING_STRATEGY_PARAM_PREFIX + "cluster.end.partition";
  
  public SparkSelectedPartitionsClusteringPlanStrategy(HoodieSparkCopyOnWriteTable<T> table,
                                                       HoodieSparkEngineContext engineContext,
                                                       HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  public SparkSelectedPartitionsClusteringPlanStrategy(HoodieSparkMergeOnReadTable<T> table,
                                                       HoodieSparkEngineContext engineContext,
                                                       HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected List<String> filterPartitionPaths(List<String> partitionPaths) {
    String beginPartition = getWriteConfig().getProps().getProperty(CONF_BEGIN_PARTITION);
    String endPartition = getWriteConfig().getProps().getProperty(CONF_END_PARTITION);
    List<String> filteredPartitions = partitionPaths.stream()
        .filter(path -> path.compareTo(beginPartition) >= 0 && path.compareTo(endPartition) <= 0)
        .collect(Collectors.toList());
    LOG.info("Filtered to the following partitions: " + filteredPartitions);
    return filteredPartitions;
  }
}
