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

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkMergeOnReadTable;
import org.apache.hudi.table.action.cluster.strategy.PartitionAwareClusteringPlanStrategy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.config.HoodieClusteringConfig.CLUSTERING_SORT_COLUMNS_PROPERTY;

/**
 * Clustering Strategy based on following.
 * 1) Creates clustering groups based on max size allowed per group.
 * 2) Excludes files that are greater than 'small.file.limit' from clustering plan.
 */
public class SparkSizeBasedClusteringPlanStrategy<T extends HoodieRecordPayload<T>>
    extends PartitionAwareClusteringPlanStrategy<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {
  private static final Logger LOG = LogManager.getLogger(SparkSizeBasedClusteringPlanStrategy.class);

  public SparkSizeBasedClusteringPlanStrategy(HoodieSparkCopyOnWriteTable<T> table,
                                              HoodieSparkEngineContext engineContext,
                                              HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  public SparkSizeBasedClusteringPlanStrategy(HoodieSparkMergeOnReadTable<T> table,
                                              HoodieSparkEngineContext engineContext,
                                              HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected Stream<HoodieClusteringGroup> buildClusteringGroupsForPartition(String partitionPath, List<FileSlice> fileSlices) {
    List<Pair<List<FileSlice>, Integer>> fileSliceGroups = new ArrayList<>();
    List<FileSlice> currentGroup = new ArrayList<>();
    long totalSizeSoFar = 0;
    for (FileSlice currentSlice : fileSlices) {
      // assume each filegroup size is ~= parquet.max.file.size
      totalSizeSoFar += currentSlice.getBaseFile().isPresent() ? currentSlice.getBaseFile().get().getFileSize() : getWriteConfig().getParquetMaxFileSize();
      // check if max size is reached and create new group, if needed.
      if (totalSizeSoFar >= getWriteConfig().getClusteringMaxBytesInGroup() && !currentGroup.isEmpty()) {
        int numOutputGroups = getNumberOfOutputFileGroups(totalSizeSoFar, getWriteConfig().getClusteringTargetFileMaxBytes());
        LOG.info("Adding one clustering group " + totalSizeSoFar + " max bytes: "
            + getWriteConfig().getClusteringMaxBytesInGroup() + " num input slices: " + currentGroup.size() + " output groups: " + numOutputGroups);
        fileSliceGroups.add(Pair.of(currentGroup, numOutputGroups));
        currentGroup = new ArrayList<>();
        totalSizeSoFar = 0;
      }
      currentGroup.add(currentSlice);
    }
    if (!currentGroup.isEmpty()) {
      int numOutputGroups = getNumberOfOutputFileGroups(totalSizeSoFar, getWriteConfig().getClusteringTargetFileMaxBytes());
      LOG.info("Adding final clustering group " + totalSizeSoFar + " max bytes: "
          + getWriteConfig().getClusteringMaxBytesInGroup() + " num input slices: " + currentGroup.size() + " output groups: " + numOutputGroups);
      fileSliceGroups.add(Pair.of(currentGroup, numOutputGroups));
    }

    return fileSliceGroups.stream().map(fileSliceGroup -> HoodieClusteringGroup.newBuilder()
        .setSlices(getFileSliceInfo(fileSliceGroup.getLeft()))
        .setNumOutputFileGroups(fileSliceGroup.getRight())
        .setMetrics(buildMetrics(fileSliceGroup.getLeft()))
        .build());
  }

  @Override
  protected Map<String, String> getStrategyParams() {
    Map<String, String> params = new HashMap<>();
    if (!StringUtils.isNullOrEmpty(getWriteConfig().getClusteringSortColumns())) {
      params.put(CLUSTERING_SORT_COLUMNS_PROPERTY.key(), getWriteConfig().getClusteringSortColumns());
    }
    return params;
  }

  @Override
  protected List<String> filterPartitionPaths(List<String> partitionPaths) {
    return partitionPaths;
  }

  @Override
  protected Stream<FileSlice> getFileSlicesEligibleForClustering(final String partition) {
    return super.getFileSlicesEligibleForClustering(partition)
        // Only files that have basefile size smaller than small file size are eligible.
        .filter(slice -> slice.getBaseFile().map(HoodieBaseFile::getFileSize).orElse(0L) < getWriteConfig().getClusteringSmallFileLimit());
  }

  private int getNumberOfOutputFileGroups(long groupSize, long targetFileSize) {
    return (int) Math.ceil(groupSize / (double) targetFileSize);
  }
}
