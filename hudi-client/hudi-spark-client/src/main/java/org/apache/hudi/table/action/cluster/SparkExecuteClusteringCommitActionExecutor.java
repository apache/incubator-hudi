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

package org.apache.hudi.table.action.cluster;

import org.apache.avro.Schema;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;
import org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SparkExecuteClusteringCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseSparkCommitActionExecutor<T> {

  private static final Logger LOG = LogManager.getLogger(SparkExecuteClusteringCommitActionExecutor.class);
  private final HoodieClusteringPlan clusteringPlan;

  public SparkExecuteClusteringCommitActionExecutor(HoodieEngineContext context,
                                                    HoodieWriteConfig config, HoodieTable table,
                                                    String instantTime) {
    super(context, config, table, instantTime, WriteOperationType.CLUSTER);
    this.clusteringPlan = ClusteringUtils.getClusteringPlan(table.getMetaClient(), HoodieTimeline.getReplaceCommitRequestedInstant(instantTime))
      .map(Pair::getRight).orElseThrow(() -> new HoodieClusteringException("Unable to read clustering plan for instant: " + instantTime));
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> execute() {
    HoodieInstant instant = HoodieTimeline.getReplaceCommitRequestedInstant(instantTime);
    // Mark instant as clustering inflight
    table.getActiveTimeline().transitionReplaceRequestedToInflight(instant, Option.empty());
    table.getMetaClient().reloadActiveTimeline();

    final Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata = ((ClusteringExecutionStrategy<T, JavaRDD<HoodieRecord<? extends HoodieRecordPayload>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>>)
        ReflectionUtils.loadClass(config.getClusteringExecutionStrategyClass(),
            new Class<?>[] {HoodieTable.class, HoodieEngineContext.class, HoodieWriteConfig.class}, table, context, config))
        .performClustering(clusteringPlan, schema, instantTime);
    JavaRDD<WriteStatus> writeStatusRDD = writeMetadata.getWriteStatuses();
    JavaRDD<WriteStatus> statuses = updateIndex(writeStatusRDD, writeMetadata);
    writeMetadata.setWriteStats(statuses.map(WriteStatus::getStat).collect());
    writeMetadata.setPartitionToReplaceFileIds(getPartitionToReplacedFileIds(writeMetadata));
    validateWriteResult(writeMetadata);
    commitOnAutoCommit(writeMetadata);
    if (!writeMetadata.getCommitMetadata().isPresent()) {
      HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(writeMetadata.getWriteStats().get(), writeMetadata.getPartitionToReplaceFileIds(),
          extraMetadata, operationType, getSchemaToStoreInCommit(), getCommitActionType());
      writeMetadata.setCommitMetadata(Option.of(commitMetadata));
    }
    return writeMetadata;
  }

  /**
   * Validate actions taken by clustering. In the first implementation, we validate at least one new file is written.
   * But we can extend this to add more validation. E.g. number of records read = number of records written etc.
   * 
   * We can also make these validations in BaseCommitActionExecutor to reuse pre-commit hooks for multiple actions.
   */
  private void validateWriteResult(HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata) {
    if (writeMetadata.getWriteStatuses().isEmpty()) {
      throw new HoodieClusteringException("Clustering plan produced 0 WriteStatus for " + instantTime
          + " #groups: " + clusteringPlan.getInputGroups().size() + " expected at least "
          + clusteringPlan.getInputGroups().stream().mapToInt(HoodieClusteringGroup::getNumOutputFileGroups).sum()
          + " write statuses");
    }
  }

  @Override
  protected String getCommitActionType() {
    return HoodieTimeline.REPLACE_COMMIT_ACTION;
  }

  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata) {
    Set<HoodieFileGroupId> newFilesWritten = new HashSet(writeMetadata.getWriteStats().get().stream()
        .map(s -> new HoodieFileGroupId(s.getPartitionPath(),s.getFileId()))
        .collect(Collectors.toList()));
    return ClusteringUtils.getFileGroupsFromClusteringPlan(clusteringPlan)
        .filter(fg -> !newFilesWritten.contains(fg))
        .collect(Collectors.groupingBy(fg -> fg.getPartitionPath(), Collectors.mapping(fg -> fg.getFileId(), Collectors.toList())));
  }
}
