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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Clustering specific configs.
 */
public class HoodieClusteringConfig extends HoodieConfig {

  // Any strategy specific params can be saved with this prefix
  public static final String CLUSTERING_STRATEGY_PARAM_PREFIX = "hoodie.clustering.plan.strategy.";

  public static final ConfigProperty<String> CLUSTERING_TARGET_PARTITIONS = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "daybased.lookback.partitions")
      .defaultValue("2")
      .sinceVersion("0.7.0")
      .withDocumentation("Number of partitions to list to create ClusteringPlan");

  public static final ConfigProperty<String> CLUSTERING_PLAN_STRATEGY_CLASS = ConfigProperty
      .key("hoodie.clustering.plan.strategy.class")
      .defaultValue("org.apache.hudi.client.clustering.plan.strategy.SparkRecentDaysClusteringPlanStrategy")
      .sinceVersion("0.7.0")
      .withDocumentation("Config to provide a strategy class (subclass of ClusteringPlanStrategy) to create clustering plan "
          + "i.e select what file groups are being clustered. Default strategy, looks at the last N (determined by "
          + CLUSTERING_TARGET_PARTITIONS.key() + ") day based partitions picks the small file slices within those partitions.");

  public static final ConfigProperty<String> CLUSTERING_EXECUTION_STRATEGY_CLASS = ConfigProperty
      .key("hoodie.clustering.execution.strategy.class")
      .defaultValue("org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy")
      .sinceVersion("0.7.0")
      .withDocumentation("Config to provide a strategy class (subclass of RunClusteringStrategy) to define how the "
          + " clustering plan is executed. By default, we sort the file groups in th plan by the specified columns, while "
          + " meeting the configured target file sizes.");

  public static final ConfigProperty<String> INLINE_CLUSTERING_PROP = ConfigProperty
      .key("hoodie.clustering.inline")
      .defaultValue("false")
      .sinceVersion("0.7.0")
      .withDocumentation("Turn on inline clustering - clustering will be run after each write operation is complete");

  public static final ConfigProperty<String> INLINE_CLUSTERING_MAX_COMMIT_PROP = ConfigProperty
      .key("hoodie.clustering.inline.max.commits")
      .defaultValue("4")
      .sinceVersion("0.7.0")
      .withDocumentation("Config to control frequency of clustering planning");

  public static final ConfigProperty<String> ASYNC_CLUSTERING_MAX_COMMIT_PROP = ConfigProperty
      .key("hoodie.clustering.async.max.commits")
      .defaultValue("4")
      .sinceVersion("0.9.0")
      .withDocumentation("Config to control frequency of async clustering");

  public static final ConfigProperty<String> CLUSTERING_PLAN_SMALL_FILE_LIMIT = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "small.file.limit")
      .defaultValue(String.valueOf(600 * 1024 * 1024L))
      .sinceVersion("0.7.0")
      .withDocumentation("Files smaller than the size specified here are candidates for clustering");

  public static final ConfigProperty<String> CLUSTERING_MAX_BYTES_PER_GROUP = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "max.bytes.per.group")
      .defaultValue(String.valueOf(2 * 1024 * 1024 * 1024L))
      .sinceVersion("0.7.0")
      .withDocumentation("Each clustering operation can create multiple output file groups. Total amount of data processed by clustering operation"
          + " is defined by below two properties (CLUSTERING_MAX_BYTES_PER_GROUP * CLUSTERING_MAX_NUM_GROUPS)."
          + " Max amount of data to be included in one group");

  public static final ConfigProperty<String> CLUSTERING_MAX_NUM_GROUPS = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "max.num.groups")
      .defaultValue("30")
      .sinceVersion("0.7.0")
      .withDocumentation("Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism");

  public static final ConfigProperty<String> CLUSTERING_TARGET_FILE_MAX_BYTES = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "target.file.max.bytes")
      .defaultValue(String.valueOf(1024 * 1024 * 1024L))
      .sinceVersion("0.7.0")
      .withDocumentation("Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups");

  public static final ConfigProperty<String> CLUSTERING_SORT_COLUMNS_PROPERTY = ConfigProperty
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "sort.columns")
      .noDefaultValue()
      .sinceVersion("0.7.0")
      .withDocumentation("Columns to sort the data by when clustering");

  public static final ConfigProperty<String> CLUSTERING_UPDATES_STRATEGY_PROP = ConfigProperty
      .key("hoodie.clustering.updates.strategy")
      .defaultValue("org.apache.hudi.client.clustering.update.strategy.SparkRejectUpdateStrategy")
      .sinceVersion("0.7.0")
      .withDocumentation("Determines how to handle updates, deletes to file groups that are under clustering."
          + " Default strategy just rejects the update");

  public static final ConfigProperty<String> ASYNC_CLUSTERING_ENABLE_OPT_KEY = ConfigProperty
      .key("hoodie.clustering.async.enabled")
      .defaultValue("false")
      .sinceVersion("0.7.0")
      .withDocumentation("Enable running of clustering service, asynchronously as inserts happen on the table.");

  public static final ConfigProperty<Boolean> CLUSTERING_PRESERVE_HOODIE_COMMIT_METADATA = ConfigProperty
      .key("hoodie.clustering.preserve.commit.metadata")
      .defaultValue(false)
      .sinceVersion("0.9.0")
      .withDocumentation("When rewriting data, preserves existing hoodie_commit_time");
  
  public HoodieClusteringConfig() {
    super();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final HoodieClusteringConfig clusteringConfig = new HoodieClusteringConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.clusteringConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder withClusteringPlanStrategyClass(String clusteringStrategyClass) {
      clusteringConfig.setValue(CLUSTERING_PLAN_STRATEGY_CLASS, clusteringStrategyClass);
      return this;
    }

    public Builder withClusteringExecutionStrategyClass(String runClusteringStrategyClass) {
      clusteringConfig.setValue(CLUSTERING_EXECUTION_STRATEGY_CLASS, runClusteringStrategyClass);
      return this;
    }

    public Builder withClusteringTargetPartitions(int clusteringTargetPartitions) {
      clusteringConfig.setValue(CLUSTERING_TARGET_PARTITIONS, String.valueOf(clusteringTargetPartitions));
      return this;
    }

    public Builder withClusteringPlanSmallFileLimit(long clusteringSmallFileLimit) {
      clusteringConfig.setValue(CLUSTERING_PLAN_SMALL_FILE_LIMIT, String.valueOf(clusteringSmallFileLimit));
      return this;
    }
    
    public Builder withClusteringSortColumns(String sortColumns) {
      clusteringConfig.setValue(CLUSTERING_SORT_COLUMNS_PROPERTY, sortColumns);
      return this;
    }

    public Builder withClusteringMaxBytesInGroup(long clusteringMaxGroupSize) {
      clusteringConfig.setValue(CLUSTERING_MAX_BYTES_PER_GROUP, String.valueOf(clusteringMaxGroupSize));
      return this;
    }

    public Builder withClusteringMaxNumGroups(int maxNumGroups) {
      clusteringConfig.setValue(CLUSTERING_MAX_NUM_GROUPS, String.valueOf(maxNumGroups));
      return this;
    }

    public Builder withClusteringTargetFileMaxBytes(long targetFileSize) {
      clusteringConfig.setValue(CLUSTERING_TARGET_FILE_MAX_BYTES, String.valueOf(targetFileSize));
      return this;
    }

    public Builder withInlineClustering(Boolean inlineClustering) {
      clusteringConfig.setValue(INLINE_CLUSTERING_PROP, String.valueOf(inlineClustering));
      return this;
    }

    public Builder withInlineClusteringNumCommits(int numCommits) {
      clusteringConfig.setValue(INLINE_CLUSTERING_MAX_COMMIT_PROP, String.valueOf(numCommits));
      return this;
    }

    public Builder withAsyncClusteringMaxCommits(int numCommits) {
      clusteringConfig.setValue(ASYNC_CLUSTERING_MAX_COMMIT_PROP, String.valueOf(numCommits));
      return this;
    }

    public Builder fromProperties(Properties props) {
      this.clusteringConfig.getProps().putAll(props);
      return this;
    }

    public Builder withClusteringUpdatesStrategy(String updatesStrategyClass) {
      clusteringConfig.setValue(CLUSTERING_UPDATES_STRATEGY_PROP, updatesStrategyClass);
      return this;
    }

    public Builder withAsyncClustering(Boolean asyncClustering) {
      clusteringConfig.setValue(ASYNC_CLUSTERING_ENABLE_OPT_KEY, String.valueOf(asyncClustering));
      return this;
    }

    public Builder withPreserveHoodieCommitMetadata(Boolean preserveHoodieCommitMetadata) {
      clusteringConfig.setValue(CLUSTERING_PRESERVE_HOODIE_COMMIT_METADATA, String.valueOf(preserveHoodieCommitMetadata));
      return this;
    }

    public HoodieClusteringConfig build() {
      clusteringConfig.setDefaults(HoodieClusteringConfig.class.getName());
      return clusteringConfig;
    }
  }
}
