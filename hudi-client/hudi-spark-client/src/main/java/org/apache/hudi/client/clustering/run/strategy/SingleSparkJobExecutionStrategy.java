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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.ConcatenatingIterator;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Clustering strategy to submit single spark jobs.
 * MultipleSparkJobExecution strategy is not ideal for use cases that require large number of clustering groups
 */
public abstract class SingleSparkJobExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends ClusteringExecutionStrategy<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {
  private static final Logger LOG = LogManager.getLogger(SingleSparkJobExecutionStrategy.class);

  public SingleSparkJobExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }
  
  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> performClustering(final HoodieClusteringPlan clusteringPlan, final Schema schema, final String instantTime) {
    JavaSparkContext engineContext = HoodieSparkEngineContext.getSparkContext(getEngineContext());
    final TaskContextSupplier taskContextSupplier = getEngineContext().getTaskContextSupplier();
    final SerializableSchema serializableSchema = new SerializableSchema(schema);
    final List<ClusteringGroupInfo> clusteringGroupInfos = clusteringPlan.getInputGroups().stream().map(clusteringGroup ->
        ClusteringGroupInfo.create(clusteringGroup)).collect(Collectors.toList());

    String umask = engineContext.hadoopConfiguration().get("fs.permissions.umask-mode");
    Broadcast<String> umaskBroadcastValue = engineContext.broadcast(umask);

    JavaRDD<ClusteringGroupInfo> groupInfoJavaRDD = engineContext.parallelize(clusteringGroupInfos, clusteringGroupInfos.size());
    LOG.info("number of partitions for clustering " + groupInfoJavaRDD.getNumPartitions());
    JavaRDD<WriteStatus> writeStatusRDD = groupInfoJavaRDD
        .mapPartitions(clusteringOps -> {
          Configuration configuration = new Configuration();
          configuration.set("fs.permissions.umask-mode", umaskBroadcastValue.getValue());
          Iterable<ClusteringGroupInfo> clusteringOpsIterable = () -> clusteringOps;
          List<ClusteringGroupInfo> groupsInPartition = StreamSupport.stream(clusteringOpsIterable.spliterator(), false).collect(Collectors.toList());
          return groupsInPartition.stream().flatMap(clusteringOp -> 
              runClusteringForGroup(clusteringOp, clusteringPlan.getStrategy().getStrategyParams(),
                  Option.ofNullable(clusteringPlan.getPreserveHoodieMetadata()).orElse(false),
                  serializableSchema, taskContextSupplier, instantTime)
          ).iterator();
        });
    
    HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata = new HoodieWriteMetadata<>();
    writeMetadata.setWriteStatuses(writeStatusRDD);
    return writeMetadata;
  }


  /**
   * Submit job to execute clustering for the group.
   */
  private Stream<WriteStatus> runClusteringForGroup(ClusteringGroupInfo clusteringOps, Map<String, String> strategyParams,
                                                    boolean preserveHoodieMetadata, SerializableSchema schema,
                                                    TaskContextSupplier taskContextSupplier, String instantTime) {

    List<HoodieFileGroupId> inputFileIds = clusteringOps.getOperations().stream()
        .map(op -> new HoodieFileGroupId(op.getPartitionPath(), op.getFileId()))
        .collect(Collectors.toList());

    Iterator<HoodieRecord<T>> inputRecords = readRecordsForGroupBaseFiles(clusteringOps.getOperations());
    Iterator<List<WriteStatus>> writeStatuses = performClusteringWithRecordsIterator(inputRecords, clusteringOps.getNumOutputGroups(), instantTime,
            strategyParams, schema.get(), inputFileIds, preserveHoodieMetadata, taskContextSupplier);

    Iterable<List<WriteStatus>> writestatusIterable = () -> writeStatuses;
    return StreamSupport.stream(writestatusIterable.spliterator(), false)
        .flatMap(writeStatusList -> writeStatusList.stream());
  }


  /**
   * Execute clustering to write inputRecords into new files as defined by rules in strategy parameters. 
   * The number of new file groups created is bounded by numOutputGroups.
   * Note that commit is not done as part of strategy. commit is callers responsibility.
   */
  public abstract Iterator<List<WriteStatus>> performClusteringWithRecordsIterator(final Iterator<HoodieRecord<T>> records, final int numOutputGroups,
                                                                                   final String instantTime, 
                                                                                   final Map<String, String> strategyParams, final Schema schema, 
                                                                                   final List<HoodieFileGroupId> fileGroupIdList, final boolean preserveHoodieMetadata,
                                                                                   final TaskContextSupplier taskContextSupplier);

  /**
   * Read records from baseFiles and get iterator.
   */
  private Iterator<HoodieRecord<T>> readRecordsForGroupBaseFiles(List<ClusteringOperation> clusteringOps) {
    List<Iterator<HoodieRecord<T>>> iteratorsForPartition = clusteringOps.stream().map(clusteringOp -> {

      Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(getWriteConfig().getSchema()));
      Iterable<IndexedRecord> indexedRecords = () -> {
        try {
          return HoodieFileReaderFactory.getFileReader(getHoodieTable().getHadoopConf(), new Path(clusteringOp.getDataFilePath())).getRecordIterator(readerSchema);
        } catch (IOException e) {
          throw new HoodieClusteringException("Error reading input data for " + clusteringOp.getDataFilePath()
              + " and " + clusteringOp.getDeltaFilePaths(), e);
        }
      };

      return StreamSupport.stream(indexedRecords.spliterator(), false).map(record -> transform(record)).iterator();
    }).collect(Collectors.toList());

    return new ConcatenatingIterator<>(iteratorsForPartition);
  }
}
