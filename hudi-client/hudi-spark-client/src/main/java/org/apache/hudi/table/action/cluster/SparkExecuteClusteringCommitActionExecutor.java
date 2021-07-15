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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.utils.ConcatenatingIterator;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.log.HoodieFileSliceReader;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;
import org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    JavaSparkContext engineContext = HoodieSparkEngineContext.getSparkContext(context);
    // execute clustering for each group async and collect WriteStatus
    Stream<JavaRDD<WriteStatus>> writeStatusRDDStream = clusteringPlan.getInputGroups().stream()
        .map(inputGroup -> runClusteringForGroupAsync(inputGroup, clusteringPlan.getStrategy().getStrategyParams()))
        .map(CompletableFuture::join);

    JavaRDD<WriteStatus>[] writeStatuses = convertStreamToArray(writeStatusRDDStream);
    JavaRDD<WriteStatus> writeStatusRDD = engineContext.union(writeStatuses);
    
    HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata = buildWriteMetadata(writeStatusRDD);
    JavaRDD<WriteStatus> statuses = updateIndex(writeStatusRDD, writeMetadata);
    writeMetadata.setWriteStats(statuses.map(WriteStatus::getStat).collect());
    // validate clustering action before committing result
    validateWriteResult(writeMetadata);
    commitOnAutoCommit(writeMetadata);
    if (!writeMetadata.getCommitMetadata().isPresent()) {
      HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(writeStatusRDD.map(WriteStatus::getStat).collect(), writeMetadata.getPartitionToReplaceFileIds(),
          extraMetadata, operationType, getSchemaToStoreInCommit(), getCommitActionType());
      writeMetadata.setCommitMetadata(Option.of(commitMetadata));
    }
    return writeMetadata;
  }

  /**
   * Stream to array conversion with generic type is not straightforward.
   * Implement a utility method to abstract high level logic. This needs to be improved in future
   */
  private JavaRDD<WriteStatus>[] convertStreamToArray(Stream<JavaRDD<WriteStatus>> writeStatusRDDStream) {
    Object[] writeStatusObjects = writeStatusRDDStream.toArray(Object[]::new);
    JavaRDD<WriteStatus>[] writeStatusRDDArray = new JavaRDD[writeStatusObjects.length];
    for (int i = 0; i < writeStatusObjects.length; i++) {
      writeStatusRDDArray[i] = (JavaRDD<WriteStatus>) writeStatusObjects[i];
    }
    return writeStatusRDDArray;
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

  /**
   * Submit job to execute clustering for the group.
   */
  private CompletableFuture<JavaRDD<WriteStatus>> runClusteringForGroupAsync(HoodieClusteringGroup clusteringGroup, Map<String, String> strategyParams) {
    CompletableFuture<JavaRDD<WriteStatus>> writeStatusesFuture = CompletableFuture.supplyAsync(() -> {
      JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
      JavaRDD<HoodieRecord<? extends HoodieRecordPayload>> inputRecords = readRecordsForGroup(jsc, clusteringGroup);
      Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
      return ((ClusteringExecutionStrategy<T, JavaRDD<HoodieRecord<? extends HoodieRecordPayload>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>>)
          ReflectionUtils.loadClass(config.getClusteringExecutionStrategyClass(), table, context, config))
          .performClustering(inputRecords, clusteringGroup.getNumOutputFileGroups(), instantTime, strategyParams, readerSchema);
    });

    return writeStatusesFuture;
  }

  @Override
  protected String getCommitActionType() {
    return HoodieTimeline.REPLACE_COMMIT_ACTION;
  }

  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(JavaRDD<WriteStatus> writeStatuses) {
    return ClusteringUtils.getFileGroupsFromClusteringPlan(clusteringPlan).collect(
        Collectors.groupingBy(fg -> fg.getPartitionPath(), Collectors.mapping(fg -> fg.getFileId(), Collectors.toList())));
  }

  /**
   * Get RDD of all records for the group. This includes all records from file slice (Apply updates from log files, if any).
   */
  private JavaRDD<HoodieRecord<? extends HoodieRecordPayload>> readRecordsForGroup(JavaSparkContext jsc, HoodieClusteringGroup clusteringGroup) {
    List<ClusteringOperation> clusteringOps = clusteringGroup.getSlices().stream().map(ClusteringOperation::create).collect(Collectors.toList());
    boolean hasLogFiles = clusteringOps.stream().filter(op -> op.getDeltaFilePaths().size() > 0).findAny().isPresent();
    if (hasLogFiles) {
      // if there are log files, we read all records into memory for a file group and apply updates.
      return readRecordsForGroupWithLogs(jsc, clusteringOps);
    } else {
      // We want to optimize reading records for case there are no log files.
      return readRecordsForGroupBaseFiles(jsc, clusteringOps);
    }
  }

  /**
   * Read records from baseFiles, apply updates and convert to RDD.
   */
  private JavaRDD<HoodieRecord<? extends HoodieRecordPayload>> readRecordsForGroupWithLogs(JavaSparkContext jsc,
                                                                                           List<ClusteringOperation> clusteringOps) {
    return jsc.parallelize(clusteringOps, clusteringOps.size()).mapPartitions(clusteringOpsPartition -> {
      List<Iterator<HoodieRecord<? extends HoodieRecordPayload>>> recordIterators = new ArrayList<>();
      clusteringOpsPartition.forEachRemaining(clusteringOp -> {
        long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(new SparkTaskContextSupplier(), config);
        LOG.info("MaxMemoryPerCompaction run as part of clustering => " + maxMemoryPerCompaction);
        try {
          Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
          HoodieFileReader<? extends IndexedRecord> baseFileReader = HoodieFileReaderFactory.getFileReader(table.getHadoopConf(), new Path(clusteringOp.getDataFilePath()));
          HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
              .withFileSystem(table.getMetaClient().getFs())
              .withBasePath(table.getMetaClient().getBasePath())
              .withLogFilePaths(clusteringOp.getDeltaFilePaths())
              .withReaderSchema(readerSchema)
              .withLatestInstantTime(instantTime)
              .withMaxMemorySizeInBytes(maxMemoryPerCompaction)
              .withReadBlocksLazily(config.getCompactionLazyBlockReadEnabled())
              .withReverseReader(config.getCompactionReverseLogReadEnabled())
              .withBufferSize(config.getMaxDFSStreamBufferSize())
              .withSpillableMapBasePath(config.getSpillableMapBasePath())
              .build();

          recordIterators.add(HoodieFileSliceReader.getFileSliceReader(baseFileReader, scanner, readerSchema,
              table.getMetaClient().getTableConfig().getPayloadClass(), table.getMetaClient().getTableConfig().getPreCombineField()));
        } catch (IOException e) {
          throw new HoodieClusteringException("Error reading input data for " + clusteringOp.getDataFilePath()
              + " and " + clusteringOp.getDeltaFilePaths(), e);
        }
      });

      return new ConcatenatingIterator<>(recordIterators);
    });
  }

  /**
   * Read records from baseFiles and convert to RDD.
   */
  private JavaRDD<HoodieRecord<? extends HoodieRecordPayload>> readRecordsForGroupBaseFiles(JavaSparkContext jsc,
                                                                                            List<ClusteringOperation> clusteringOps) {
    return jsc.parallelize(clusteringOps, clusteringOps.size()).mapPartitions(clusteringOpsPartition -> {
      List<Iterator<IndexedRecord>> iteratorsForPartition = new ArrayList<>();
      clusteringOpsPartition.forEachRemaining(clusteringOp -> {
        try {
          Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
          HoodieFileReader<IndexedRecord> baseFileReader = HoodieFileReaderFactory.getFileReader(table.getHadoopConf(), new Path(clusteringOp.getDataFilePath()));
          iteratorsForPartition.add(baseFileReader.getRecordIterator(readerSchema));
        } catch (IOException e) {
          throw new HoodieClusteringException("Error reading input data for " + clusteringOp.getDataFilePath()
              + " and " + clusteringOp.getDeltaFilePaths(), e);
        }
      });

      return new ConcatenatingIterator<>(iteratorsForPartition);
    }).map(this::transform);
  }

  /**
   * Transform IndexedRecord into HoodieRecord.
   */
  private HoodieRecord<? extends HoodieRecordPayload> transform(IndexedRecord indexedRecord) {
    GenericRecord record = (GenericRecord) indexedRecord;
    String key = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    String partition = record.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
    HoodieKey hoodieKey = new HoodieKey(key, partition);

    HoodieRecordPayload avroPayload = ReflectionUtils.loadPayload(table.getMetaClient().getTableConfig().getPayloadClass(),
        new Object[] {Option.of(record)}, Option.class);
    HoodieRecord hoodieRecord = new HoodieRecord(hoodieKey, avroPayload);
    return hoodieRecord;
  }

  private HoodieWriteMetadata<JavaRDD<WriteStatus>> buildWriteMetadata(JavaRDD<WriteStatus> writeStatusJavaRDD) {
    HoodieWriteMetadata<JavaRDD<WriteStatus>> result = new HoodieWriteMetadata<>();
    result.setPartitionToReplaceFileIds(getPartitionToReplacedFileIds(writeStatusJavaRDD));
    result.setWriteStatuses(writeStatusJavaRDD);
    result.setCommitMetadata(Option.empty());
    result.setCommitted(false);
    return result;
  }
}
