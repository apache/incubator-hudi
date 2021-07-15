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

package org.apache.hudi.client;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.FlinkHoodieIndex;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.io.FlinkAppendHandle;
import org.apache.hudi.io.FlinkCreateHandle;
import org.apache.hudi.io.FlinkMergeAndReplaceHandle;
import org.apache.hudi.io.FlinkMergeHandle;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.io.MiniBatchHandle;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.HoodieTimelineArchiveLog;
import org.apache.hudi.table.MarkerFiles;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.table.action.compact.FlinkCompactHelpers;
import org.apache.hudi.table.upgrade.FlinkUpgradeDowngrade;
import org.apache.hudi.util.FlinkClientUtil;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:LineLength")
public class HoodieFlinkWriteClient<T extends HoodieRecordPayload> extends
    AbstractHoodieWriteClient<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkWriteClient.class);

  /**
   * FileID to write handle mapping in order to record the write handles for each file group,
   * so that we can append the mini-batch data buffer incrementally.
   */
  private Map<String, HoodieWriteHandle<?, ?, ?, ?>> bucketToHandles;

  public HoodieFlinkWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    this(context, clientConfig, false);
  }

  @Deprecated
  public HoodieFlinkWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending) {
    super(context, writeConfig);
    this.bucketToHandles = new HashMap<>();
  }

  @Deprecated
  public HoodieFlinkWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending,
                                Option<EmbeddedTimelineService> timelineService) {
    super(context, writeConfig, timelineService);
  }

  /**
   * Complete changes performed at the given instantTime marker with specified action.
   */
  @Override
  protected HoodieIndex<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> createIndex(HoodieWriteConfig writeConfig) {
    return FlinkHoodieIndex.createIndex((HoodieFlinkEngineContext) context, config);
  }

  @Override
  public boolean commit(String instantTime, List<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata, String commitActionType, Map<String, List<String>> partitionToReplacedFileIds) {
    List<HoodieWriteStat> writeStats = writeStatuses.parallelStream().map(WriteStatus::getStat).collect(Collectors.toList());
    return commitStats(instantTime, writeStats, extraMetadata, commitActionType, partitionToReplacedFileIds);
  }

  @Override
  protected HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> createTable(HoodieWriteConfig config, Configuration hadoopConf) {
    return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
  }

  @Override
  public List<HoodieRecord<T>> filterExists(List<HoodieRecord<T>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieFlinkTable<T> table = getHoodieTable();
    Timer.Context indexTimer = metrics.getIndexCtx();
    List<HoodieRecord<T>> recordsWithLocation = getIndex().tagLocation(hoodieRecords, context, table);
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    return recordsWithLocation.stream().filter(v1 -> !v1.isCurrentLocationKnown()).collect(Collectors.toList());
  }

  @Override
  public void bootstrap(Option<Map<String, String>> extraMetadata) {
    throw new HoodieNotSupportedException("Bootstrap operation is not supported yet");
  }

  @Override
  public List<WriteStatus> upsert(List<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.UPSERT, instantTime);
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.UPSERT, table.getMetaClient());
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle = getOrCreateWriteHandle(records.get(0), getConfig(),
        instantTime, table, records.listIterator());
    HoodieWriteMetadata<List<WriteStatus>> result = ((HoodieFlinkTable<T>) table).upsert(context, writeHandle, instantTime, records);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> upsertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime) {
    throw new HoodieNotSupportedException("UpsertPrepped operation is not supported yet");
  }

  @Override
  public List<WriteStatus> insert(List<HoodieRecord<T>> records, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.INSERT, instantTime);
    table.validateUpsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT, table.getMetaClient());
    // create the write handle if not exists
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle = getOrCreateWriteHandle(records.get(0), getConfig(),
        instantTime, table, records.listIterator());
    HoodieWriteMetadata<List<WriteStatus>> result = ((HoodieFlinkTable<T>) table).insert(context, writeHandle, instantTime, records);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  /**
   * Removes all existing records from the partitions affected and inserts the given HoodieRecords, into the table.
   *
   * @param records     HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return list of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> insertOverwrite(
      List<HoodieRecord<T>> records, final String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.INSERT_OVERWRITE, instantTime);
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_OVERWRITE, table.getMetaClient());
    // create the write handle if not exists
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle = getOrCreateWriteHandle(records.get(0), getConfig(),
        instantTime, table, records.listIterator());
    HoodieWriteMetadata result = ((HoodieFlinkTable<T>) table).insertOverwrite(context, writeHandle, instantTime, records);
    return postWrite(result, instantTime, table);
  }

  /**
   * Removes all existing records of the Hoodie table and inserts the given HoodieRecords, into the table.
   *
   * @param records     HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return list of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> insertOverwriteTable(
      List<HoodieRecord<T>> records, final String instantTime) {
    HoodieTable table = getTableAndInitCtx(WriteOperationType.INSERT_OVERWRITE_TABLE, instantTime);
    table.validateInsertSchema();
    preWrite(instantTime, WriteOperationType.INSERT_OVERWRITE_TABLE, table.getMetaClient());
    // create the write handle if not exists
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle = getOrCreateWriteHandle(records.get(0), getConfig(),
        instantTime, table, records.listIterator());
    HoodieWriteMetadata result = ((HoodieFlinkTable<T>) table).insertOverwriteTable(context, writeHandle, instantTime, records);
    return postWrite(result, instantTime, table);
  }

  @Override
  public List<WriteStatus> insertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime) {
    throw new HoodieNotSupportedException("InsertPrepped operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records, String instantTime) {
    throw new HoodieNotSupportedException("BulkInsert operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records, String instantTime, Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsert operation is not supported yet");
  }

  @Override
  public List<WriteStatus> bulkInsertPreppedRecords(List<HoodieRecord<T>> preppedRecords, String instantTime, Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> bulkInsertPartitioner) {
    throw new HoodieNotSupportedException("BulkInsertPrepped operation is not supported yet");
  }

  @Override
  public List<WriteStatus> delete(List<HoodieKey> keys, String instantTime) {
    HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.DELETE, instantTime);
    preWrite(instantTime, WriteOperationType.DELETE, table.getMetaClient());
    HoodieWriteMetadata<List<WriteStatus>> result = table.delete(context, instantTime, keys);
    return postWrite(result, instantTime, table);
  }

  @Override
  protected void preWrite(String instantTime, WriteOperationType writeOperationType, HoodieTableMetaClient metaClient) {
    setOperationType(writeOperationType);
    // Note: the code to read the commit metadata is not thread safe for JSON deserialization,
    // remove the table metadata sync

    // remove the async cleaning
  }

  /**
   * Starts async cleaning service for finished commits.
   *
   * <p>The Flink write client is designed to write data set as buckets
   * but cleaning action should trigger after all the write actions within a
   * checkpoint finish.
   */
  public void startAsyncCleaning() {
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this);
  }

  /**
   * Blocks and wait for the async cleaning service to finish.
   *
   * <p>The Flink write client is designed to write data set as buckets
   * but cleaning action should trigger after all the write actions within a
   * checkpoint finish.
   */
  public void waitForCleaningFinish() {
    if (this.asyncCleanerService != null) {
      LOG.info("Cleaner has been spawned already. Waiting for it to finish");
      AsyncCleanerService.waitForCompletion(asyncCleanerService);
      LOG.info("Cleaner has finished");
    }
  }

  @Override
  protected List<WriteStatus> postWrite(HoodieWriteMetadata<List<WriteStatus>> result,
                                        String instantTime,
                                        HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) {
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(getOperationType().name(), result.getIndexUpdateDuration().get().toMillis());
    }
    return result.getWriteStatuses();
  }

  /**
   * Post commit is rewrite to be invoked after a successful commit.
   *
   * <p>The Flink write client is designed to write data set as buckets
   * but cleaning action should trigger after all the write actions within a
   * checkpoint finish.
   *
   * @param table         Table to commit on
   * @param metadata      Commit Metadata corresponding to committed instant
   * @param instantTime   Instant Time
   * @param extraMetadata Additional Metadata passed by user
   */
  @Override
  protected void postCommit(HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
                            HoodieCommitMetadata metadata,
                            String instantTime,
                            Option<Map<String, String>> extraMetadata) {
    try {
      // Delete the marker directory for the instant.
      new MarkerFiles(createTable(config, hadoopConf), instantTime)
          .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
      // We cannot have unbounded commit files. Archive commits if we have to archive
      HoodieTimelineArchiveLog archiveLog = new HoodieTimelineArchiveLog(config, table);
      archiveLog.archiveIfRequired(context);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    } finally {
      this.heartbeatClient.stop(instantTime);
    }
  }

  @Override
  public void commitCompaction(
      String compactionInstantTime,
      List<WriteStatus> writeStatuses,
      Option<Map<String, String>> extraMetadata) throws IOException {
    HoodieFlinkTable<T> table = getHoodieTable();
    HoodieCommitMetadata metadata = FlinkCompactHelpers.newInstance().createCompactionMetadata(
        table, compactionInstantTime, writeStatuses, config.getSchema());
    extraMetadata.ifPresent(m -> m.forEach(metadata::addMetadata));
    completeCompaction(metadata, writeStatuses, table, compactionInstantTime);
  }

  @Override
  public void completeCompaction(
      HoodieCommitMetadata metadata,
      List<WriteStatus> writeStatuses,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
      String compactionCommitTime) {
    this.context.setJobStatus(this.getClass().getSimpleName(), "Collect compaction write status and commit compaction");
    List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
    finalizeWrite(table, compactionCommitTime, writeStats);
    LOG.info("Committing Compaction {} finished with result {}.", compactionCommitTime, metadata);
    FlinkCompactHelpers.newInstance().completeInflightCompaction(table, compactionCommitTime, metadata);

    if (compactionTimer != null) {
      long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
      try {
        metrics.updateCommitMetrics(HoodieActiveTimeline.parseInstantTime(compactionCommitTime).getTime(),
            durationInMs, metadata, HoodieActiveTimeline.COMPACTION_ACTION);
      } catch (ParseException e) {
        throw new HoodieCommitException("Commit time is not of valid format. Failed to commit compaction "
            + config.getBasePath() + " at time " + compactionCommitTime, e);
      }
    }
    LOG.info("Compacted successfully on commit " + compactionCommitTime);
  }

  @Override
  protected List<WriteStatus> compact(String compactionInstantTime, boolean shouldComplete) {
    throw new HoodieNotSupportedException("Compaction is not supported yet");
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> cluster(final String clusteringInstant, final boolean shouldComplete) {
    throw new HoodieNotSupportedException("Clustering is not supported yet");
  }

  @Override
  protected HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> getTableAndInitCtx(WriteOperationType operationType, String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    new FlinkUpgradeDowngrade(metaClient, config, context).run(metaClient, HoodieTableVersion.current(), config, context, instantTime);
    return getTableAndInitCtx(metaClient, operationType);
  }

  /**
   * Clean the write handles within a checkpoint interval.
   * All the handles should have been closed already.
   */
  public void cleanHandles() {
    this.bucketToHandles.clear();
  }

  /**
   * Clean the write handles within a checkpoint interval, this operation
   * would close the underneath file handles, if any error happens, clean the
   * corrupted data file.
   */
  public void cleanHandlesGracefully() {
    this.bucketToHandles.values()
        .forEach(handle -> ((MiniBatchHandle) handle).closeGracefully());
    this.bucketToHandles.clear();
  }

  /**
   * Get or create a new write handle in order to reuse the file handles.
   *
   * @param record      The first record in the bucket
   * @param config      Write config
   * @param instantTime The instant time
   * @param table       The table
   * @param recordItr   Record iterator
   * @return Existing write handle or create a new one
   */
  private HoodieWriteHandle<?, ?, ?, ?> getOrCreateWriteHandle(
      HoodieRecord<T> record,
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
      Iterator<HoodieRecord<T>> recordItr) {
    final HoodieRecordLocation loc = record.getCurrentLocation();
    final String fileID = loc.getFileId();
    final String partitionPath = record.getPartitionPath();
    // append only mode always use FlinkCreateHandle
    if (loc.getInstantTime().equals(BucketType.APPEND_ONLY.name())) {
      return new FlinkCreateHandle<>(config, instantTime, table, partitionPath,
          fileID, table.getTaskContextSupplier());
    }

    if (bucketToHandles.containsKey(fileID)) {
      MiniBatchHandle lastHandle = (MiniBatchHandle) bucketToHandles.get(fileID);
      if (lastHandle.shouldReplace()) {
        HoodieWriteHandle<?, ?, ?, ?> writeHandle = new FlinkMergeAndReplaceHandle<>(
            config, instantTime, table, recordItr, partitionPath, fileID, table.getTaskContextSupplier(),
            lastHandle.getWritePath());
        this.bucketToHandles.put(fileID, writeHandle); // override with new replace handle
        return writeHandle;
      }
    }

    final boolean isDelta = table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ);
    final HoodieWriteHandle<?, ?, ?, ?> writeHandle;
    if (isDelta) {
      writeHandle = new FlinkAppendHandle<>(config, instantTime, table, partitionPath, fileID, recordItr,
          table.getTaskContextSupplier());
    } else if (loc.getInstantTime().equals(BucketType.INSERT.name()) || loc.getInstantTime().equals(BucketType.APPEND_ONLY.name())) {
      // use the same handle for insert bucket
      writeHandle = new FlinkCreateHandle<>(config, instantTime, table, partitionPath,
          fileID, table.getTaskContextSupplier());
    } else {
      writeHandle = new FlinkMergeHandle<>(config, instantTime, table, recordItr, partitionPath,
          fileID, table.getTaskContextSupplier());
    }
    this.bucketToHandles.put(fileID, writeHandle);
    return writeHandle;
  }

  private HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> getTableAndInitCtx(HoodieTableMetaClient metaClient, WriteOperationType operationType) {
    if (operationType == WriteOperationType.DELETE) {
      setWriteSchemaForDeletes(metaClient);
    }
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieFlinkTable<T> table = getHoodieTable();
    if (table.getMetaClient().getCommitActionType().equals(HoodieTimeline.COMMIT_ACTION)) {
      writeTimer = metrics.getCommitCtx();
    } else {
      writeTimer = metrics.getDeltaCommitCtx();
    }
    return table;
  }

  public String getLastPendingInstant(HoodieTableType tableType) {
    final String actionType = CommitUtils.getCommitActionType(tableType);
    return getLastPendingInstant(actionType);
  }

  public String getLastPendingInstant(String actionType) {
    HoodieTimeline unCompletedTimeline = FlinkClientUtil.createMetaClient(basePath)
        .getCommitsTimeline().filterInflightsAndRequested();
    return unCompletedTimeline.getInstants()
        .filter(x -> x.getAction().equals(actionType))
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList()).stream()
        .max(Comparator.naturalOrder())
        .orElse(null);
  }

  public String getLastCompletedInstant(HoodieTableType tableType) {
    final String commitType = CommitUtils.getCommitActionType(tableType);
    HoodieTimeline completedTimeline = FlinkClientUtil.createMetaClient(basePath)
        .getCommitsTimeline().filterCompletedInstants();
    return completedTimeline.getInstants()
        .filter(x -> x.getAction().equals(commitType))
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList()).stream()
        .max(Comparator.naturalOrder())
        .orElse(null);
  }

  public void transitionRequestedToInflight(String commitType, String inFlightInstant) {
    HoodieActiveTimeline activeTimeline = FlinkClientUtil.createMetaClient(basePath).getActiveTimeline();
    HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, commitType, inFlightInstant);
    activeTimeline.transitionRequestedToInflight(requested, Option.empty(),
        config.shouldAllowMultiWriteOnSameInstant());
  }

  public HoodieFlinkTable<T> getHoodieTable() {
    return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);
  }

  public Map<String, List<String>> getPartitionToReplacedFileIds(
      WriteOperationType writeOperationType,
      List<WriteStatus> writeStatuses) {
    HoodieFlinkTable<T> table = getHoodieTable();
    switch (writeOperationType) {
      case INSERT_OVERWRITE:
        return writeStatuses.stream().map(status -> status.getStat().getPartitionPath()).distinct()
            .collect(
                Collectors.toMap(
                    partition -> partition,
                    partitionPath -> getAllExistingFileIds(table, partitionPath)));
      case INSERT_OVERWRITE_TABLE:
        Map<String, List<String>> partitionToExistingFileIds = new HashMap<>();
        List<String> partitionPaths =
            FSUtils.getAllPartitionPaths(context, config.getMetadataConfig(), table.getMetaClient().getBasePath());
        if (partitionPaths != null && partitionPaths.size() > 0) {
          context.setJobStatus(this.getClass().getSimpleName(), "Getting ExistingFileIds of all partitions");
          partitionToExistingFileIds = partitionPaths.stream().parallel()
              .collect(
                  Collectors.toMap(
                      partition -> partition,
                      partition -> getAllExistingFileIds(table, partition)));
        }
        return partitionToExistingFileIds;
      default:
        throw new AssertionError();
    }
  }

  private List<String> getAllExistingFileIds(HoodieFlinkTable<T> table, String partitionPath) {
    // because new commit is not complete. it is safe to mark all existing file Ids as old files
    return table.getSliceView().getLatestFileSlices(partitionPath).map(FileSlice::getFileId).distinct().collect(Collectors.toList());
  }
}
