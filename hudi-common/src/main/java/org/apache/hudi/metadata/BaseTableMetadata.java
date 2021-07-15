
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

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieMetadataException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseTableMetadata implements HoodieTableMetadata {

  private static final Logger LOG = LogManager.getLogger(BaseTableMetadata.class);

  static final long MAX_MEMORY_SIZE_IN_BYTES = 1024 * 1024 * 1024;
  static final int BUFFER_SIZE = 10 * 1024 * 1024;

  protected final transient HoodieEngineContext engineContext;
  protected final SerializableConfiguration hadoopConf;
  protected final String datasetBasePath;
  protected final HoodieTableMetaClient datasetMetaClient;
  protected final Option<HoodieMetadataMetrics> metrics;
  protected final HoodieMetadataConfig metadataConfig;
  // Directory used for Spillable Map when merging records
  protected final String spillableMapDirectory;

  protected boolean enabled;
  private TimelineMergedTableMetadata timelineMergedMetadata;

  protected BaseTableMetadata(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                              String datasetBasePath, String spillableMapDirectory) {
    this.engineContext = engineContext;
    this.hadoopConf = new SerializableConfiguration(engineContext.getHadoopConf());
    this.datasetBasePath = datasetBasePath;
    this.datasetMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf.get()).setBasePath(datasetBasePath).build();
    this.spillableMapDirectory = spillableMapDirectory;
    this.metadataConfig = metadataConfig;

    this.enabled = metadataConfig.useFileListingMetadata();
    if (metadataConfig.enableMetrics()) {
      this.metrics = Option.of(new HoodieMetadataMetrics(Registry.getRegistry("HoodieMetadata")));
    } else {
      this.metrics = Option.empty();
    }
  }

  /**
   * Return the list of partitions in the dataset.
   *
   * If the Metadata Table is enabled, the listing is retrieved from the stored metadata. Otherwise, the list of
   * partitions is retrieved directly from the underlying {@code FileSystem}.
   *
   * On any errors retrieving the listing from the metadata, defaults to using the file system listings.
   *
   */
  @Override
  public List<String> getAllPartitionPaths() throws IOException {
    if (enabled) {
      try {
        return fetchAllPartitionPaths();
      } catch (Exception e) {
        throw new HoodieMetadataException("Failed to retrieve list of partition from metadata", e);
      }
    }
    return new FileSystemBackedTableMetadata(getEngineContext(), hadoopConf, datasetBasePath,
        metadataConfig.shouldAssumeDatePartitioning()).getAllPartitionPaths();
  }

  /**
   * Return the list of files in a partition.
   *
   * If the Metadata Table is enabled, the listing is retrieved from the stored metadata. Otherwise, the list of
   * partitions is retrieved directly from the underlying {@code FileSystem}.
   *
   * On any errors retrieving the listing from the metadata, defaults to using the file system listings.
   *
   * @param partitionPath The absolute path of the partition to list
   */
  @Override
  public FileStatus[] getAllFilesInPartition(Path partitionPath)
      throws IOException {
    if (enabled) {
      try {
        return fetchAllFilesInPartition(partitionPath);
      } catch (Exception e) {
        throw new HoodieMetadataException("Failed to retrieve files in partition " + partitionPath + " from metadata", e);
      }
    }

    return new FileSystemBackedTableMetadata(getEngineContext(), hadoopConf, datasetBasePath, metadataConfig.shouldAssumeDatePartitioning())
        .getAllFilesInPartition(partitionPath);
  }

  /**
   * Returns a list of all partitions.
   */
  protected List<String> fetchAllPartitionPaths() throws IOException {
    HoodieTimer timer = new HoodieTimer().startTimer();
    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getMergedRecordByKey(RECORDKEY_PARTITION_LIST);
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_PARTITIONS_STR, timer.endTimer()));

    List<String> partitions = Collections.emptyList();
    if (hoodieRecord.isPresent()) {
      if (!hoodieRecord.get().getData().getDeletions().isEmpty()) {
        throw new HoodieMetadataException("Metadata partition list record is inconsistent: "
            + hoodieRecord.get().getData());
      }

      partitions = hoodieRecord.get().getData().getFilenames();
      // Partition-less tables have a single empty partition
      if (partitions.contains(NON_PARTITIONED_NAME)) {
        partitions.remove(NON_PARTITIONED_NAME);
        partitions.add("");
      }
    }

    if (metadataConfig.validateFileListingMetadata()) {
      // Validate the Metadata Table data by listing the partitions from the file system
      timer.startTimer();
      FileSystemBackedTableMetadata fileSystemBackedTableMetadata = new FileSystemBackedTableMetadata(getEngineContext(),
          hadoopConf, datasetBasePath, metadataConfig.shouldAssumeDatePartitioning());
      List<String> actualPartitions = fileSystemBackedTableMetadata.getAllPartitionPaths();
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.VALIDATE_PARTITIONS_STR, timer.endTimer()));

      Collections.sort(actualPartitions);
      Collections.sort(partitions);
      if (!actualPartitions.equals(partitions)) {
        LOG.error("Validation of metadata partition list failed. Lists do not match.");
        LOG.error("Partitions from metadata: " + Arrays.toString(partitions.toArray()));
        LOG.error("Partitions from file system: " + Arrays.toString(actualPartitions.toArray()));

        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.VALIDATE_ERRORS_STR, 0));
      }

      // Return the direct listing as it should be correct
      partitions = actualPartitions;
    }

    LOG.info("Listed partitions from metadata: #partitions=" + partitions.size());
    return partitions;
  }

  /**
   * Return all the files from the partition.
   *
   * @param partitionPath The absolute path of the partition
   */
  FileStatus[] fetchAllFilesInPartition(Path partitionPath) throws IOException {
    String partitionName = FSUtils.getRelativePartitionPath(new Path(datasetBasePath), partitionPath);
    if (partitionName.isEmpty()) {
      partitionName = NON_PARTITIONED_NAME;
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    Option<HoodieRecord<HoodieMetadataPayload>> hoodieRecord = getMergedRecordByKey(partitionName);
    metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.LOOKUP_FILES_STR, timer.endTimer()));

    FileStatus[] statuses = {};
    if (hoodieRecord.isPresent()) {
      if (!hoodieRecord.get().getData().getDeletions().isEmpty()) {
        throw new HoodieMetadataException("Metadata record for partition " + partitionName + " is inconsistent: "
            + hoodieRecord.get().getData());
      }
      statuses = hoodieRecord.get().getData().getFileStatuses(hadoopConf.get(), partitionPath);
    }

    if (metadataConfig.validateFileListingMetadata()) {
      // Validate the Metadata Table data by listing the partitions from the file system
      timer.startTimer();

      String partitionPathStr = FSUtils.getRelativePartitionPath(new Path(datasetMetaClient.getBasePath()), partitionPath);
      String latestDataInstantTime = getLatestDatasetInstantTime();
      HoodieTableFileSystemView dataFsView = new HoodieTableFileSystemView(datasetMetaClient, datasetMetaClient.getActiveTimeline());
      List<FileStatus> directStatuses = dataFsView.getAllFileSlices(partitionPathStr).flatMap(slice -> {
        List<FileStatus> paths = new ArrayList<>();
        slice.getBaseFile().ifPresent(baseFile -> {
          if (HoodieTimeline.compareTimestamps(baseFile.getCommitTime(), HoodieTimeline.LESSER_THAN_OR_EQUALS, latestDataInstantTime)) {
            paths.add(baseFile.getFileStatus());
          }
        });
        //TODO(metadata): this will remain problematic; no way to know the commit time based on log file written
        slice.getLogFiles().forEach(logFile -> paths.add(logFile.getFileStatus()));
        return paths.stream();
      }).collect(Collectors.toList());

      List<String> directFilenames = directStatuses.stream()
          .map(fileStatus -> fileStatus.getPath().getName()).sorted()
          .collect(Collectors.toList());
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.VALIDATE_FILES_STR, timer.endTimer()));

      List<String> metadataFilenames = Arrays.stream(statuses)
          .map(s -> s.getPath().getName()).sorted()
          .collect(Collectors.toList());

      if (!metadataFilenames.equals(directFilenames)) {
        LOG.error("Validation of metadata file listing for partition " + partitionName + " failed.");
        LOG.error("File list from metadata: " + Arrays.toString(metadataFilenames.toArray()));
        LOG.error("File list from direct listing: " + Arrays.toString(directFilenames.toArray()));

        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.VALIDATE_ERRORS_STR, 0));
      }

      // Return the direct listing as it should be correct
      statuses = directStatuses.toArray(new FileStatus[0]);
    }

    LOG.info("Listed file in partition from metadata: partition=" + partitionName + ", #files=" + statuses.length);
    return statuses;
  }

  /**
   * Retrieve the merged {@code HoodieRecord} mapped to the given key.
   *
   * @param key The key of the record
   */
  private Option<HoodieRecord<HoodieMetadataPayload>> getMergedRecordByKey(String key) {
    Option<HoodieRecord<HoodieMetadataPayload>> mergedRecord;
    Option<HoodieRecord<HoodieMetadataPayload>> metadataHoodieRecord = getRecordByKeyFromMetadata(key);
    // Retrieve record from unsynced timeline instants
    Option<HoodieRecord<HoodieMetadataPayload>> timelineHoodieRecord = timelineMergedMetadata.getRecordByKey(key);
    if (timelineHoodieRecord.isPresent()) {
      if (metadataHoodieRecord.isPresent()) {
        HoodieRecordPayload mergedPayload = timelineHoodieRecord.get().getData().preCombine(metadataHoodieRecord.get().getData());
        mergedRecord = Option.of(new HoodieRecord(metadataHoodieRecord.get().getKey(), mergedPayload));
      } else {
        mergedRecord = timelineHoodieRecord;
      }
    } else {
      mergedRecord = metadataHoodieRecord;
    }
    return mergedRecord;
  }

  protected abstract Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKeyFromMetadata(String key);

  protected void openTimelineScanner(HoodieTimeline metadataTableTimeline) {
    if (timelineMergedMetadata == null) {
      List<HoodieInstant> unSyncedInstants = findInstantsToSyncForReader();
      timelineMergedMetadata =
          new TimelineMergedTableMetadata(datasetMetaClient, metadataTableTimeline, unSyncedInstants, getSyncedInstantTime(), null);
    }
  }

  /**
   * Return the instants which are not-synced to the {@code HoodieTableMetadata}.
   *
   * This is the list of all completed but un-synched instants.
   */
  protected abstract List<HoodieInstant> findInstantsToSyncForReader();

  /**
   * Return the instants which are not-synced to the {@code HoodieTableMetadataWriter}.
   *
   * This is the list of all completed but un-synched instants which do not have any incomplete instants in between them.
   */
  protected abstract List<HoodieInstant> findInstantsToSyncForWriter();

  @Override
  public boolean isInSync() {
    return enabled && findInstantsToSyncForWriter().isEmpty();
  }

  protected HoodieEngineContext getEngineContext() {
    return engineContext != null ? engineContext : new HoodieLocalEngineContext(hadoopConf.get());
  }

  public HoodieMetadataConfig getMetadataConfig() {
    return metadataConfig;
  }

  protected String getLatestDatasetInstantTime() {
    return datasetMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant()
        .map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);
  }

  @Override
  public Option<String> getSyncedInstantTimeForReader() {
    if (timelineMergedMetadata == null) {
      return Option.empty();
    }

    return timelineMergedMetadata.getSyncedInstantTime();
  }

}
