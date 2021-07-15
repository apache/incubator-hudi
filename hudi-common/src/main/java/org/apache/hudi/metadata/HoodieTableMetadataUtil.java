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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadata.NON_PARTITIONED_NAME;

/**
 * A utility to convert timeline information to metadata table records.
 */
public class HoodieTableMetadataUtil {

  private static final Logger LOG = LogManager.getLogger(HoodieTableMetadataUtil.class);

  /**
   * Converts a timeline instant to metadata table records.
   *
   * @param datasetMetaClient The meta client associated with the timeline instant
   * @param metadataTableTimeline Current timeline of the Metadata Table
   * @param instant to fetch and convert to metadata table records
   * @return a list of metadata table records
   * @throws IOException
   */
  public static Option<List<HoodieRecord>> convertInstantToMetaRecords(HoodieTableMetaClient datasetMetaClient,
      HoodieTimeline metadataTableTimeline, HoodieInstant instant, Option<String> lastSyncTs) throws IOException {
    HoodieTimeline timeline = datasetMetaClient.getActiveTimeline();
    Option<List<HoodieRecord>> records = Option.empty();
    ValidationUtils.checkArgument(instant.isCompleted(), "Only completed instants can be synced.");

    switch (instant.getAction()) {
      case HoodieTimeline.CLEAN_ACTION:
        HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(datasetMetaClient, instant);
        records = Option.of(convertMetadataToRecords(cleanMetadata, instant.getTimestamp()));
        break;
      case HoodieTimeline.DELTA_COMMIT_ACTION:
      case HoodieTimeline.COMMIT_ACTION:
      case HoodieTimeline.COMPACTION_ACTION:
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
        records = Option.of(convertMetadataToRecords(commitMetadata, instant.getTimestamp()));
        break;
      case HoodieTimeline.ROLLBACK_ACTION:
        HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(
            timeline.getInstantDetails(instant).get());
        records = Option.of(convertMetadataToRecords(metadataTableTimeline, rollbackMetadata, instant.getTimestamp(), lastSyncTs));
        break;
      case HoodieTimeline.RESTORE_ACTION:
        HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(
            timeline.getInstantDetails(instant).get());
        records = Option.of(convertMetadataToRecords(metadataTableTimeline, restoreMetadata, instant.getTimestamp(), lastSyncTs));
        break;
      case HoodieTimeline.SAVEPOINT_ACTION:
        // Nothing to be done here
        break;
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
        HoodieReplaceCommitMetadata replaceMetadata = HoodieReplaceCommitMetadata.fromBytes(
            timeline.getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
        // Note: we only add new files created here. Replaced files are removed from metadata later by cleaner.
        records = Option.of(convertMetadataToRecords(replaceMetadata, instant.getTimestamp()));
        break;
      default:
        throw new HoodieException("Unknown type of action " + instant.getAction());
    }

    return records;
  }

  /**
   * Finds all new files/partitions created as part of commit and creates metadata table records for them.
   *
   * @param commitMetadata
   * @param instantTime
   * @return a list of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToRecords(HoodieCommitMetadata commitMetadata, String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    List<String> allPartitions = new LinkedList<>();
    commitMetadata.getPartitionToWriteStats().forEach((partitionStatName, writeStats) -> {
      final String partition = partitionStatName.equals("") ? NON_PARTITIONED_NAME : partitionStatName;
      allPartitions.add(partition);

      Map<String, Long> newFiles = new HashMap<>(writeStats.size());
      writeStats.forEach(hoodieWriteStat -> {
        String pathWithPartition = hoodieWriteStat.getPath();
        if (pathWithPartition == null) {
          // Empty partition
          LOG.warn("Unable to find path in write stat to update metadata table " + hoodieWriteStat);
          return;
        }

        int offset = partition.equals(NON_PARTITIONED_NAME) ? 0 : partition.length() + 1;
        String filename = pathWithPartition.substring(offset);
        ValidationUtils.checkState(!newFiles.containsKey(filename), "Duplicate files in HoodieCommitMetadata");
        newFiles.put(filename, hoodieWriteStat.getTotalWriteBytes());
      });

      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(
          partition, Option.of(newFiles), Option.empty());
      records.add(record);
    });

    // New partitions created
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(new ArrayList<>(allPartitions));
    records.add(record);

    LOG.info("Updating at " + instantTime + " from Commit/" + commitMetadata.getOperationType()
        + ". #partitions_updated=" + records.size());
    return records;
  }

  /**
   * Finds all files that will be deleted as part of a planned clean and creates metadata table records for them.
   *
   * @param cleanerPlan from timeline to convert
   * @param instantTime
   * @return a list of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToRecords(HoodieCleanerPlan cleanerPlan, String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();

    int[] fileDeleteCount = {0};
    cleanerPlan.getFilePathsToBeDeletedPerPartition().forEach((partition, deletedPathInfo) -> {
      fileDeleteCount[0] += deletedPathInfo.size();

      // Files deleted from a partition
      List<String> deletedFilenames = deletedPathInfo.stream().map(p -> new Path(p.getFilePath()).getName())
          .collect(Collectors.toList());
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.empty(),
          Option.of(deletedFilenames));
      records.add(record);
    });

    LOG.info("Found at " + instantTime + " from CleanerPlan. #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileDeleteCount[0]);
    return records;
  }

  /**
   * Finds all files that were deleted as part of a clean and creates metadata table records for them.
   *
   * @param cleanMetadata
   * @param instantTime
   * @return a list of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToRecords(HoodieCleanMetadata cleanMetadata, String instantTime) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileDeleteCount = {0};

    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getSuccessDeleteFiles();
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.empty(),
          Option.of(new ArrayList<>(deletedFiles)));

      records.add(record);
      fileDeleteCount[0] += deletedFiles.size();
    });

    LOG.info("Updating at " + instantTime + " from Clean. #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileDeleteCount[0]);
    return records;
  }

  /**
   * Aggregates all files deleted and appended to from all rollbacks associated with a restore operation then
   * creates metadata table records for them.
   *
   * @param restoreMetadata
   * @param instantTime
   * @return a list of metadata table records
   */
  public static List<HoodieRecord> convertMetadataToRecords(HoodieTimeline metadataTableTimeline,
      HoodieRestoreMetadata restoreMetadata, String instantTime, Option<String> lastSyncTs) {
    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    restoreMetadata.getHoodieRestoreMetadata().values().forEach(rms -> {
      rms.forEach(rm -> processRollbackMetadata(metadataTableTimeline, rm, partitionToDeletedFiles, partitionToAppendedFiles, lastSyncTs));
    });

    return convertFilesToRecords(partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Restore");
  }

  public static List<HoodieRecord> convertMetadataToRecords(HoodieTimeline metadataTableTimeline,
      HoodieRollbackMetadata rollbackMetadata, String instantTime, Option<String> lastSyncTs) {
    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    processRollbackMetadata(metadataTableTimeline, rollbackMetadata, partitionToDeletedFiles, partitionToAppendedFiles, lastSyncTs);
    return convertFilesToRecords(partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Rollback");
  }

  /**
   * Extracts information about the deleted and append files from the {@code HoodieRollbackMetadata}.
   *
   * During a rollback files may be deleted (COW, MOR) or rollback blocks be appended (MOR only) to files. This
   * function will extract this change file for each partition.
   * @param metadataTableTimeline Current timeline of the Metdata Table
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param partitionToDeletedFiles The {@code Map} to fill with files deleted per partition.
   * @param partitionToAppendedFiles The {@code Map} to fill with files appended per partition and their sizes.
   */
  private static void processRollbackMetadata(HoodieTimeline metadataTableTimeline, HoodieRollbackMetadata rollbackMetadata,
                                              Map<String, List<String>> partitionToDeletedFiles,
                                              Map<String, Map<String, Long>> partitionToAppendedFiles,
                                              Option<String> lastSyncTs) {

    rollbackMetadata.getPartitionMetadata().values().forEach(pm -> {
      final String instantToRollback = rollbackMetadata.getCommitsRollback().get(0);
      // Has this rollback produced new files?
      boolean hasRollbackLogFiles = pm.getRollbackLogFiles() != null && !pm.getRollbackLogFiles().isEmpty();
      boolean hasNonZeroRollbackLogFiles = hasRollbackLogFiles && pm.getRollbackLogFiles().values().stream().mapToLong(Long::longValue).sum() > 0;

      // If instant-to-rollback has not been synced to metadata table yet then there is no need to update metadata
      // This can happen in two cases:
      //  Case 1: Metadata Table timeline is behind the instant-to-rollback.
      boolean shouldSkip = lastSyncTs.isPresent()
          && HoodieTimeline.compareTimestamps(instantToRollback, HoodieTimeline.GREATER_THAN, lastSyncTs.get());

      if (!hasNonZeroRollbackLogFiles && shouldSkip) {
        LOG.info(String.format("Skipping syncing of rollbackMetadata at %s, given metadata table is already synced upto to %s",
            instantToRollback, lastSyncTs.get()));
        return;
      }

      // Case 2: The instant-to-rollback was never committed to Metadata Table. This can happen if the instant-to-rollback
      // was a failed commit (never completed) as only completed instants are synced to Metadata Table.
      HoodieInstant syncedInstant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, instantToRollback);
      shouldSkip = !metadataTableTimeline.containsInstant(syncedInstant);
      if (!hasNonZeroRollbackLogFiles && shouldSkip) {
        LOG.info(String.format("Skipping syncing of rollbackMetadata at %s, since this instant was never committed to Metadata Table",
            instantToRollback));
        return;
      }

      // The above check only succeeds if the required Metadata Table instants were not archived
      if (metadataTableTimeline.isBeforeTimelineStarts(syncedInstant.getTimestamp())) {
        throw new HoodieMetadataException(String.format("The instant %s required to sync rollback of %s has been archived",
            syncedInstant, instantToRollback));
      }

      final String partition = pm.getPartitionPath();
      if (!pm.getSuccessDeleteFiles().isEmpty() && !shouldSkip) {
        if (!partitionToDeletedFiles.containsKey(partition)) {
          partitionToDeletedFiles.put(partition, new ArrayList<>());
        }

        // Extract deleted file name from the absolute paths saved in getSuccessDeleteFiles()
        List<String> deletedFiles = pm.getSuccessDeleteFiles().stream().map(p -> new Path(p).getName())
            .collect(Collectors.toList());
        partitionToDeletedFiles.get(partition).addAll(deletedFiles);
      }

      BiFunction<Long, Long, Long> fileMergeFn = (oldSize, newSizeCopy) -> {
        // if a file exists in both written log files and rollback log files, we want to pick the one that is higher
        // as rollback file could have been updated after written log files are computed.
        return oldSize > newSizeCopy ? oldSize : newSizeCopy;
      };

      if (hasRollbackLogFiles) {
        if (!partitionToAppendedFiles.containsKey(partition)) {
          partitionToAppendedFiles.put(partition, new HashMap<>());
        }

        // Extract appended file name from the absolute paths saved in getAppendFiles()
        pm.getRollbackLogFiles().forEach((path, size) -> {
          partitionToAppendedFiles.get(partition).merge(new Path(path).getName(), size, fileMergeFn);
        });
      }

      if (pm.getWrittenLogFiles() != null && !pm.getWrittenLogFiles().isEmpty()) {
        if (!partitionToAppendedFiles.containsKey(partition)) {
          partitionToAppendedFiles.put(partition, new HashMap<>());
        }

        // Extract appended file name from the absolute paths saved in getWrittenLogFiles()
        pm.getWrittenLogFiles().forEach((path, size) -> {
          partitionToAppendedFiles.get(partition).merge(new Path(path).getName(), size, fileMergeFn);
        });
      }
    });
  }

  private static List<HoodieRecord> convertFilesToRecords(Map<String, List<String>> partitionToDeletedFiles,
                                                          Map<String, Map<String, Long>> partitionToAppendedFiles, String instantTime,
                                                          String operation) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileChangeCount = {0, 0}; // deletes, appends

    partitionToDeletedFiles.forEach((partition, deletedFiles) -> {
      fileChangeCount[0] += deletedFiles.size();

      Option<Map<String, Long>> filesAdded = Option.empty();
      if (partitionToAppendedFiles.containsKey(partition)) {
        filesAdded = Option.of(partitionToAppendedFiles.remove(partition));
      }

      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, filesAdded,
          Option.of(new ArrayList<>(deletedFiles)));
      records.add(record);
    });

    partitionToAppendedFiles.forEach((partition, appendedFileMap) -> {
      fileChangeCount[1] += appendedFileMap.size();

      // Validate that no appended file has been deleted
      ValidationUtils.checkState(
          !appendedFileMap.keySet().removeAll(partitionToDeletedFiles.getOrDefault(partition, Collections.emptyList())),
          "Rollback file cannot both be appended and deleted");

      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.of(appendedFileMap),
          Option.empty());
      records.add(record);
    });

    LOG.info("Found at " + instantTime + " from " + operation + ". #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileChangeCount[0] + ", #files_appended=" + fileChangeCount[1]);

    return records;
  }
}
