/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.sink.partitioner.profile.WriteProfile;
import org.apache.hudi.sink.partitioner.profile.WriteProfiles;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.table.action.commit.SmallFile;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.util.Preconditions;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bucket assigner that assigns the data buffer of one checkpoint into buckets.
 *
 * <p>This assigner assigns the record one by one.
 * If the record is an update, checks and reuse existing UPDATE bucket or generates a new one;
 * If the record is an insert, checks the record partition for small files first, try to find a small file
 * that has space to append new records and reuse the small file's data bucket, if
 * there is no small file(or no left space for new records), generates an INSERT bucket.
 *
 * <p>Use {partition}_{fileId} as the bucket identifier, so that the bucket is unique
 * within and among partitions.
 */
public class BucketAssigner implements AutoCloseable {
  private static final Logger LOG = LogManager.getLogger(BucketAssigner.class);

  /**
   * Task ID.
   */
  private final int taskID;

  /**
   * Number of tasks.
   */
  private final int numTasks;

  /**
   * Remembers what type each bucket is for later.
   */
  private final HashMap<String, BucketInfo> bucketInfoMap;

  /**
   * The write config.
   */
  protected final HoodieWriteConfig config;

  /**
   * Write profile.
   */
  private final WriteProfile writeProfile;

  /**
   * Partition path to small file assign mapping.
   */
  private final Map<String, SmallFileAssign> smallFileAssignMap;

  /**
   * Bucket ID(partition + fileId) -> new file assign state.
   */
  private final Map<String, NewFileAssignState> newFileAssignStates;

  public BucketAssigner(
      int taskID,
      int numTasks,
      WriteProfile profile,
      HoodieWriteConfig config) {
    this.taskID = taskID;
    this.numTasks = numTasks;
    this.config = config;
    this.writeProfile = profile;

    this.bucketInfoMap = new HashMap<>();
    this.smallFileAssignMap = new HashMap<>();
    this.newFileAssignStates = new HashMap<>();
  }

  /**
   * Reset the states of this assigner, should do once for each checkpoint,
   * all the states are accumulated within one checkpoint interval.
   */
  public void reset() {
    bucketInfoMap.clear();
    newFileAssignStates.clear();
  }

  public BucketInfo addUpdate(String partitionPath, String fileIdHint) {
    final String key = StreamerUtil.generateBucketKey(partitionPath, fileIdHint);
    if (!bucketInfoMap.containsKey(key)) {
      BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, fileIdHint, partitionPath);
      bucketInfoMap.put(key, bucketInfo);
    }
    // else do nothing because the bucket already exists.
    return bucketInfoMap.get(key);
  }

  public BucketInfo addInsert(String partitionPath) {
    // for new inserts, compute buckets depending on how many records we have for each partition
    SmallFileAssign smallFileAssign = getSmallFileAssign(partitionPath);

    // first try packing this into one of the smallFiles
    if (smallFileAssign != null && smallFileAssign.assign()) {
      final String key = StreamerUtil.generateBucketKey(partitionPath, smallFileAssign.getFileId());
      // create a new bucket or reuse an existing bucket
      BucketInfo bucketInfo;
      if (bucketInfoMap.containsKey(key)) {
        // Assigns an inserts to existing update bucket
        bucketInfo = bucketInfoMap.get(key);
      } else {
        bucketInfo = addUpdate(partitionPath, smallFileAssign.getFileId());
      }
      return bucketInfo;
    }

    // if we have anything more, create new insert buckets, like normal
    return getOrCreateNewFileBucket(partitionPath, BucketType.INSERT);
  }

  public BucketInfo addAppendOnly(String partitionPath) {
    return getOrCreateNewFileBucket(partitionPath, BucketType.APPEND_ONLY);
  }

  private BucketInfo getOrCreateNewFileBucket(String partitionPath, BucketType bucketType) {
    if (newFileAssignStates.containsKey(partitionPath)) {
      NewFileAssignState newFileAssignState = newFileAssignStates.get(partitionPath);
      if (newFileAssignState.canAssign()) {
        newFileAssignState.assign();
        final String key = StreamerUtil.generateBucketKey(partitionPath, newFileAssignState.fileId);
        return bucketInfoMap.get(key);
      }
    }
    BucketInfo bucketInfo = new BucketInfo(bucketType, FSUtils.createNewFileIdPfx(), partitionPath);
    final String key = StreamerUtil.generateBucketKey(partitionPath, bucketInfo.getFileIdPrefix());
    bucketInfoMap.put(key, bucketInfo);
    NewFileAssignState newFileAssignState = new NewFileAssignState(bucketInfo.getFileIdPrefix(), writeProfile.getRecordsPerBucket());
    newFileAssignState.assign();
    newFileAssignStates.put(partitionPath, newFileAssignState);
    return bucketInfo;
  }

  private SmallFileAssign getSmallFileAssign(String partitionPath) {
    if (smallFileAssignMap.containsKey(partitionPath)) {
      return smallFileAssignMap.get(partitionPath);
    }
    List<SmallFile> smallFiles = smallFilesOfThisTask(writeProfile.getSmallFiles(partitionPath));
    if (smallFiles.size() > 0) {
      LOG.info("For partitionPath : " + partitionPath + " Small Files => " + smallFiles);
      SmallFileAssignState[] states = smallFiles.stream()
          .map(smallFile -> new SmallFileAssignState(config.getParquetMaxFileSize(), smallFile, writeProfile.getAvgSize()))
          .toArray(SmallFileAssignState[]::new);
      SmallFileAssign assign = new SmallFileAssign(states);
      smallFileAssignMap.put(partitionPath, assign);
      return assign;
    }
    return null;
  }

  /**
   * Refresh the table state like TableFileSystemView and HoodieTimeline.
   */
  public void reload(long checkpointId) {
    this.smallFileAssignMap.clear();
    this.writeProfile.reload(checkpointId);
  }

  public HoodieTable<?, ?, ?, ?> getTable() {
    return this.writeProfile.getTable();
  }

  private List<SmallFile> smallFilesOfThisTask(List<SmallFile> smallFiles) {
    // computes the small files to write inserts for this task.
    List<SmallFile> smallFilesOfThisTask = new ArrayList<>();
    for (int i = taskID; i < smallFiles.size(); i += numTasks) {
      smallFilesOfThisTask.add(smallFiles.get(i));
    }
    return smallFilesOfThisTask;
  }

  public void close() {
    reset();
    WriteProfiles.clean(config.getBasePath());
  }

  /**
   * Assigns the record to one of the small files under one partition.
   *
   * <p> The tool is initialized with an array of {@link SmallFileAssignState}s.
   * A pointer points to the current small file we are ready to assign,
   * if the current small file can not be assigned anymore (full assigned), the pointer
   * move to next small file.
   * <pre>
   *       |  ->
   *       V
   *   | smallFile_1 | smallFile_2 | smallFile_3 | ... | smallFile_N |
   * </pre>
   *
   * <p>If all the small files are full assigned, a flag {@code noSpace} was marked to true, and
   * we can return early for future check.
   */
  private static class SmallFileAssign {
    final SmallFileAssignState[] states;
    int assignIdx = 0;
    boolean noSpace = false;

    SmallFileAssign(SmallFileAssignState[] states) {
      this.states = states;
    }

    public boolean assign() {
      if (noSpace) {
        return false;
      }
      SmallFileAssignState state = states[assignIdx];
      while (!state.canAssign()) {
        assignIdx += 1;
        if (assignIdx >= states.length) {
          noSpace = true;
          return false;
        }
        // move to next slot if possible
        state = states[assignIdx];
      }
      state.assign();
      return true;
    }

    public String getFileId() {
      return states[assignIdx].fileId;
    }
  }

  /**
   * Candidate bucket state for small file. It records the total number of records
   * that the bucket can append and the current number of assigned records.
   */
  private static class SmallFileAssignState {
    long assigned;
    long totalUnassigned;
    final String fileId;

    SmallFileAssignState(long parquetMaxFileSize, SmallFile smallFile, long averageRecordSize) {
      this.assigned = 0;
      this.totalUnassigned = (parquetMaxFileSize - smallFile.sizeBytes) / averageRecordSize;
      this.fileId = smallFile.location.getFileId();
    }

    public boolean canAssign() {
      return this.totalUnassigned > 0 && this.totalUnassigned > this.assigned;
    }

    /**
     * Remembers to invoke {@link #canAssign()} first.
     */
    public void assign() {
      Preconditions.checkState(canAssign(),
          "Can not assign insert to small file: assigned => "
              + this.assigned + " totalUnassigned => " + this.totalUnassigned);
      this.assigned++;
    }
  }

  /**
   * Candidate bucket state for a new file. It records the total number of records
   * that the bucket can append and the current number of assigned records.
   */
  private static class NewFileAssignState {
    long assigned;
    long totalUnassigned;
    final String fileId;

    NewFileAssignState(String fileId, long insertRecordsPerBucket) {
      this.fileId = fileId;
      this.assigned = 0;
      this.totalUnassigned = insertRecordsPerBucket;
    }

    public boolean canAssign() {
      return this.totalUnassigned > 0 && this.totalUnassigned > this.assigned;
    }

    /**
     * Remembers to invoke {@link #canAssign()} first.
     */
    public void assign() {
      Preconditions.checkState(canAssign(),
          "Can not assign insert to new file: assigned => "
              + this.assigned + " totalUnassigned => " + this.totalUnassigned);
      this.assigned++;
    }
  }
}
