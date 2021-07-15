/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.hudi.common.testutils.FileCreateUtils.baseFileName;
import static org.apache.hudi.common.testutils.FileCreateUtils.createCleanFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightCleanFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightReplaceCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightRollbackFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createMarkerFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createReplaceCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedCleanFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedCompaction;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedReplaceCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRollbackFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.logFileName;

public class HoodieTestTable {

  protected final String basePath;
  protected final FileSystem fs;
  protected HoodieTableMetaClient metaClient;
  protected String currentInstantTime;

  protected HoodieTestTable(String basePath, FileSystem fs, HoodieTableMetaClient metaClient) {
    ValidationUtils.checkArgument(Objects.equals(basePath, metaClient.getBasePath()));
    ValidationUtils.checkArgument(Objects.equals(fs, metaClient.getRawFs()));
    this.basePath = basePath;
    this.fs = fs;
    this.metaClient = metaClient;
  }

  public static HoodieTestTable of(HoodieTableMetaClient metaClient) {
    return new HoodieTestTable(metaClient.getBasePath(), metaClient.getRawFs(), metaClient);
  }

  public static String makeNewCommitTime(int sequence) {
    return String.format("%09d", sequence);
  }

  public static String makeNewCommitTime() {
    return makeNewCommitTime(Instant.now());
  }

  public static String makeNewCommitTime(Instant dateTime) {
    return HoodieActiveTimeline.formatInstantTime(Date.from(dateTime));
  }

  public static List<String> makeIncrementalCommitTimes(int num) {
    return makeIncrementalCommitTimes(num, 1);
  }

  public static List<String> makeIncrementalCommitTimes(int num, int firstOffsetSeconds) {
    final Instant now = Instant.now();
    return IntStream.range(0, num)
        .mapToObj(i -> makeNewCommitTime(now.plus(firstOffsetSeconds + i, SECONDS)))
        .collect(Collectors.toList());
  }

  public HoodieTestTable addRequestedCommit(String instantTime) throws Exception {
    createRequestedCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addInflightCommit(String instantTime) throws Exception {
    createRequestedCommit(basePath, instantTime);
    createInflightCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addCommit(String instantTime) throws Exception {
    createRequestedCommit(basePath, instantTime);
    createInflightCommit(basePath, instantTime);
    createCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addCommit(String instantTime, HoodieCommitMetadata metadata) throws Exception {
    createRequestedCommit(basePath, instantTime);
    createInflightCommit(basePath, instantTime);
    createCommit(basePath, instantTime, metadata);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addDeltaCommit(String instantTime) throws Exception {
    createRequestedDeltaCommit(basePath, instantTime);
    createInflightDeltaCommit(basePath, instantTime);
    createDeltaCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addReplaceCommit(
      String instantTime,
      Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata,
      Option<HoodieCommitMetadata> inflightReplaceMetadata,
      HoodieReplaceCommitMetadata completeReplaceMetadata) throws Exception {
    createRequestedReplaceCommit(basePath, instantTime, requestedReplaceMetadata);
    createInflightReplaceCommit(basePath, instantTime, inflightReplaceMetadata);
    createReplaceCommit(basePath, instantTime, completeReplaceMetadata);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addRequestedReplace(String instantTime, Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata) throws Exception {
    createRequestedReplaceCommit(basePath, instantTime, requestedReplaceMetadata);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addInflightClean(String instantTime, HoodieCleanerPlan cleanerPlan) throws IOException {
    createRequestedCleanFile(basePath, instantTime, cleanerPlan);
    createInflightCleanFile(basePath, instantTime, cleanerPlan);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addClean(String instantTime, HoodieCleanerPlan cleanerPlan, HoodieCleanMetadata metadata) throws IOException {
    createRequestedCleanFile(basePath, instantTime, cleanerPlan);
    createInflightCleanFile(basePath, instantTime, cleanerPlan);
    createCleanFile(basePath, instantTime, metadata);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addInflightRollback(String instantTime) throws IOException {
    createInflightRollbackFile(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addRollback(String instantTime, HoodieRollbackMetadata rollbackMetadata) throws IOException {
    createInflightRollbackFile(basePath, instantTime);
    createRollbackFile(basePath, instantTime, rollbackMetadata);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addRequestedCompaction(String instantTime) throws IOException {
    createRequestedCompaction(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addRequestedCompaction(String instantTime, HoodieCompactionPlan compactionPlan) throws IOException {
    HoodieInstant compactionInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
    metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
        TimelineMetadataUtils.serializeCompactionPlan(compactionPlan));
    return addRequestedCompaction(instantTime);
  }

  public HoodieTestTable addRequestedCompaction(String instantTime, FileSlice... fileSlices) throws IOException {
    HoodieCompactionPlan plan = CompactionUtils
        .buildFromFileSlices(Arrays.stream(fileSlices).map(fs -> Pair.of(fs.getPartitionPath(), fs))
            .collect(Collectors.toList()), Option.empty(), Option.empty());
    return addRequestedCompaction(instantTime, plan);
  }

  public HoodieTestTable forCommit(String instantTime) {
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable forDeltaCommit(String instantTime) {
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable forReplaceCommit(String instantTime) {
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable withPartitionMetaFiles(String... partitionPaths) throws IOException {
    for (String partitionPath : partitionPaths) {
      FileCreateUtils.createPartitionMetaFile(basePath, partitionPath);
    }
    return this;
  }

  public HoodieTestTable withMarkerFile(String partitionPath, String fileId, IOType ioType) throws IOException {
    createMarkerFile(basePath, partitionPath, currentInstantTime, fileId, ioType);
    return this;
  }

  public HoodieTestTable withMarkerFiles(String partitionPath, int num, IOType ioType) throws IOException {
    String[] fileIds = IntStream.range(0, num).mapToObj(i -> UUID.randomUUID().toString()).toArray(String[]::new);
    return withMarkerFiles(partitionPath, fileIds, ioType);
  }

  public HoodieTestTable withMarkerFiles(String partitionPath, String[] fileIds, IOType ioType) throws IOException {
    for (String fileId : fileIds) {
      createMarkerFile(basePath, partitionPath, currentInstantTime, fileId, ioType);
    }
    return this;
  }

  /**
   * Insert one base file to each of the given distinct partitions.
   *
   * @return A {@link Map} of partition and its newly inserted file's id.
   */
  public Map<String, String> getFileIdsWithBaseFilesInPartitions(String... partitions) throws Exception {
    Map<String, String> partitionFileIdMap = new HashMap<>();
    for (String p : partitions) {
      String fileId = UUID.randomUUID().toString();
      FileCreateUtils.createBaseFile(basePath, p, currentInstantTime, fileId);
      partitionFileIdMap.put(p, fileId);
    }
    return partitionFileIdMap;
  }

  public HoodieTestTable withBaseFilesInPartitions(Map<String, String> partitionAndFileId) throws Exception {
    for (Map.Entry<String, String> pair : partitionAndFileId.entrySet()) {
      withBaseFilesInPartition(pair.getKey(), pair.getValue());
    }
    return this;
  }

  public HoodieTestTable withBaseFilesInPartition(String partition, String... fileIds) throws Exception {
    for (String f : fileIds) {
      FileCreateUtils.createBaseFile(basePath, partition, currentInstantTime, f);
    }
    return this;
  }

  public HoodieTestTable withBaseFilesInPartition(String partition, int... lengths) throws Exception {
    for (int l : lengths) {
      String fileId = UUID.randomUUID().toString();
      FileCreateUtils.createBaseFile(basePath, partition, currentInstantTime, fileId, l);
    }
    return this;
  }

  public String getFileIdWithLogFile(String partitionPath) throws Exception {
    String fileId = UUID.randomUUID().toString();
    withLogFile(partitionPath, fileId);
    return fileId;
  }

  public HoodieTestTable withLogFile(String partitionPath, String fileId) throws Exception {
    return withLogFile(partitionPath, fileId, 0);
  }

  public HoodieTestTable withLogFile(String partitionPath, String fileId, int... versions) throws Exception {
    for (int version : versions) {
      FileCreateUtils.createLogFile(basePath, partitionPath, currentInstantTime, fileId, version);
    }
    return this;
  }

  public boolean inflightCommitExists(String instantTime) {
    try {
      return fs.exists(getInflightCommitFilePath(instantTime));
    } catch (IOException e) {
      throw new HoodieTestTableException(e);
    }
  }

  public boolean commitExists(String instantTime) {
    try {
      return fs.exists(getCommitFilePath(instantTime));
    } catch (IOException e) {
      throw new HoodieTestTableException(e);
    }
  }

  public boolean baseFilesExist(Map<String, String> partitionAndFileId, String instantTime) {
    return partitionAndFileId.entrySet().stream().allMatch(entry -> {
      String partition = entry.getKey();
      String fileId = entry.getValue();
      return baseFileExists(partition, instantTime, fileId);
    });
  }

  public boolean baseFileExists(String partition, String instantTime, String fileId) {
    try {
      return fs.exists(new Path(Paths.get(basePath, partition, baseFileName(instantTime, fileId)).toString()));
    } catch (IOException e) {
      throw new HoodieTestTableException(e);
    }
  }

  public boolean logFilesExist(String partition, String instantTime, String fileId, int... versions) {
    return Arrays.stream(versions).allMatch(v -> logFileExists(partition, instantTime, fileId, v));
  }

  public boolean logFileExists(String partition, String instantTime, String fileId, int version) {
    try {
      return fs.exists(new Path(Paths.get(basePath, partition, logFileName(instantTime, fileId, version)).toString()));
    } catch (IOException e) {
      throw new HoodieTestTableException(e);
    }
  }

  public Path getInflightCommitFilePath(String instantTime) {
    return new Path(Paths.get(basePath, HoodieTableMetaClient.METAFOLDER_NAME, instantTime + HoodieTimeline.INFLIGHT_COMMIT_EXTENSION).toUri());
  }

  public Path getCommitFilePath(String instantTime) {
    return new Path(Paths.get(basePath, HoodieTableMetaClient.METAFOLDER_NAME, instantTime + HoodieTimeline.COMMIT_EXTENSION).toUri());
  }

  public Path getRequestedCompactionFilePath(String instantTime) {
    return new Path(Paths.get(basePath, HoodieTableMetaClient.AUXILIARYFOLDER_NAME, instantTime + HoodieTimeline.REQUESTED_COMPACTION_EXTENSION).toUri());
  }

  public Path getPartitionPath(String partition) {
    return new Path(Paths.get(basePath, partition).toUri());
  }

  public Path getBaseFilePath(String partition, String fileId) {
    return new Path(Paths.get(basePath, partition, getBaseFileNameById(fileId)).toUri());
  }

  public String getBaseFileNameById(String fileId) {
    return baseFileName(currentInstantTime, fileId);
  }

  public FileStatus[] listAllBaseFiles() throws IOException {
    return listAllBaseFiles(HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP.defaultValue().getFileExtension());
  }

  public FileStatus[] listAllBaseFiles(String fileExtension) throws IOException {
    return FileSystemTestUtils.listRecursive(fs, new Path(basePath)).stream()
        .filter(status -> status.getPath().getName().endsWith(fileExtension))
        .toArray(FileStatus[]::new);
  }

  public FileStatus[] listAllLogFiles() throws IOException {
    return listAllLogFiles(HoodieFileFormat.HOODIE_LOG.getFileExtension());
  }

  public FileStatus[] listAllLogFiles(String fileExtension) throws IOException {
    return FileSystemTestUtils.listRecursive(fs, new Path(basePath)).stream()
        .filter(status -> status.getPath().getName().contains(fileExtension))
        .toArray(FileStatus[]::new);
  }

  public FileStatus[] listAllBaseAndLogFiles() throws IOException {
    return Stream.concat(Stream.of(listAllBaseFiles()), Stream.of(listAllLogFiles())).toArray(FileStatus[]::new);
  }

  public FileStatus[] listAllFilesInPartition(String partitionPath) throws IOException {
    return FileSystemTestUtils.listRecursive(fs, new Path(Paths.get(basePath, partitionPath).toString())).toArray(new FileStatus[0]);
  }

  public FileStatus[] listAllFilesInTempFolder() throws IOException {
    return FileSystemTestUtils.listRecursive(fs, new Path(Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME).toString())).toArray(new FileStatus[0]);
  }

  public static class HoodieTestTableException extends RuntimeException {
    public HoodieTestTableException(Throwable t) {
      super(t);
    }
  }
}
