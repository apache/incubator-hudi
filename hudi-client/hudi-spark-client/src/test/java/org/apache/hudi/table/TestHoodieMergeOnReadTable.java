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

package org.apache.hudi.table;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.Transformations;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.HoodieHFileInputFormat;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieHFileRealtimeInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.table.action.deltacommit.AbstractSparkDeltaCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.SparkDeleteDeltaCommitActionExecutor;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMergeOnReadTable extends HoodieClientTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestHoodieMergeOnReadTable.class);

  private JobConf roSnapshotJobConf;
  private JobConf roJobConf;
  private JobConf rtJobConf;

  @TempDir
  public java.nio.file.Path tempFolder;

  private HoodieFileFormat baseFileFormat;

  public void init(HoodieFileFormat baseFileFormat) throws IOException {
    this.baseFileFormat = baseFileFormat;
    initDFS();
    initSparkContexts("TestHoodieMergeOnReadTable");
    hadoopConf.addResource(dfs.getConf());
    jsc.hadoopConfiguration().addResource(dfs.getConf());
    context = new HoodieSparkEngineContext(jsc);
    initPath();
    dfs.mkdirs(new Path(basePath));
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ, baseFileFormat);
    initTestDataGenerator();

    roSnapshotJobConf = new JobConf(hadoopConf);
    roJobConf = new JobConf(hadoopConf);
    rtJobConf = new JobConf(hadoopConf);
  }

  @BeforeEach
  public void init() throws IOException {
    init(HoodieTableConfig.HOODIE_BASE_FILE_FORMAT_PROP.defaultValue());
  }

  @AfterEach
  public void clean() throws IOException {
    cleanupResources();
  }

  @Test
  public void testSimpleInsertAndUpdate() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      insertRecords(records, client, cfg, newCommitTime);

      /**
       * Write 2 (updates)
       */
      newCommitTime = "004";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 100);
      updateRecords(records, client, cfg, newCommitTime);

      String compactionCommitTime = client.scheduleCompaction(Option.empty()).get().toString();
      client.compact(compactionCommitTime);

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);
      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent());

      // verify that there is a commit
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTimeline timeline = metaClient.getCommitTimeline().filterCompletedInstants();
      assertEquals(1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(),
          "Expecting a single commit.");
      String latestCompactionCommitTime = timeline.lastInstant().get().getTimestamp();
      assertTrue(HoodieTimeline.compareTimestamps("000", HoodieTimeline.LESSER_THAN, latestCompactionCommitTime));

      assertEquals(200, HoodieClientTestUtils.countRecordsSince(jsc, basePath, sqlContext, timeline, "000"),
          "Must contain 200 records");
    }
  }

  @Test
  public void testSimpleInsertAndUpdateHFile() throws Exception {
    clean();
    init(HoodieFileFormat.HFILE);

    HoodieWriteConfig cfg = getConfig(true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      insertRecords(records, client, cfg, newCommitTime);

      /**
       * Write 2 (updates)
       */
      newCommitTime = "004";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 100);
      updateRecords(records, client, cfg, newCommitTime);

      String compactionCommitTime = client.scheduleCompaction(Option.empty()).get().toString();
      client.compact(compactionCommitTime);

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);
      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      HoodieTableFileSystemView roView = new HoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent());

      // verify that there is a commit
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTimeline timeline = metaClient.getCommitTimeline().filterCompletedInstants();
      assertEquals(1, timeline.findInstantsAfter("000", Integer.MAX_VALUE).countInstants(),
          "Expecting a single commit.");
      String latestCompactionCommitTime = timeline.lastInstant().get().getTimestamp();
      assertTrue(HoodieTimeline.compareTimestamps("000", HoodieTimeline.LESSER_THAN, latestCompactionCommitTime));

      assertEquals(200, HoodieClientTestUtils.countRecordsSince(jsc, basePath, sqlContext, timeline, "000"),
          "Must contain 200 records");
    }
  }

  @Test
  public void testSimpleClusteringNoUpdates() throws Exception {
    testClustering(false);
  }

  @Test
  public void testSimpleClusteringWithUpdates() throws Exception {
    testClustering(true);
  }

  private void testClustering(boolean doUpdates) throws Exception {
    // set low compaction small File Size to generate more file groups.
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1).build();
    HoodieWriteConfig cfg = getConfigBuilder(true, 10L, clusteringConfig).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 400);
      insertRecords(records.subList(0, 200), client, cfg, newCommitTime);

      /**
       * Write 2 (more inserts to create new files)
       */
      // we already set small file size to small number to force inserts to go into new file.
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);
      insertRecords(records.subList(200, 400), client, cfg, newCommitTime);

      if (doUpdates) {
        /**
         * Write 3 (updates)
         */
        newCommitTime = "003";
        client.startCommitWithTime(newCommitTime);
        records = dataGen.generateUpdates(newCommitTime, 100);
        updateRecords(records, client, cfg, newCommitTime);
      }

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);
      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      // expect 2 base files for each partition
      assertEquals(dataGen.getPartitionPaths().length * 2, allFiles.length);

      String clusteringCommitTime = client.scheduleClustering(Option.empty()).get().toString();
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);
      // verify all files are included in clustering plan.
      assertEquals(allFiles.length, hoodieTable.getFileSystemView().getFileGroupsInPendingClustering().map(Pair::getLeft).count());

      // Do the clustering and validate
      client.cluster(clusteringCommitTime, true);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      final HoodieTable clusteredTable = HoodieSparkTable.create(cfg, context, metaClient);
      Stream<HoodieBaseFile> dataFilesToRead = Arrays.stream(dataGen.getPartitionPaths())
          .flatMap(p -> clusteredTable.getBaseFileOnlyView().getLatestBaseFiles(p));
      // verify there should be only one base file per partition after clustering.
      assertEquals(dataGen.getPartitionPaths().length, dataFilesToRead.count());

      HoodieTimeline timeline = metaClient.getCommitTimeline().filterCompletedInstants();
      assertEquals(1, timeline.findInstantsAfter("003", Integer.MAX_VALUE).countInstants(),
          "Expecting a single commit.");
      assertEquals(clusteringCommitTime, timeline.lastInstant().get().getTimestamp());
      assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, timeline.lastInstant().get().getAction());
      assertEquals(400, HoodieClientTestUtils.countRecordsSince(jsc, basePath, sqlContext, timeline, "000"),
          "Must contain 200 records");
    }
  }

  // test incremental read does not go past compaction instant for RO views
  // For RT views, incremental read can go past compaction
  @Test
  public void testIncrementalReadsWithCompaction() throws Exception {
    String partitionPath = "2020/02/20"; // use only one partition for this test
    dataGen = new HoodieTestDataGenerator(new String[] { partitionPath });
    HoodieWriteConfig cfg = getConfig(true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts)
       */
      String commitTime1 = "001";
      client.startCommitWithTime(commitTime1);

      List<HoodieRecord> records001 = dataGen.generateInserts(commitTime1, 200);
      insertRecords(records001, client, cfg, commitTime1);

      // verify only one base file shows up with commit time 001
      FileStatus[] snapshotROFiles = getROSnapshotFiles(partitionPath);
      validateFiles(partitionPath, 1, snapshotROFiles, false, roSnapshotJobConf, 200, commitTime1);

      FileStatus[] incrementalROFiles = getROIncrementalFiles(partitionPath, true);
      validateFiles(partitionPath, 1, incrementalROFiles, false, roJobConf, 200, commitTime1);
      Path firstFilePath = incrementalROFiles[0].getPath();

      FileStatus[] incrementalRTFiles = getRTIncrementalFiles(partitionPath);
      validateFiles(partitionPath, 1, incrementalRTFiles, true, rtJobConf,200, commitTime1);

      assertEquals(firstFilePath, incrementalRTFiles[0].getPath());

      /**
       * Write 2 (updates)
       */
      String updateTime = "004";
      client.startCommitWithTime(updateTime);
      List<HoodieRecord> records004 = dataGen.generateUpdates(updateTime, 100);
      updateRecords(records004, client, cfg, updateTime);

      // verify RO incremental reads - only one base file shows up because updates to into log files
      incrementalROFiles = getROIncrementalFiles(partitionPath, false);
      validateFiles(partitionPath, 1, incrementalROFiles, false, roJobConf, 200, commitTime1);
      assertEquals(firstFilePath, incrementalROFiles[0].getPath());

      // verify RT incremental reads includes updates also
      incrementalRTFiles = getRTIncrementalFiles(partitionPath);
      validateFiles(partitionPath, 1, incrementalRTFiles, true, rtJobConf, 200, commitTime1, updateTime);

      // request compaction, but do not perform compaction
      String compactionCommitTime = "005";
      client.scheduleCompactionAtInstant("005", Option.empty());

      // verify RO incremental reads - only one base file shows up because updates go into log files
      incrementalROFiles = getROIncrementalFiles(partitionPath, true);
      validateFiles(partitionPath,1, incrementalROFiles, false, roJobConf, 200, commitTime1);

      // verify RT incremental reads includes updates also
      incrementalRTFiles = getRTIncrementalFiles(partitionPath);
      validateFiles(partitionPath, 1, incrementalRTFiles, true, rtJobConf, 200, commitTime1, updateTime);

      // write 3 - more inserts
      String insertsTime = "006";
      List<HoodieRecord> records006 = dataGen.generateInserts(insertsTime, 200);
      client.startCommitWithTime(insertsTime);
      insertRecords(records006, client, cfg, insertsTime);

      // verify new write shows up in snapshot mode even though there is pending compaction
      snapshotROFiles = getROSnapshotFiles(partitionPath);
      validateFiles(partitionPath, 2, snapshotROFiles, false, roSnapshotJobConf,400, commitTime1, insertsTime);

      incrementalROFiles = getROIncrementalFiles(partitionPath, true);
      assertEquals(firstFilePath, incrementalROFiles[0].getPath());
      // verify 006 does not show up in RO mode because of pending compaction

      validateFiles(partitionPath, 1, incrementalROFiles, false, roJobConf, 200, commitTime1);

      // verify that if stopAtCompaction is disabled, inserts from "insertsTime" show up
      incrementalROFiles = getROIncrementalFiles(partitionPath, false);
      validateFiles(partitionPath,2, incrementalROFiles, false, roJobConf, 400, commitTime1, insertsTime);

      // verify 006 shows up in RT views
      incrementalRTFiles = getRTIncrementalFiles(partitionPath);
      validateFiles(partitionPath, 2, incrementalRTFiles, true, rtJobConf, 400, commitTime1, updateTime, insertsTime);

      // perform the scheduled compaction
      client.compact(compactionCommitTime);

      // verify new write shows up in snapshot mode after compaction is complete
      snapshotROFiles = getROSnapshotFiles(partitionPath);
      validateFiles(partitionPath,2, snapshotROFiles, false, roSnapshotJobConf,400, commitTime1, compactionCommitTime,
          insertsTime);

      incrementalROFiles = getROIncrementalFiles(partitionPath, "002", -1, true);
      assertTrue(incrementalROFiles.length == 2);
      // verify 006 shows up because of pending compaction
      validateFiles(partitionPath, 2, incrementalROFiles, false, roJobConf, 400, commitTime1, compactionCommitTime,
                    insertsTime);
    }
  }

  // Check if record level metadata is aggregated properly at the end of write.
  @Test
  public void testMetadataAggregateFromWriteStatus() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false).withWriteStatusClass(MetadataMergeWriteStatus.class).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      String newCommitTime = "001";
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      client.startCommitWithTime(newCommitTime);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);
      Map<String, String> allWriteStatusMergedMetadataMap =
          MetadataMergeWriteStatus.mergeMetadataForWriteStatuses(statuses);
      assertTrue(allWriteStatusMergedMetadataMap.containsKey("InputRecordCount_1506582000"));
      // For metadata key InputRecordCount_1506582000, value is 2 for each record. So sum of this
      // should be 2 * records.size()
      assertEquals(String.valueOf(2 * records.size()),
          allWriteStatusMergedMetadataMap.get("InputRecordCount_1506582000"));
    }
  }

  @Test
  public void testSimpleInsertUpdateAndDelete() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts, written as base file)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      metaClient = getHoodieMetaClient(hadoopConf, cfg.getBasePath());
      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = tableView.getLatestBaseFiles();
      assertFalse(dataFilesToRead.findAny().isPresent());

      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent(),
          "should list the base files we wrote in the delta commit");

      /**
       * Write 2 (only updates, written to .log file)
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);
      writeRecords = jsc.parallelize(records, 1);
      statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      /**
       * Write 2 (only deletes, written to .log file)
       */
      newCommitTime = "004";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletesFromExistingRecords(records);

      statuses = client.upsert(jsc.parallelize(fewRecordsForDelete, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("004", deltaCommit.get().getTimestamp(), "Latest Delta commit should be 004");

      commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      allFiles = listAllBaseFilesInPath(hoodieTable);
      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent());

      List<String> dataFiles = tableView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
      List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf, dataFiles, basePath);
      // Wrote 20 records and deleted 20 records, so remaining 20-20 = 0
      assertEquals(0, recordsRead.size(), "Must contain 0 records");
    }
  }

  private void testCOWToMORConvertedTableRollback(Boolean rollbackUsingMarkers) throws Exception {
    // Set TableType to COW
    HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);

    HoodieWriteConfig cfg = getConfig(false, rollbackUsingMarkers);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      // verify there are no errors
      assertNoWriteErrors(statuses);
      client.commit(newCommitTime, jsc.parallelize(statuses));

      metaClient = getHoodieMetaClient(hadoopConf, cfg.getBasePath());
      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertTrue(commit.isPresent());
      assertEquals("001", commit.get().getTimestamp(), "commit should be 001");

      /**
       * Write 2 (updates)
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);

      statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      // Set TableType to MOR
      HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);

      // rollback a COW commit when TableType is MOR
      client.rollback(newCommitTime);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);
      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);

      final String absentCommit = newCommitTime;
      assertFalse(tableView.getLatestBaseFiles().anyMatch(file -> absentCommit.equals(file.getCommitTime())));
    }
  }

  @Test
  public void testCOWToMORConvertedTableRollbackUsingFileList() throws Exception {
    testCOWToMORConvertedTableRollback(false);
  }

  @Test
  public void testCOWToMORConvertedTableRollbackUsingMarkers() throws Exception {
    testCOWToMORConvertedTableRollback(true);
  }

  private void testRollbackWithDeltaAndCompactionCommit(Boolean rollbackUsingMarkers) throws Exception {
    HoodieWriteConfig cfg = getConfig(false, rollbackUsingMarkers);
    LOG.warn("\n\nStarting a test case ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      // Test delta commit rollback
      /**
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD);
      List<WriteStatus> statuses = writeStatusJavaRDD.collect();
      assertNoWriteErrors(statuses);

      metaClient = getHoodieMetaClient(hadoopConf, cfg.getBasePath());
      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      LOG.warn("Deltacommit 1 complete and avaialble " + deltaCommit.get().toString());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      tableView =
          getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(!dataFilesToRead.findAny().isPresent());

      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent(),
          "should list the base files we wrote in the delta commit");
      List<HoodieBaseFile> baseFiles = tableView.getLatestBaseFiles().collect(Collectors.toList());
      for (HoodieBaseFile baseFile: baseFiles) {
        LOG.warn("Base files in first detla commit. commit time " + baseFile.getCommitTime() + ", fileid  "
            + baseFile.getFileId() + ", filename " + baseFile.getFileName());
      }

      /**
       * Write 2 (inserts + updates - testing failed delta commit)
       */
      final String commitTime1 = "002";
      // WriteClient with custom config (disable small file handling)
      try (SparkRDDWriteClient secondClient = getHoodieWriteClient(getHoodieWriteConfigWithSmallFileHandlingOff());) {
        secondClient.startCommitWithTime(commitTime1);

        List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
        copyOfRecords = dataGen.generateUpdates(commitTime1, copyOfRecords);
        copyOfRecords.addAll(dataGen.generateInserts(commitTime1, 200));

        List<String> dataFiles = tableView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
        List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf, dataFiles,
            basePath);
        assertEquals(200, recordsRead.size());

        statuses = secondClient.upsert(jsc.parallelize(copyOfRecords, 1), commitTime1).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // added extra
        metaClient = getHoodieMetaClient(hadoopConf, cfg.getBasePath());
        hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);

        deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
        assertTrue(deltaCommit.isPresent());
        LOG.warn("Deltacommit 2 complete and avaialble " + deltaCommit.get().toString());
        assertEquals("002", deltaCommit.get().getTimestamp(), "Delta commit should be 002");

        commit = metaClient.getActiveTimeline().getCommitTimeline().lastInstant();
        assertFalse(commit.isPresent());

        allFiles = listAllBaseFilesInPath(hoodieTable);

        tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
        dataFilesToRead = tableView.getLatestBaseFiles();
        assertTrue(dataFilesToRead.findAny().isPresent(),
            "should list the base files we wrote in the delta commit 2");
        baseFiles = tableView.getLatestBaseFiles().collect(Collectors.toList());
        LOG.warn("Printing all base files just before rollback of DC2");
        for (HoodieBaseFile baseFile: baseFiles) {
          LOG.warn("Base files after second detla commit. commit time " + baseFile.getCommitTime() + ", fileid  "
              + baseFile.getFileId() + ", filename " + baseFile.getFileName());
        }
        LOG.warn("\n\nRolling back commit 2 " + commitTime1);

        // Test failed delta commit rollback
        secondClient.rollback(commitTime1);
        allFiles = listAllBaseFilesInPath(hoodieTable);
        LOG.warn("Printing all base files after rollback of DC2");
        for (HoodieBaseFile baseFile: baseFiles) {
          LOG.warn("Base files after second detla commit and rollback. commit time " + baseFile.getCommitTime() + ", fileid  "
              + baseFile.getFileId() + ", filename " + baseFile.getFileName());
        }
        // After rollback, there should be no base file with the failed commit time
        List<String> remainingFiles = Arrays.stream(allFiles).filter(file -> file.getPath().getName()
            .contains(commitTime1)).map(fileStatus -> fileStatus.getPath().toString()).collect(Collectors.toList());
        assertEquals(0, remainingFiles.size(), "There files should have been rolled-back "
            + "when rolling back commit " + commitTime1 + " but are still remaining. Files: " + remainingFiles);
        dataFiles = tableView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
        recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf, dataFiles, basePath);
        assertEquals(200, recordsRead.size());
      }

      /**
       * Write 3 (inserts + updates - testing successful delta commit)
       */
      final String commitTime2 = "002";
      try (SparkRDDWriteClient thirdClient = getHoodieWriteClient(cfg);) {
        thirdClient.startCommitWithTime(commitTime2);

        List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
        copyOfRecords = dataGen.generateUpdates(commitTime2, copyOfRecords);
        copyOfRecords.addAll(dataGen.generateInserts(commitTime2, 200));

        List<String> dataFiles = tableView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
        List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf, dataFiles,
            basePath);
        assertEquals(200, recordsRead.size());

        writeRecords = jsc.parallelize(copyOfRecords, 1);
        writeStatusJavaRDD = thirdClient.upsert(writeRecords, commitTime2);
        statuses = writeStatusJavaRDD.collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // adding extra
        metaClient = getHoodieMetaClient(hadoopConf, cfg.getBasePath());
        hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);

        deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
        assertTrue(deltaCommit.isPresent());
        LOG.warn("Deltacommit 3 complete and avaialble " + deltaCommit.get().toString());
        assertEquals("002", deltaCommit.get().getTimestamp(), "Delta commit should be 002");

        commit = metaClient.getActiveTimeline().getCommitTimeline().lastInstant();
        assertFalse(commit.isPresent());

        allFiles = listAllBaseFilesInPath(hoodieTable);

        tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
        dataFilesToRead = tableView.getLatestBaseFiles();
        assertTrue(dataFilesToRead.findAny().isPresent(),
            "should list the base files we wrote in the delta commit 2");
        baseFiles = tableView.getLatestBaseFiles().collect(Collectors.toList());
        LOG.warn("Printing all base files just before rollback of DC3");
        for (HoodieBaseFile baseFile: baseFiles) {
          LOG.warn("Base files after third detla commit. commit time " + baseFile.getCommitTime() + ", fileid  "
              + baseFile.getFileId() + ", filename " + baseFile.getFileName());
        }
        LOG.warn("\n\nRolling back commit 3 " + commitTime1);

        // Test successful delta commit rollback
        thirdClient.rollback(commitTime2);
        allFiles = listAllBaseFilesInPath(hoodieTable);
        LOG.warn("Printing all base files after rollback of DC3");
        for (HoodieBaseFile baseFile: baseFiles) {
          LOG.warn("Base files after second detla commit. commit time " + baseFile.getCommitTime() + ", fileid  "
              + baseFile.getFileId() + ", filename " + baseFile.getFileName());
        }

        // After rollback, there should be no base file with the failed commit time
        assertEquals(0, Arrays.stream(allFiles)
            .filter(file -> file.getPath().getName().contains(commitTime2)).count());

        metaClient = HoodieTableMetaClient.reload(metaClient);
        hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);
        tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
        dataFiles = tableView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
        for (HoodieBaseFile baseFile: baseFiles) {
          LOG.warn("Base files after third detla commit. commit time " + baseFile.getCommitTime() + ", fileid  "
              + baseFile.getFileId() + ", filename " + baseFile.getFileName());
        }
        recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf, dataFiles, basePath);
        // check that the number of records read is still correct after rollback operation
        assertEquals(200, recordsRead.size());

        // Test compaction commit rollback
        /**
         * Write 4 (updates)
         */
        newCommitTime = "003";
        thirdClient.startCommitWithTime(newCommitTime);

        writeStatusJavaRDD = thirdClient.upsert(writeRecords, newCommitTime);
        thirdClient.commit(newCommitTime, writeStatusJavaRDD);
        statuses = writeStatusJavaRDD.collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        metaClient = HoodieTableMetaClient.reload(metaClient);

        String compactionInstantTime = thirdClient.scheduleCompaction(Option.empty()).get().toString();
        thirdClient.compact(compactionInstantTime);

        allFiles = listAllBaseFilesInPath(hoodieTable);
        metaClient = HoodieTableMetaClient.reload(metaClient);
        tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline(), allFiles);

        final String compactedCommitTime = metaClient.getActiveTimeline().reload().lastInstant().get().getTimestamp();
        assertTrue(Arrays.stream(listAllBaseFilesInPath(hoodieTable))
                .anyMatch(file -> compactedCommitTime.equals(new HoodieBaseFile(file).getCommitTime())));
        thirdClient.rollbackInflightCompaction(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactedCommitTime),
                hoodieTable);
        allFiles = listAllBaseFilesInPath(hoodieTable);
        metaClient = HoodieTableMetaClient.reload(metaClient);
        tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline(), allFiles);

        assertFalse(tableView.getLatestBaseFiles().anyMatch(file -> compactedCommitTime.equals(file.getCommitTime())));
      }
    }
  }

  @RepeatedTest(100)
  public void testRollbackWithDeltaAndCompactionCommitUsingFileList() throws Exception {
    testRollbackWithDeltaAndCompactionCommit(false);
  }

  @Test
  public void testRollbackWithDeltaAndCompactionCommitUsingMarkers() throws Exception {
    testRollbackWithDeltaAndCompactionCommit(true);
  }

  @Test
  public void testMultiRollbackWithDeltaAndCompactionCommit() throws Exception {
    HoodieWriteConfig cfg = getConfig(false);
    try (final SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      /**
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD);
      List<WriteStatus> statuses = writeStatusJavaRDD.collect();
      assertNoWriteErrors(statuses);

      metaClient = getHoodieMetaClient(hadoopConf, cfg.getBasePath());
      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = tableView.getLatestBaseFiles();
      assertFalse(dataFilesToRead.findAny().isPresent());

      tableView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = tableView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent(),
          "Should list the base files we wrote in the delta commit");

      /**
       * Write 2 (inserts + updates)
       */
      newCommitTime = "002";
      // WriteClient with custom config (disable small file handling)
      SparkRDDWriteClient nClient = getHoodieWriteClient(getHoodieWriteConfigWithSmallFileHandlingOff());
      nClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> copyOfRecords = new ArrayList<>(records);
      copyOfRecords = dataGen.generateUpdates(newCommitTime, copyOfRecords);
      copyOfRecords.addAll(dataGen.generateInserts(newCommitTime, 200));

      List<String> dataFiles = tableView.getLatestBaseFiles().map(hf -> hf.getPath()).collect(Collectors.toList());
      List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf, dataFiles,
          basePath);
      assertEquals(200, recordsRead.size());

      statuses = nClient.upsert(jsc.parallelize(copyOfRecords, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);
      nClient.commit(newCommitTime, writeStatusJavaRDD);
      copyOfRecords.clear();

      // Schedule a compaction
      /**
       * Write 3 (inserts + updates)
       */
      newCommitTime = "003";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> newInserts = dataGen.generateInserts(newCommitTime, 100);
      records = dataGen.generateUpdates(newCommitTime, records);
      records.addAll(newInserts);
      writeRecords = jsc.parallelize(records, 1);

      writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD);
      statuses = writeStatusJavaRDD.collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);

      String compactionInstantTime = "004";
      client.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());

      // Compaction commit
      /**
       * Write 4 (updates)
       */
      newCommitTime = "005";
      client.startCommitWithTime(newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);
      writeRecords = jsc.parallelize(records, 1);

      writeStatusJavaRDD = client.upsert(writeRecords, newCommitTime);
      client.commit(newCommitTime, writeStatusJavaRDD);
      statuses = writeStatusJavaRDD.collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);

      compactionInstantTime = "006";
      client.scheduleCompactionAtInstant(compactionInstantTime, Option.empty());
      JavaRDD<WriteStatus> ws = (JavaRDD<WriteStatus>) client.compact(compactionInstantTime);
      client.commitCompaction(compactionInstantTime, ws, Option.empty());

      allFiles = listAllBaseFilesInPath(hoodieTable);
      metaClient = HoodieTableMetaClient.reload(metaClient);
      tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitsTimeline(), allFiles);

      final String compactedCommitTime =
          metaClient.getActiveTimeline().reload().getCommitsTimeline().lastInstant().get().getTimestamp();

      assertTrue(tableView.getLatestBaseFiles().anyMatch(file -> compactedCommitTime.equals(file.getCommitTime())));

      /**
       * Write 5 (updates)
       */
      newCommitTime = "007";
      client.startCommitWithTime(newCommitTime);
      copyOfRecords = new ArrayList<>(records);
      copyOfRecords = dataGen.generateUpdates(newCommitTime, copyOfRecords);
      copyOfRecords.addAll(dataGen.generateInserts(newCommitTime, 200));

      statuses = client.upsert(jsc.parallelize(copyOfRecords, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);
      client.commit(newCommitTime, writeStatusJavaRDD);
      copyOfRecords.clear();

      // Rollback latest commit first
      client.restoreToInstant("000");

      metaClient = HoodieTableMetaClient.reload(metaClient);
      allFiles = listAllBaseFilesInPath(hoodieTable);
      tableView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      dataFilesToRead = tableView.getLatestBaseFiles();
      assertFalse(dataFilesToRead.findAny().isPresent());
      SliceView rtView = getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      List<HoodieFileGroup> fileGroups =
          ((HoodieTableFileSystemView) rtView).getAllFileGroups().collect(Collectors.toList());
      assertTrue(fileGroups.isEmpty());

      // make sure there are no log files remaining
      assertEquals(0L, ((HoodieTableFileSystemView) rtView).getAllFileGroups()
          .filter(fileGroup -> fileGroup.getAllRawFileSlices().noneMatch(f -> f.getLogFiles().count() == 0))
          .count());

    }
  }

  protected HoodieWriteConfig getHoodieWriteConfigWithSmallFileHandlingOff() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withDeleteParallelism(2)
        .withAutoCommit(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withEmbeddedTimelineServerEnabled(true)
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024).parquetMaxFileSize(1024).build()).forTable("test-trip-table")
        .build();
  }

  @Test
  public void testUpsertPartitioner() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts, written as base file)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      metaClient = getHoodieMetaClient(hadoopConf, cfg.getBasePath());
      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      BaseFileOnlyView roView = getHoodieTableFileSystemView(metaClient,
          metaClient.getCommitsTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = roView.getLatestBaseFiles();
      Map<String, Long> fileIdToSize =
          dataFilesToRead.collect(Collectors.toMap(HoodieBaseFile::getFileId, HoodieBaseFile::getFileSize));

      roView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = roView.getLatestBaseFiles();
      List<HoodieBaseFile> dataFilesList = dataFilesToRead.collect(Collectors.toList());
      assertTrue(dataFilesList.size() > 0,
          "Should list the base files we wrote in the delta commit");

      /**
       * Write 2 (only updates + inserts, written to .log file + correction of existing base file size)
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> newRecords = dataGen.generateUpdates(newCommitTime, records);
      newRecords.addAll(dataGen.generateInserts(newCommitTime, 20));

      statuses = client.upsert(jsc.parallelize(newRecords), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("002", deltaCommit.get().getTimestamp(), "Latest Delta commit should be 002");

      commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      allFiles = listAllBaseFilesInPath(hoodieTable);
      roView = getHoodieTableFileSystemView(metaClient,
          hoodieTable.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants(), allFiles);
      dataFilesToRead = roView.getLatestBaseFiles();
      List<HoodieBaseFile> newDataFilesList = dataFilesToRead.collect(Collectors.toList());
      Map<String, Long> fileIdToNewSize =
          newDataFilesList.stream().collect(Collectors.toMap(HoodieBaseFile::getFileId, HoodieBaseFile::getFileSize));

      assertTrue(fileIdToNewSize.entrySet().stream().anyMatch(entry -> fileIdToSize.get(entry.getKey()) < entry.getValue()));

      List<String> dataFiles = roView.getLatestBaseFiles().map(HoodieBaseFile::getPath).collect(Collectors.toList());
      List<GenericRecord> recordsRead = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf, dataFiles,
          basePath);
      // Wrote 20 records in 2 batches
      assertEquals(40, recordsRead.size(), "Must contain 40 records");
    }
  }

  @Test
  public void testLogFileCountsAfterCompaction() throws Exception {
    // insert 100 records
    HoodieWriteConfig config = getConfig(true);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // Update all the 100 records
      metaClient = getHoodieMetaClient(hadoopConf, basePath);

      newCommitTime = "101";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);

      HoodieReadClient readClient = new HoodieReadClient(context, config);
      updatedRecords = readClient.tagLocation(updatedRecordsRDD).collect();

      // Write them to corresponding avro logfiles
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
      HoodieSparkWriteableTestTable.of(table, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS)
          .withLogAppends(updatedRecords);
      // In writeRecordsToLogFiles, no commit files are getting added, so resetting file-system view state
      ((SyncableFileSystemView) (table.getSliceView())).reset();

      // Verify that all data file has one log file
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> groupedLogFiles =
            table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
        for (FileSlice fileSlice : groupedLogFiles) {
          assertEquals(1, fileSlice.getLogFiles().count(), "There should be 1 log file written for every data file");
        }
      }

      // Mark 2nd delta-instant as completed
      metaClient.getActiveTimeline().createNewInstant(new HoodieInstant(State.INFLIGHT,
          HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime));
      metaClient.getActiveTimeline().saveAsComplete(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime), Option.empty());

      // Do a compaction
      String compactionInstantTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      JavaRDD<WriteStatus> result = (JavaRDD<WriteStatus>) writeClient.compact(compactionInstantTime);

      // Verify that recently written compacted data file has no log file
      metaClient = HoodieTableMetaClient.reload(metaClient);
      table = HoodieSparkTable.create(config, context, metaClient);
      HoodieActiveTimeline timeline = metaClient.getActiveTimeline();

      assertTrue(HoodieTimeline
              .compareTimestamps(timeline.lastInstant().get().getTimestamp(), HoodieTimeline.GREATER_THAN, newCommitTime),
          "Compaction commit should be > than last insert");

      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> groupedLogFiles =
            table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
        for (FileSlice slice : groupedLogFiles) {
          assertEquals(0, slice.getLogFiles().count(), "After compaction there should be no log files visible on a full view");
        }
        List<WriteStatus> writeStatuses = result.collect();
        assertTrue(writeStatuses.stream().anyMatch(writeStatus -> writeStatus.getStat().getPartitionPath().contentEquals(partitionPath)));
      }
    }
  }

  @Test
  public void testSimpleInsertsGeneratedIntoLogFiles() throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, IndexType.INMEMORY).build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
      writeClient.commit(newCommitTime, statuses);

      HoodieTable table =
          HoodieSparkTable.create(config, context, getHoodieMetaClient(hadoopConf, basePath));
      SliceView tableRTFileSystemView = table.getSliceView();

      long numLogFiles = 0;
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> allSlices = tableRTFileSystemView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
        assertEquals(0, allSlices.stream().filter(fileSlice -> fileSlice.getBaseFile().isPresent()).count());
        assertTrue(allSlices.stream().anyMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
        long logFileCount = allSlices.stream().filter(fileSlice -> fileSlice.getLogFiles().count() > 0).count();
        if (logFileCount > 0) {
          // check the log versions start from the base version
          assertTrue(allSlices.stream().map(slice -> slice.getLogFiles().findFirst().get().getLogVersion())
              .allMatch(version -> version.equals(HoodieLogFile.LOGFILE_BASE_VERSION)));
        }
        numLogFiles += logFileCount;
      }

      assertTrue(numLogFiles > 0);
      // Do a compaction
      String instantTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      statuses = (JavaRDD<WriteStatus>) writeClient.compact(instantTime);
      String extension = table.getBaseFileExtension();
      assertEquals(numLogFiles, statuses.map(status -> status.getStat().getPath().contains(extension)).count());
      assertEquals(numLogFiles, statuses.count());
      writeClient.commitCompaction(instantTime, statuses, Option.empty());
    }
  }

  private void testInsertsGeneratedIntoLogFilesRollback(Boolean rollbackUsingMarkers) throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, rollbackUsingMarkers, IndexType.INMEMORY).build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
      // trigger an action
      List<WriteStatus> writeStatuses = statuses.collect();

      // Ensure that inserts are written to only log files
      assertEquals(0,
          writeStatuses.stream().filter(writeStatus -> !writeStatus.getStat().getPath().contains("log")).count());
      assertTrue(
          writeStatuses.stream().anyMatch(writeStatus -> writeStatus.getStat().getPath().contains("log")));

      // rollback a failed commit
      boolean rollback = writeClient.rollback(newCommitTime);
      assertTrue(rollback);

      // insert 100 records
      newCommitTime = "101";
      writeClient.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 100);
      recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // Sleep for small interval (at least 1 second) to force a new rollback start time.
      Thread.sleep(1000);

      // We will test HUDI-204 here. We will simulate rollback happening twice by copying the commit file to local fs
      // and calling rollback twice
      final String lastCommitTime = newCommitTime;
      metaClient = getHoodieMetaClient(hadoopConf, basePath);

      // Save the .commit file to local directory.
      // Rollback will be called twice to test the case where rollback failed first time and retried.
      // We got the "BaseCommitTime cannot be null" exception before the fix
      Map<String, String> fileNameMap = new HashMap<>();
      for (State state : Arrays.asList(State.REQUESTED, State.INFLIGHT)) {
        HoodieInstant toCopy = new HoodieInstant(state, HoodieTimeline.DELTA_COMMIT_ACTION, lastCommitTime);
        File file = Files.createTempFile(tempFolder, null, null).toFile();
        metaClient.getFs().copyToLocalFile(new Path(metaClient.getMetaPath(), toCopy.getFileName()),
                new Path(file.getAbsolutePath()));
        fileNameMap.put(file.getAbsolutePath(), toCopy.getFileName());
      }
      Path markerDir = new Path(Files.createTempDirectory(tempFolder,null).toAbsolutePath().toString());
      if (rollbackUsingMarkers) {
        metaClient.getFs().copyToLocalFile(new Path(metaClient.getMarkerFolderPath(lastCommitTime)),
            markerDir);
      }

      writeClient.rollback(newCommitTime);
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieTable table = HoodieSparkTable.create(config, context);
      SliceView tableRTFileSystemView = table.getSliceView();

      long numLogFiles = 0;
      for (String partitionPath : dataGen.getPartitionPaths()) {
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).noneMatch(fileSlice -> fileSlice.getBaseFile().isPresent()));
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).noneMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
        numLogFiles += tableRTFileSystemView.getLatestFileSlices(partitionPath)
            .filter(fileSlice -> fileSlice.getLogFiles().count() > 0).count();
      }
      assertEquals(0, numLogFiles);
      fileNameMap.forEach((key, value) -> {
        try {
          metaClient.getFs().copyFromLocalFile(new Path(key),
                  new Path(metaClient.getMetaPath(), value));
        } catch (IOException e) {
          throw new HoodieIOException("Error copying state from local disk.", e);
        }
      });
      if (rollbackUsingMarkers) {
        metaClient.getFs().copyFromLocalFile(markerDir,
            new Path(metaClient.getMarkerFolderPath(lastCommitTime)));
      }
      Thread.sleep(1000);
      // Rollback again to pretend the first rollback failed partially. This should not error out
      writeClient.rollback(newCommitTime);
    }
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollbackUsingFileList() throws Exception {
    testInsertsGeneratedIntoLogFilesRollback(false);
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollbackUsingMarkers() throws Exception {
    testInsertsGeneratedIntoLogFilesRollback(true);
  }

  private void testInsertsGeneratedIntoLogFilesRollbackAfterCompaction(Boolean rollbackUsingMarkers) throws Exception {
    // insert 100 records
    // Setting IndexType to be InMemory to simulate Global Index nature
    HoodieWriteConfig config = getConfigBuilder(false, rollbackUsingMarkers, IndexType.INMEMORY).build();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      JavaRDD<WriteStatus> statuses = writeClient.insert(recordsRDD, newCommitTime);
      writeClient.commit(newCommitTime, statuses);
      // trigger an action
      statuses.collect();

      HoodieTable table = HoodieSparkTable.create(config, context, getHoodieMetaClient(hadoopConf, basePath));
      SliceView tableRTFileSystemView = table.getSliceView();

      long numLogFiles = 0;
      for (String partitionPath : dataGen.getPartitionPaths()) {
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).noneMatch(fileSlice -> fileSlice.getBaseFile().isPresent()));
        assertTrue(tableRTFileSystemView.getLatestFileSlices(partitionPath).anyMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
        numLogFiles += tableRTFileSystemView.getLatestFileSlices(partitionPath)
            .filter(fileSlice -> fileSlice.getLogFiles().count() > 0).count();
      }

      assertTrue(numLogFiles > 0);
      // Do a compaction
      newCommitTime = writeClient.scheduleCompaction(Option.empty()).get().toString();
      statuses = (JavaRDD<WriteStatus>) writeClient.compact(newCommitTime);
      // Ensure all log files have been compacted into base files
      String extension = table.getBaseFileExtension();
      assertEquals(numLogFiles, statuses.map(status -> status.getStat().getPath().contains(extension)).count());
      assertEquals(numLogFiles, statuses.count());
      //writeClient.commitCompaction(newCommitTime, statuses, Option.empty());
      // Trigger a rollback of compaction
      table.getActiveTimeline().reload();
      writeClient.rollbackInflightCompaction(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, newCommitTime), table);

      table = HoodieSparkTable.create(config, context, getHoodieMetaClient(hadoopConf, basePath));
      tableRTFileSystemView = table.getSliceView();
      ((SyncableFileSystemView) tableRTFileSystemView).reset();

      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> fileSlices =  getFileSystemViewWithUnCommittedSlices(getHoodieMetaClient(hadoopConf, basePath))
                .getAllFileSlices(partitionPath).filter(fs -> fs.getBaseInstantTime().equals("100")).collect(Collectors.toList());
        assertTrue(fileSlices.stream().noneMatch(fileSlice -> fileSlice.getBaseFile().isPresent()));
        assertTrue(fileSlices.stream().anyMatch(fileSlice -> fileSlice.getLogFiles().count() > 0));
      }
    }
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollbackAfterCompactionUsingFileList() throws Exception {
    testInsertsGeneratedIntoLogFilesRollbackAfterCompaction(false);
  }

  @Test
  public void testInsertsGeneratedIntoLogFilesRollbackAfterCompactionUsingMarkers() throws Exception {
    testInsertsGeneratedIntoLogFilesRollbackAfterCompaction(true);
  }

  /**
   * Test to ensure metadata stats are correctly written to metadata file.
   */
  public void testMetadataStatsOnCommit(Boolean rollbackUsingMarkers) throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false, rollbackUsingMarkers, IndexType.INMEMORY)
        .withAutoCommit(false).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      metaClient = getHoodieMetaClient(hadoopConf, basePath);
      HoodieTable table = HoodieSparkTable.create(cfg, context, metaClient);

      // Create a commit without metadata stats in metadata to test backwards compatibility
      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      String commitActionType = table.getMetaClient().getCommitActionType();
      HoodieInstant instant = new HoodieInstant(State.REQUESTED, commitActionType, "000");
      activeTimeline.createNewInstant(instant);
      activeTimeline.transitionRequestedToInflight(instant, Option.empty());
      instant = new HoodieInstant(State.INFLIGHT, commitActionType, "000");
      activeTimeline.saveAsComplete(instant, Option.empty());

      String instantTime = "001";
      client.startCommitWithTime(instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      JavaRDD<WriteStatus> statuses = client.insert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context);
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline().getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      int inserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          inserts += stat.getNumInserts();
        }
      }
      assertEquals(200, inserts);

      instantTime = "002";
      client.startCommitWithTime(instantTime);
      records = dataGen.generateUpdates(instantTime, records);
      writeRecords = jsc.parallelize(records, 1);
      statuses = client.upsert(writeRecords, instantTime);
      //assertTrue(client.commit(instantTime, statuses), "Commit should succeed");
      inserts = 0;
      int upserts = 0;
      List<WriteStatus> writeStatusList = statuses.collect();
      for (WriteStatus ws: writeStatusList) {
        inserts += ws.getStat().getNumInserts();
        upserts += ws.getStat().getNumUpdateWrites();
      }

      // Read from commit file
      assertEquals(0, inserts);
      assertEquals(200, upserts);

      client.rollback(instantTime);

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context);
      metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      inserts = 0;
      upserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          inserts += stat.getNumInserts();
          upserts += stat.getNumUpdateWrites();
        }
      }
      assertEquals(200, inserts);
      assertEquals(0, upserts);
    }
  }

  /**
   * Test to ensure rolling stats are correctly written to metadata file.
   */
  @Test
  public void testMetadataStatsOnCommitUsingFileList() throws Exception {
    testMetadataStatsOnCommit(false);
  }

  @Test
  public void testMetadataStatsOnCommitUsingMarkers() throws Exception {
    testMetadataStatsOnCommit(true);
  }

  /**
   * Test to ensure rolling stats are correctly written to the metadata file, identifies small files and corrects them.
   */
  @Test
  public void testRollingStatsWithSmallFileHandling() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder(false, IndexType.INMEMORY).withAutoCommit(false).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      Map<String, Long> fileIdToInsertsMap = new HashMap<>();
      Map<String, Long> fileIdToUpsertsMap = new HashMap<>();

      String instantTime = "000";
      client.startCommitWithTime(instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      JavaRDD<WriteStatus> statuses = client.insert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      HoodieTable table = HoodieSparkTable.create(cfg, context);
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      int inserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          inserts += stat.getNumInserts();
          fileIdToInsertsMap.put(stat.getFileId(), stat.getNumInserts());
          fileIdToUpsertsMap.put(stat.getFileId(), stat.getNumUpdateWrites());
        }
      }
      assertEquals(200, inserts);

      instantTime = "001";
      client.startCommitWithTime(instantTime);
      // generate updates + inserts. inserts should be handled into small files
      records = dataGen.generateUpdates(instantTime, records);
      records.addAll(dataGen.generateInserts(instantTime, 200));
      writeRecords = jsc.parallelize(records, 1);
      statuses = client.upsert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context);
      metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      inserts = 0;
      int upserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          assertTrue(fileIdToInsertsMap.containsKey(stat.getFileId()));
          assertTrue(fileIdToUpsertsMap.containsKey(stat.getFileId()));
          inserts += stat.getNumInserts();
          upserts += stat.getNumUpdateWrites();
        }
      }

      assertEquals(200, inserts);
      assertEquals(200, upserts);

      // Test small file handling after compaction
      instantTime = "002";
      client.scheduleCompactionAtInstant(instantTime, Option.of(metadata.getExtraMetadata()));
      statuses = (JavaRDD<WriteStatus>) client.compact(instantTime);
      client.commitCompaction(instantTime, statuses, Option.empty());

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context);
      HoodieCommitMetadata metadata1 = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getCommitsTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);

      // Ensure that the metadata stats from the extra metadata of delta commits is copied over to the compaction commit
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        assertTrue(metadata1.getPartitionToWriteStats().containsKey(pstat.getKey()));
        assertEquals(metadata1.getPartitionToWriteStats().get(pstat.getKey()).size(),
            pstat.getValue().size());
      }

      // Write inserts + updates
      instantTime = "003";
      client.startCommitWithTime(instantTime);
      // generate updates + inserts. inserts should be handled into small files
      records = dataGen.generateUpdates(instantTime, records);
      records.addAll(dataGen.generateInserts(instantTime, 200));
      writeRecords = jsc.parallelize(records, 1);
      statuses = client.upsert(writeRecords, instantTime);
      assertTrue(client.commit(instantTime, statuses), "Commit should succeed");

      // Read from commit file
      table = HoodieSparkTable.create(cfg, context);
      metadata = HoodieCommitMetadata.fromBytes(
          table.getActiveTimeline()
              .getInstantDetails(table.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get()).get(),
          HoodieCommitMetadata.class);
      inserts = 0;
      upserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          assertTrue(fileIdToInsertsMap.containsKey(stat.getFileId()));
          inserts += stat.getNumInserts();
          upserts += stat.getNumUpdateWrites();
        }
      }

      assertEquals(200, inserts);
      assertEquals(400, upserts);
    }
  }

  /**
   * Test to validate invoking table.handleUpdate() with input records from multiple partitions will fail.
   */
  @Test
  public void testHandleUpdateWithMultiplePartitions() throws Exception {
    HoodieWriteConfig cfg = getConfig(true);
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      /**
       * Write 1 (only inserts, written as base file)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      metaClient = getHoodieMetaClient(hadoopConf, cfg.getBasePath());
      HoodieSparkMergeOnReadTable hoodieTable = (HoodieSparkMergeOnReadTable) HoodieSparkTable.create(cfg, context, metaClient);

      Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().firstInstant();
      assertTrue(deltaCommit.isPresent());
      assertEquals("001", deltaCommit.get().getTimestamp(), "Delta commit should be 001");

      Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
      assertFalse(commit.isPresent());

      FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
      BaseFileOnlyView roView =
          getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
      Stream<HoodieBaseFile> dataFilesToRead = roView.getLatestBaseFiles();
      assertFalse(dataFilesToRead.findAny().isPresent());

      roView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
      dataFilesToRead = roView.getLatestBaseFiles();
      assertTrue(dataFilesToRead.findAny().isPresent(),
          "should list the base files we wrote in the delta commit");

      /**
       * Write 2 (only updates, written to .log file)
       */
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);
      metaClient.reloadActiveTimeline();
      records = dataGen.generateUpdates(newCommitTime, records);
      writeRecords = jsc.parallelize(records, 1);
      statuses = client.upsert(writeRecords, newCommitTime).collect();
      assertNoWriteErrors(statuses);

      /**
       * Write 3 (only deletes, written to .log file)
       */
      final String newDeleteTime = "004";
      final String partitionPath = records.get(0).getPartitionPath();
      final String fileId = statuses.get(0).getFileId();
      client.startCommitWithTime(newDeleteTime);
      metaClient.reloadActiveTimeline();

      List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletesFromExistingRecords(records);
      JavaRDD<HoodieRecord> deleteRDD = jsc.parallelize(fewRecordsForDelete, 1);

      // initialize partitioner
      AbstractSparkDeltaCommitActionExecutor actionExecutor = new SparkDeleteDeltaCommitActionExecutor(context, cfg, hoodieTable,
          newDeleteTime, deleteRDD);
      actionExecutor.getUpsertPartitioner(new WorkloadProfile(buildProfile(deleteRDD)));
      final List<List<WriteStatus>> deleteStatus = jsc.parallelize(Arrays.asList(1)).map(x -> {
        return actionExecutor.handleUpdate(partitionPath, fileId, fewRecordsForDelete.iterator());
      }).map(Transformations::flatten).collect();

      // Verify there are  errors because records are from multiple partitions (but handleUpdate is invoked for
      // specific partition)
      WriteStatus status = deleteStatus.get(0).get(0);
      assertTrue(status.hasErrors());
      long numRecordsInPartition = fewRecordsForDelete.stream().filter(u ->
              u.getPartitionPath().equals(partitionPath)).count();
      assertEquals(fewRecordsForDelete.size() - numRecordsInPartition, status.getTotalErrorRecords());
    }
  }

  private HoodieWriteConfig getConfig(Boolean autoCommit) {
    return getConfigBuilder(autoCommit).build();
  }

  private HoodieWriteConfig getConfig(Boolean autoCommit, Boolean rollbackUsingMarkers) {
    return getConfigBuilder(autoCommit, rollbackUsingMarkers, IndexType.BLOOM).build();
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit) {
    return getConfigBuilder(autoCommit, IndexType.BLOOM);
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, HoodieIndex.IndexType indexType) {
    return getConfigBuilder(autoCommit, false, indexType);
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, long compactionSmallFileSize, HoodieClusteringConfig clusteringConfig) {
    return getConfigBuilder(autoCommit, false, IndexType.BLOOM, compactionSmallFileSize, clusteringConfig);
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, Boolean rollbackUsingMarkers, HoodieIndex.IndexType indexType) {
    return getConfigBuilder(autoCommit, rollbackUsingMarkers, indexType, 1024 * 1024 * 1024L, HoodieClusteringConfig.newBuilder().build());
  }

  protected HoodieWriteConfig.Builder getConfigBuilder(Boolean autoCommit, Boolean rollbackUsingMarkers, HoodieIndex.IndexType indexType,
                                                       long compactionSmallFileSize, HoodieClusteringConfig clusteringConfig) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .withDeleteParallelism(2)
        .withAutoCommit(autoCommit)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(compactionSmallFileSize)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024 * 1024).parquetMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true).forTable("test-trip-table")
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withClusteringConfig(clusteringConfig)
        .withRollbackUsingMarkers(rollbackUsingMarkers);
  }

  private void insertRecords(List<HoodieRecord> records, SparkRDDWriteClient client,
                             HoodieWriteConfig cfg, String commitTime) throws IOException {
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    List<WriteStatus> statuses = client.insert(writeRecords, commitTime).collect();
    assertNoWriteErrors(statuses);
    assertFileSizes(statuses);

    metaClient = getHoodieMetaClient(hadoopConf, cfg.getBasePath());
    HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context, metaClient);

    Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals(commitTime, deltaCommit.get().getTimestamp(), "Delta commit should be specified value");

    Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().lastInstant();
    assertFalse(commit.isPresent());

    FileStatus[] allFiles = listAllBaseFilesInPath(hoodieTable);
    BaseFileOnlyView roView =
        getHoodieTableFileSystemView(metaClient, metaClient.getCommitTimeline().filterCompletedInstants(), allFiles);
    Stream<HoodieBaseFile> dataFilesToRead = roView.getLatestBaseFiles();
    assertTrue(!dataFilesToRead.findAny().isPresent());

    roView = getHoodieTableFileSystemView(metaClient, hoodieTable.getCompletedCommitsTimeline(), allFiles);
    dataFilesToRead = roView.getLatestBaseFiles();
    assertTrue(dataFilesToRead.findAny().isPresent(),
        "should list the base files we wrote in the delta commit");
  }

  private void updateRecords(List<HoodieRecord> records, SparkRDDWriteClient client,
                             HoodieWriteConfig cfg, String commitTime) throws IOException {
    Map<HoodieKey, HoodieRecord> recordsMap = new HashMap<>();
    for (HoodieRecord rec : records) {
      if (!recordsMap.containsKey(rec.getKey())) {
        recordsMap.put(rec.getKey(), rec);
      }
    }

    List<WriteStatus> statuses = client.upsert(jsc.parallelize(records, 1), commitTime).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);
    assertFileSizes(statuses);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    Option<HoodieInstant> deltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant();
    assertTrue(deltaCommit.isPresent());
    assertEquals(commitTime, deltaCommit.get().getTimestamp(),
        "Latest Delta commit should match specified time");

    Option<HoodieInstant> commit = metaClient.getActiveTimeline().getCommitTimeline().firstInstant();
    assertFalse(commit.isPresent());
  }

  private void assertFileSizes(List<WriteStatus> statuses) throws IOException {
    for (WriteStatus status: statuses) {
      assertEquals(FSUtils.getFileSize(metaClient.getFs(), new Path(metaClient.getBasePath(), status.getStat().getPath())),
          status.getStat().getFileSizeInBytes());
    }
  }

  private FileStatus[] getROSnapshotFiles(String partitionPath)
      throws Exception {
    FileInputFormat.setInputPaths(roSnapshotJobConf, basePath + "/" + partitionPath);
    return listStatus(roSnapshotJobConf, false);
  }

  private FileStatus[] getROIncrementalFiles(String partitionPath, boolean stopAtCompaction)
      throws Exception {
    return getROIncrementalFiles(partitionPath, "000", -1, stopAtCompaction);
  }

  private FileStatus[] getROIncrementalFiles(String partitionPath, String startCommitTime, int numCommitsToPull, boolean stopAtCompaction)
          throws Exception {
    setupIncremental(roJobConf, startCommitTime, numCommitsToPull, stopAtCompaction);
    FileInputFormat.setInputPaths(roJobConf, Paths.get(basePath, partitionPath).toString());
    return listStatus(roJobConf, false);
  }

  private FileStatus[] getRTIncrementalFiles(String partitionPath)
          throws Exception {
    return getRTIncrementalFiles(partitionPath, "000", -1);
  }

  private FileStatus[] getRTIncrementalFiles(String partitionPath, String startCommitTime, int numCommitsToPull)
          throws Exception {
    setupIncremental(rtJobConf, startCommitTime, numCommitsToPull, false);
    FileInputFormat.setInputPaths(rtJobConf, Paths.get(basePath, partitionPath).toString());
    return listStatus(rtJobConf, true);
  }

  private void setupIncremental(JobConf jobConf, String startCommit, int numberOfCommitsToPull, boolean stopAtCompaction) {
    String modePropertyName =
            String.format(HoodieHiveUtils.HOODIE_CONSUME_MODE_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(modePropertyName, HoodieHiveUtils.INCREMENTAL_SCAN_MODE);

    String startCommitTimestampName =
            String.format(HoodieHiveUtils.HOODIE_START_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.set(startCommitTimestampName, startCommit);

    String maxCommitPulls =
        String.format(HoodieHiveUtils.HOODIE_MAX_COMMIT_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setInt(maxCommitPulls, numberOfCommitsToPull);

    String stopAtCompactionPropName =
        String.format(HoodieHiveUtils.HOODIE_STOP_AT_COMPACTION_PATTERN, HoodieTestUtils.RAW_TRIPS_TEST_NAME);
    jobConf.setBoolean(stopAtCompactionPropName, stopAtCompaction);
  }

  private void validateFiles(String partitionPath, int expectedNumFiles,
                             FileStatus[] files, boolean realtime, JobConf jobConf,
                             int expectedRecords, String... expectedCommits) {

    assertEquals(expectedNumFiles, files.length);
    Set<String> expectedCommitsSet = Arrays.stream(expectedCommits).collect(Collectors.toSet());
    List<GenericRecord> records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(hadoopConf,
        Collections.singletonList(Paths.get(basePath, partitionPath).toString()), basePath, jobConf, realtime);
    assertEquals(expectedRecords, records.size());
    Set<String> actualCommits = records.stream().map(r ->
            r.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString()).collect(Collectors.toSet());
    assertEquals(expectedCommitsSet, actualCommits);
  }

  private FileStatus[] listAllBaseFilesInPath(HoodieTable table) throws IOException {
    return HoodieTestTable.of(table.getMetaClient()).listAllBaseFiles(table.getBaseFileExtension());
  }

  private FileStatus[] listStatus(JobConf jobConf, boolean realtime) throws IOException {
    // This is required as Hoodie InputFormats do not extend a common base class and FileInputFormat's
    // listStatus() is protected.
    FileInputFormat inputFormat = HoodieInputFormatUtils.getInputFormat(baseFileFormat, realtime, jobConf);
    switch (baseFileFormat) {
      case PARQUET:
        if (realtime) {
          return ((HoodieParquetRealtimeInputFormat)inputFormat).listStatus(jobConf);
        } else {
          return ((HoodieParquetInputFormat)inputFormat).listStatus(jobConf);
        }
      case HFILE:
        if (realtime) {
          return ((HoodieHFileRealtimeInputFormat)inputFormat).listStatus(jobConf);
        } else {
          return ((HoodieHFileInputFormat)inputFormat).listStatus(jobConf);
        }
      default:
        throw new HoodieIOException("Hoodie InputFormat not implemented for base file format " + baseFileFormat);
    }
  }
}

