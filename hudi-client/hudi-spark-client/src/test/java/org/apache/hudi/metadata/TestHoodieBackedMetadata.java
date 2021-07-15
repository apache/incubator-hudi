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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieBackedMetadata extends HoodieClientTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestHoodieBackedMetadata.class);

  @TempDir
  public java.nio.file.Path tempFolder;

  private String metadataTableBasePath;

  private HoodieTableType tableType;

  public void init(HoodieTableType tableType) throws IOException {
    this.tableType = tableType;
    initPath();
    initSparkContexts("TestHoodieMetadata");
    initFileSystem();
    fs.mkdirs(new Path(basePath));
    initMetaClient(tableType);
    initTestDataGenerator();
    metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);

  }

  @AfterEach
  public void clean() throws IOException {
    cleanupResources();
  }

  /**
   * Metadata Table bootstrap scenarios.
   */
  @Test
  public void testMetadataTableBootstrap() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Metadata table should not exist until created for the first time
    assertFalse(fs.exists(new Path(metadataTableBasePath)), "Metadata table should not exist");
    assertThrows(TableNotFoundException.class, () -> HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build());

    // Metadata table is not created if disabled by config
    String firstCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, false))) {
      client.startCommitWithTime(firstCommitTime);
      client.insert(jsc.parallelize(dataGen.generateInserts(firstCommitTime, 5)), firstCommitTime);
      assertFalse(fs.exists(new Path(metadataTableBasePath)), "Metadata table should not be created");
      assertThrows(TableNotFoundException.class, () -> HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build());
    }

    // Metadata table should not be created if any non-complete instants are present
    String secondCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(false, true), true)) {
      client.startCommitWithTime(secondCommitTime);
      client.insert(jsc.parallelize(dataGen.generateUpdates(secondCommitTime, 2)), secondCommitTime);
      // AutoCommit is false so no bootstrap
      client.syncTableMetadata();
      assertFalse(fs.exists(new Path(metadataTableBasePath)), "Metadata table should not be created");
      assertThrows(TableNotFoundException.class, () -> HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build());
      // rollback this commit
      client.rollback(secondCommitTime);
    }

    // Metadata table created when enabled by config & sync is called
    secondCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true), true)) {
      client.startCommitWithTime(secondCommitTime);
      client.insert(jsc.parallelize(dataGen.generateUpdates(secondCommitTime, 2)), secondCommitTime);
      client.syncTableMetadata();
      assertTrue(fs.exists(new Path(metadataTableBasePath)));
      validateMetadata(client);
    }

    // Delete all existing instants on dataset to simulate archiving. This should trigger a re-bootstrap of the metadata
    // table as last synched instant has been "archived".
    final String metadataTableMetaPath = metadataTableBasePath + Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME;
    assertTrue(fs.exists(new Path(metadataTableMetaPath, HoodieTimeline.makeDeltaFileName(secondCommitTime))));

    Arrays.stream(fs.listStatus(new Path(metaClient.getMetaPath()))).filter(status -> status.getPath().getName().matches("^\\d+\\..*"))
        .forEach(status -> {
          try {
            fs.delete(status.getPath(), false);
          } catch (IOException e) {
            LOG.warn("Error when deleting instant " + status + ": " + e);
          }
        });

    String thirdCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true), true)) {
      client.startCommitWithTime(thirdCommitTime);
      client.insert(jsc.parallelize(dataGen.generateUpdates(thirdCommitTime, 2)), thirdCommitTime);
      client.syncTableMetadata();
      assertTrue(fs.exists(new Path(metadataTableBasePath)));
      validateMetadata(client);

      // Metadata Table should not have previous delta-commits as it was re-bootstrapped
      assertFalse(fs.exists(new Path(metadataTableMetaPath, HoodieTimeline.makeDeltaFileName(firstCommitTime))));
      assertFalse(fs.exists(new Path(metadataTableMetaPath, HoodieTimeline.makeDeltaFileName(secondCommitTime))));
      assertTrue(fs.exists(new Path(metadataTableMetaPath, HoodieTimeline.makeDeltaFileName(thirdCommitTime))));
    }
  }

  /**
   * Only valid partition directories are added to the metadata.
   */
  @Test
  public void testOnlyValidPartitionsAdded() throws Exception {
    // This test requires local file system
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Create an empty directory which is not a partition directory (lacks partition metadata)
    final String nonPartitionDirectory = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-nonpartition";
    Files.createDirectories(Paths.get(basePath, nonPartitionDirectory));

    // Three directories which are partitions but will be ignored due to filter
    final String filterDirRegex = ".*-filterDir\\d|\\..*";
    final String filteredDirectoryOne = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-filterDir1";
    final String filteredDirectoryTwo = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0] + "-filterDir2";
    final String filteredDirectoryThree = ".backups";

    // Create some commits
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    testTable.withPartitionMetaFiles("p1", "p2", filteredDirectoryOne, filteredDirectoryTwo, filteredDirectoryThree)
        .addCommit("001").withBaseFilesInPartition("p1", 10).withBaseFilesInPartition("p2", 10, 10)
        .addCommit("002").withBaseFilesInPartition("p1", 10).withBaseFilesInPartition("p2", 10, 10, 10);

    final HoodieWriteConfig writeConfig =
            getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.NEVER, true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withDirectoryFilterRegex(filterDirRegex).build()).build();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      client.startCommitWithTime("005");
      client.insert(jsc.emptyRDD(), "005");

      List<String> partitions = metadataWriter(client).metadata().getAllPartitionPaths();
      assertFalse(partitions.contains(nonPartitionDirectory),
          "Must not contain the non-partition " + nonPartitionDirectory);
      assertTrue(partitions.contains("p1"), "Must contain partition p1");
      assertTrue(partitions.contains("p2"), "Must contain partition p2");

      assertFalse(partitions.contains(filteredDirectoryOne),
          "Must not contain the filtered directory " + filteredDirectoryOne);
      assertFalse(partitions.contains(filteredDirectoryTwo),
          "Must not contain the filtered directory " + filteredDirectoryTwo);
      assertFalse(partitions.contains(filteredDirectoryThree),
          "Must not contain the filtered directory " + filteredDirectoryThree);

      FileStatus[] statuses = metadata(client).getAllFilesInPartition(new Path(basePath, "p1"));
      assertTrue(statuses.length == 2);
      statuses = metadata(client).getAllFilesInPartition(new Path(basePath, "p2"));
      assertTrue(statuses.length == 5);
    }
  }

  /**
   * Test various table operations sync to Metadata Table correctly.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testTableOperations(HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {

      // Write 1 (Bulk insert)
      String newCommitTime = "001";
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Write 2 (inserts)
      newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);
      validateMetadata(client);

      records = dataGen.generateInserts(newCommitTime, 20);
      writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Write 3 (updates)
      newCommitTime = "003";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Write 4 (updates and inserts)
      newCommitTime = "004";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = "005";
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        validateMetadata(client);
      }

      // Write 5 (updates and inserts)
      newCommitTime = "006";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 5);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = "007";
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        validateMetadata(client);
      }

      // Deletes
      newCommitTime = "008";
      records = dataGen.generateDeletes(newCommitTime, 10);
      JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(records, 1).map(r -> r.getKey());
      client.startCommitWithTime(newCommitTime);
      client.delete(deleteKeys, newCommitTime);
      validateMetadata(client);

      // Clean
      newCommitTime = "009";
      client.clean(newCommitTime);
      validateMetadata(client);

      // Restore
      client.restoreToInstant("006");
      validateMetadata(client);
    }
  }

  /**
   * Test rollback of various table operations sync to Metadata Table correctly.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testRollbackOperations(HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      // Write 1 (Bulk insert)
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Write 2 (inserts) + Rollback of inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 20);
      writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
      validateMetadata(client);

      // Write 3 (updates) + Rollback of updates
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 20);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
      validateMetadata(client);

      // Rollback of updates and inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
      validateMetadata(client);

      // Rollback of Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        validateMetadata(client);
      }

      // Rollback of Deletes
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateDeletes(newCommitTime, 10);
      JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(records, 1).map(r -> r.getKey());
      client.startCommitWithTime(newCommitTime);
      writeStatuses = client.delete(deleteKeys, newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
      validateMetadata(client);

      // Rollback of Clean
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.clean(newCommitTime);
      validateMetadata(client);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
      validateMetadata(client);
    }

    // Rollback of partial commits
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext,
        getWriteConfigBuilder(false, true, false).withRollbackUsingMarkers(false).build())) {
      // Write updates and inserts
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
      validateMetadata(client);
    }

    // Marker based rollback of partial commits
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext,
        getWriteConfigBuilder(false, true, false).withRollbackUsingMarkers(true).build())) {
      // Write updates and inserts
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.rollback(newCommitTime);
      client.syncTableMetadata();
      validateMetadata(client);
    }
  }

  /**
   * Test when syncing rollback to metadata if the commit being rolled back has not been synced that essentially a no-op occurs to metadata.
   * Once explicit sync is called, metadata should match.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testRollbackUnsyncedCommit(HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      // Initialize table with metadata
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);
    }

    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, false))) {
      // Commit with metadata disabled
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      client.rollback(newCommitTime);
    }

    try (SparkRDDWriteClient client = new SparkRDDWriteClient<>(engineContext, getWriteConfig(true, true))) {
      assertFalse(metadata(client).isInSync());
      client.syncTableMetadata();
      validateMetadata(client);
    }

    // If an unsynced commit is automatically rolled back during next commit, the rollback commit gets a timestamp
    // greater than than the new commit which is started. Ensure that in this case the rollback is not processed
    // as the earlier failed commit would not have been committed.
    //
    //  Dataset:   C1        C2         C3.inflight[failed]   C4   R5[rolls back C3]
    //  Metadata:  C1.delta  C2.delta
    //
    // When R5 completes, C3.xxx will be deleted. When C4 completes, C4 and R5 will be committed to Metadata Table in
    // that order. R5 should be neglected as C3 was never committed to metadata table.
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(false, false), true)) {
      // Metadata disabled and no auto-commit
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      // Not committed so left in inflight state
      client.syncTableMetadata();
      assertTrue(metadata(client).isInSync());
      validateMetadata(client);
    }

    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient<>(engineContext, getWriteConfig(true, true), true)) {
      // Metadata enabled
      // The previous commit will be rolled back automatically
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      assertTrue(metadata(client).isInSync());
      validateMetadata(client);
    }

    // In this scenario an async operations is started and completes around the same time of the failed commit.
    // Rest of the reasoning is same as above test.
    //  C4.clean was an asynchronous clean started along with C3. The clean completed but C3 commit failed.
    //
    //  Dataset:   C1        C2         C3.inflight[failed]  C4.clean     C5   R6[rolls back C3]
    //  Metadata:  C1.delta  C2.delta
    //
    // When R6 completes, C3.xxx will be deleted. When C5 completes, C4, C5 and R6 will be committed to Metadata Table
    // in that order. R6 should be neglected as C3 was never committed to metadata table.
    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(false, false), true)) {
      // Metadata disabled and no auto-commit
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      // Not committed so left in inflight state
      client.clean();
      client.syncTableMetadata();
      assertTrue(metadata(client).isInSync());
      validateMetadata(client);
    }

    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient<>(engineContext, getWriteConfig(true, true), true)) {
      // Metadata enabled
      // The previous commit will be rolled back automatically
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      assertTrue(metadata(client).isInSync());
      validateMetadata(client);
    }
  }

  /**
   * Test that manual rollbacks work correctly and enough timeline history is maintained on the metadata table
   * timeline.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testManualRollbacks(HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Setting to archive more aggressively on the Metadata Table than the Dataset
    final int maxDeltaCommitsBeforeCompaction = 4;
    HoodieWriteConfig config = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .archiveCommitsWith(2, 3).retainCommits(1)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(4, 5)
            .retainCommits(1).retainFileVersions(1).withAutoClean(true).withAsyncClean(true).build())
        .build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, config)) {
      // Initialize table with metadata
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      // Perform multiple commits
      for (int i = 1; i < 10; ++i) {
        newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        if (i == 1) {
          records = dataGen.generateInserts(newCommitTime, 5);
        } else {
          records = dataGen.generateUpdates(newCommitTime, 2);
        }
        client.startCommitWithTime(newCommitTime);
        writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        assertNoWriteErrors(writeStatuses);
      }

      // Rollback all commits except last one.
      // Note: If we rollback all instants then the timeline will be empty and FileGroups dont work correctly.
      metaClient.reloadActiveTimeline();
      int numCommits = metaClient.getCommitsAndCompactionTimeline().countInstants();
      metaClient.getCommitsAndCompactionTimeline().getReverseOrderedInstants().limit(numCommits - 1)
          .forEach(instantToRollback -> {
            client.rollback(instantToRollback.getTimestamp());
          });

      client.syncTableMetadata();
      validateMetadata(client);
    }
  }

  /**
   * Test sync of table operations.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  @Disabled
  public void testSync(HoodieTableType tableType) throws Exception {
    init(tableType);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    String newCommitTime;
    List<HoodieRecord> records;
    List<WriteStatus> writeStatuses;

    // Initial commits without metadata table enabled
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, false))) {
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateInserts(newCommitTime, 5);
      client.startCommitWithTime(newCommitTime);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateInserts(newCommitTime, 5);
      client.startCommitWithTime(newCommitTime);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
    }

    // Enable metadata table so it initialized by listing from file system
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      // inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 5);
      writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      validateMetadata(client);
      assertTrue(metadata(client).isInSync());
    }

    // Various table operations without metadata table enabled
    String restoreToInstant;
    String inflightActionTimestamp;
    String beforeInflightActionTimestamp;
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, false))) {
      // updates
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 5);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      assertTrue(metadata(client).isInSync());

      // updates and inserts
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      assertTrue(metadata(client).isInSync());

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
        assertTrue(metadata(client).isInSync());
      }

      // Savepoint
      restoreToInstant = newCommitTime;
      if (metaClient.getTableType() == HoodieTableType.COPY_ON_WRITE) {
        client.savepoint("hoodie", "metadata test");
        assertTrue(metadata(client).isInSync());
      }

      // Record a timestamp for creating an inflight instance for sync testing
      inflightActionTimestamp = HoodieActiveTimeline.createNewInstantTime();
      beforeInflightActionTimestamp = newCommitTime;

      // Deletes
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      records = dataGen.generateDeletes(newCommitTime, 5);
      JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(records, 1).map(r -> r.getKey());
      client.startCommitWithTime(newCommitTime);
      client.delete(deleteKeys, newCommitTime);
      assertTrue(metadata(client).isInSync());

      // Clean
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.clean(newCommitTime);
      assertTrue(metadata(client).isInSync());

      // updates
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 10);
      writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      assertTrue(metadata(client).isInSync());

      // insert overwrite to test replacecommit
      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
      records = dataGen.generateInserts(newCommitTime, 5);
      HoodieWriteResult replaceResult = client.insertOverwrite(jsc.parallelize(records, 1), newCommitTime);
      writeStatuses = replaceResult.getWriteStatuses().collect();
      assertNoWriteErrors(writeStatuses);
      assertTrue(metadata(client).isInSync());
    }

    // If there is an incomplete operation, the Metadata Table is not updated beyond that operations but the
    // in-memory merge should consider all the completed operations.
    Path inflightCleanPath = new Path(metaClient.getMetaPath(), HoodieTimeline.makeInflightCleanerFileName(inflightActionTimestamp));
    fs.create(inflightCleanPath).close();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      // Restore cannot be done until the metadata table is in sync. See HUDI-1502 for details
      client.syncTableMetadata();

      // Table should sync only before the inflightActionTimestamp
      HoodieBackedTableMetadataWriter writer =
          (HoodieBackedTableMetadataWriter)SparkHoodieBackedTableMetadataWriter.create(hadoopConf, client.getConfig(), context);
      assertEquals(writer.getLatestSyncedInstantTime().get(), beforeInflightActionTimestamp);

      // Reader should sync to all the completed instants
      HoodieTableMetadata metadata  = HoodieTableMetadata.create(context, client.getConfig().getMetadataConfig(),
          client.getConfig().getBasePath(), FileSystemViewStorageConfig.FILESYSTEM_VIEW_SPILLABLE_DIR.defaultValue());
      assertEquals(metadata.getSyncedInstantTimeForReader().get(), newCommitTime);

      // Remove the inflight instance holding back table sync
      fs.delete(inflightCleanPath, false);
      client.syncTableMetadata();

      writer =
          (HoodieBackedTableMetadataWriter)SparkHoodieBackedTableMetadataWriter.create(hadoopConf, client.getConfig(), context);
      assertEquals(writer.getLatestSyncedInstantTime().get(), newCommitTime);

      // Reader should sync to all the completed instants
      metadata  = HoodieTableMetadata.create(context, client.getConfig().getMetadataConfig(),
          client.getConfig().getBasePath(), FileSystemViewStorageConfig.FILESYSTEM_VIEW_SPILLABLE_DIR.defaultValue());
      assertEquals(metadata.getSyncedInstantTime().get(), newCommitTime);
    }

    // Enable metadata table and ensure it is synced
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      client.restoreToInstant(restoreToInstant);
      assertFalse(metadata(client).isInSync());

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      client.syncTableMetadata();

      validateMetadata(client);
      assertTrue(metadata(client).isInSync());
    }
  }

  /**
   * Instants on Metadata Table should be archived as per config. Metadata Table should be automatically compacted as per config.
   */
  @Test
  public void testCleaningArchivingAndCompaction() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    final int maxDeltaCommitsBeforeCompaction = 4;
    HoodieWriteConfig config = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .archiveCommitsWith(6, 8).retainCommits(1)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction).build())
        // don't archive the data timeline at all.
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(Integer.MAX_VALUE - 1, Integer.MAX_VALUE)
            .retainCommits(1).retainFileVersions(1).withAutoClean(true).withAsyncClean(true).build())
        .build();

    List<HoodieRecord> records;
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, config)) {
      for (int i = 1; i < 10; ++i) {
        String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        if (i == 1) {
          records = dataGen.generateInserts(newCommitTime, 5);
        } else {
          records = dataGen.generateUpdates(newCommitTime, 2);
        }
        client.startCommitWithTime(newCommitTime);
        List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        assertNoWriteErrors(writeStatuses);
        validateMetadata(client);
      }
    }

    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build();
    HoodieTableMetaClient datasetMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(config.getBasePath()).build();
    HoodieActiveTimeline metadataTimeline = metadataMetaClient.getActiveTimeline();
    // check that there are compactions.
    assertTrue(metadataTimeline.getCommitTimeline().filterCompletedInstants().countInstants() > 0);
    // check that cleaning has, once after each compaction.
    assertTrue(metadataTimeline.getCleanerTimeline().filterCompletedInstants().countInstants() > 0);
    // ensure archiving has happened
    long numDataCompletedInstants = datasetMetaClient.getActiveTimeline().filterCompletedInstants().countInstants();
    long numDeltaCommits = metadataTimeline.getDeltaCommitTimeline().filterCompletedInstants().countInstants();
    assertTrue(numDeltaCommits < numDataCompletedInstants, "Must have less delta commits than total completed instants on data timeline.");
  }

  /**
   * Instants on Metadata Table cannot be archived if there are still within the dataset timeline.
   * This is a requirement to correctly handle manual rollbacks that can be performed on the dataset.
   */
  @Test
  public void testMetadataArchivingLimit() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Setting to archive more aggressively on the Metadata Table than the Dataset
    final int maxDeltaCommitsBeforeCompaction = 4;
    HoodieWriteConfig config = getWriteConfigBuilder(true, true, false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true)
            .archiveCommitsWith(2, 3).retainCommits(1)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(4, 5)
            .retainCommits(1).retainFileVersions(1).withAutoClean(true).withAsyncClean(true).build())
        .build();

    List<HoodieRecord> records;
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, config)) {
      for (int i = 1; i < 10; ++i) {
        String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
        if (i == 1) {
          records = dataGen.generateInserts(newCommitTime, 5);
        } else {
          records = dataGen.generateUpdates(newCommitTime, 2);
        }
        client.startCommitWithTime(newCommitTime);
        List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        assertNoWriteErrors(writeStatuses);
      }
    }

    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build();
    HoodieTableMetaClient datasetMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(config.getBasePath()).build();
    HoodieActiveTimeline metadataTimeline = metadataMetaClient.getActiveTimeline();
    HoodieActiveTimeline dataTimeline = datasetMetaClient.getActiveTimeline();
    HoodieInstant metadataFirstInstant = metadataTimeline.getInstants().findFirst().get();
    HoodieInstant datasetFirstInstant = dataTimeline.getInstants().findFirst().get();

    assertTrue(HoodieTimeline.compareTimestamps(metadataFirstInstant.getTimestamp(), HoodieTimeline.LESSER_THAN_OR_EQUALS,
        datasetFirstInstant.getTimestamp()));
  }

  /**
   * Test various error scenarios.
   */
  @Test
  public void testErrorCases() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // TESTCASE: If commit on the metadata table succeeds but fails on the dataset, then on next init the metadata table
    // should be rolled back to last valid commit.
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true), true)) {
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 10);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateInserts(newCommitTime, 5);
      writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      // There is no way to simulate failed commit on the main dataset, hence we simply delete the completed
      // instant so that only the inflight is left over.
      String commitInstantFileName = HoodieTimeline.makeCommitFileName(newCommitTime);
      assertTrue(fs.delete(new Path(basePath + Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME,
          commitInstantFileName), false));
    }

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true), true)) {
      String newCommitTime = client.startCommit();
      // Next insert
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 5);
      List<WriteStatus> writeStatuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);

      // Post rollback commit and metadata should be valid
      validateMetadata(client);
    }
  }

  /**
   * Test non-partitioned datasets.
   */
  //@Test
  public void testNonPartitioned() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    HoodieTestDataGenerator nonPartitionedGenerator = new HoodieTestDataGenerator(new String[] {""});
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      // Write 1 (Bulk insert)
      String newCommitTime = "001";
      List<HoodieRecord> records = nonPartitionedGenerator.generateInserts(newCommitTime, 10);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
      validateMetadata(client);

      List<String> metadataPartitions = metadata(client).getAllPartitionPaths();
      assertTrue(metadataPartitions.contains(""), "Must contain empty partition");
    }
  }

  /**
   * Test various metrics published by metadata table.
   */
  @Test
  public void testMetadataMetrics() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfigBuilder(true, true, true).build())) {
      // Write
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> writeStatuses = client.insert(jsc.parallelize(records, 1), newCommitTime).collect();
      assertNoWriteErrors(writeStatuses);
      validateMetadata(client);

      Registry metricsRegistry = Registry.getRegistry("HoodieMetadata");
      assertTrue(metricsRegistry.getAllCounts().containsKey(HoodieMetadataMetrics.INITIALIZE_STR + ".count"));
      assertTrue(metricsRegistry.getAllCounts().containsKey(HoodieMetadataMetrics.INITIALIZE_STR + ".totalDuration"));
      assertTrue(metricsRegistry.getAllCounts().get(HoodieMetadataMetrics.INITIALIZE_STR + ".count") >= 1L);
      assertTrue(metricsRegistry.getAllCounts().containsKey("basefile.size"));
      assertTrue(metricsRegistry.getAllCounts().containsKey("logfile.size"));
      assertTrue(metricsRegistry.getAllCounts().containsKey("basefile.count"));
      assertTrue(metricsRegistry.getAllCounts().containsKey("logfile.count"));
    }
  }

  /**
   * Test when reading from metadata table which is out of sync with dataset that results are still consistent.
   */
  @Test
  public void testMetadataOutOfSync() throws Exception {
    init(HoodieTableType.COPY_ON_WRITE);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    SparkRDDWriteClient unsyncedClient = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true));

    // Enable metadata so table is initialized
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, true))) {
      // Perform Bulk Insert
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.bulkInsert(jsc.parallelize(records, 1), newCommitTime).collect();
    }

    // Perform commit operations with metadata disabled
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, false))) {
      // Perform Insert
      String newCommitTime = "002";
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 20);
      client.insert(jsc.parallelize(records, 1), newCommitTime).collect();

      // Perform Upsert
      newCommitTime = "003";
      client.startCommitWithTime(newCommitTime);
      records = dataGen.generateUniqueUpdates(newCommitTime, 20);
      client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();

      // Compaction
      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = "004";
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
      }
    }

    assertFalse(metadata(unsyncedClient).isInSync());
    validateMetadata(unsyncedClient);

    // Perform clean operation with metadata disabled
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, false))) {
      // One more commit needed to trigger clean so upsert and compact
      String newCommitTime = "005";
      client.startCommitWithTime(newCommitTime);
      List<HoodieRecord> records = dataGen.generateUpdates(newCommitTime, 20);
      client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();

      if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ) {
        newCommitTime = "006";
        client.scheduleCompactionAtInstant(newCommitTime, Option.empty());
        client.compact(newCommitTime);
      }

      // Clean
      newCommitTime = "007";
      client.clean(newCommitTime);
    }

    assertFalse(metadata(unsyncedClient).isInSync());
    validateMetadata(unsyncedClient);

    // Perform restore with metadata disabled
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, getWriteConfig(true, false))) {
      client.restoreToInstant("004");
    }

    assertFalse(metadata(unsyncedClient).isInSync());
    validateMetadata(unsyncedClient);
  }


  /**
   * Validate the metadata tables contents to ensure it matches what is on the file system.
   */
  private void validateMetadata(SparkRDDWriteClient testClient) throws IOException {
    HoodieWriteConfig config = testClient.getConfig();

    SparkRDDWriteClient client;
    if (config.isEmbeddedTimelineServerEnabled()) {
      testClient.close();
      client = new SparkRDDWriteClient(testClient.getEngineContext(), testClient.getConfig());
    } else {
      client = testClient;
    }

    HoodieTableMetadata tableMetadata = metadata(client);
    assertNotNull(tableMetadata, "MetadataReader should have been initialized");
    if (!config.useFileListingMetadata()) {
      return;
    }

    HoodieTimer timer = new HoodieTimer().startTimer();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);

    // Partitions should match
    FileSystemBackedTableMetadata fsBackedTableMetadata = new FileSystemBackedTableMetadata(engineContext,
        new SerializableConfiguration(hadoopConf), config.getBasePath(), config.shouldAssumeDatePartitioning());
    List<String> fsPartitions = fsBackedTableMetadata.getAllPartitionPaths();
    List<String> metadataPartitions = tableMetadata.getAllPartitionPaths();

    Collections.sort(fsPartitions);
    Collections.sort(metadataPartitions);

    assertEquals(fsPartitions.size(), metadataPartitions.size(), "Partitions should match");
    assertTrue(fsPartitions.equals(metadataPartitions), "Partitions should match");

    // Files within each partition should match
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(config, engineContext);
    TableFileSystemView tableView = table.getHoodieView();
    fsPartitions.forEach(partition -> {
      try {
        Path partitionPath;
        if (partition.equals("")) {
          // Should be the non-partitioned case
          partitionPath = new Path(basePath);
        } else {
          partitionPath = new Path(basePath, partition);
        }
        FileStatus[] fsStatuses = FSUtils.getAllDataFilesInPartition(fs, partitionPath);
        FileStatus[] metaStatuses = tableMetadata.getAllFilesInPartition(partitionPath);
        List<String> fsFileNames = Arrays.stream(fsStatuses)
            .map(s -> s.getPath().getName()).collect(Collectors.toList());
        List<String> metadataFilenames = Arrays.stream(metaStatuses)
            .map(s -> s.getPath().getName()).collect(Collectors.toList());
        Collections.sort(fsFileNames);
        Collections.sort(metadataFilenames);

        // File sizes should be valid
        Arrays.stream(metaStatuses).forEach(s -> assertTrue(s.getLen() > 0));

        if ((fsFileNames.size() != metadataFilenames.size()) || (!fsFileNames.equals(metadataFilenames))) {
          LOG.info("*** File system listing = " + Arrays.toString(fsFileNames.toArray()));
          LOG.info("*** Metadata listing = " + Arrays.toString(metadataFilenames.toArray()));

          for (String fileName : fsFileNames) {
            if (!metadataFilenames.contains(fileName)) {
              LOG.error(partition + "FsFilename " + fileName + " not found in Meta data");
            }
          }
          for (String fileName : metadataFilenames) {
            if (!fsFileNames.contains(fileName)) {
              LOG.error(partition + "Metadata file " + fileName + " not found in original FS");
            }
          }
        }

        // Block sizes should be valid
        Arrays.stream(metaStatuses).forEach(s -> assertTrue(s.getBlockSize() > 0));
        List<Long> fsBlockSizes = Arrays.stream(fsStatuses).map(FileStatus::getBlockSize).collect(Collectors.toList());
        Collections.sort(fsBlockSizes);
        List<Long> metadataBlockSizes = Arrays.stream(metaStatuses).map(FileStatus::getBlockSize).collect(Collectors.toList());
        Collections.sort(metadataBlockSizes);
        assertEquals(fsBlockSizes, metadataBlockSizes);

        assertEquals(fsFileNames.size(), metadataFilenames.size(), "Files within partition " + partition + " should match");
        assertTrue(fsFileNames.equals(metadataFilenames), "Files within partition " + partition + " should match");

        // FileSystemView should expose the same data
        List<HoodieFileGroup> fileGroups = tableView.getAllFileGroups(partition).collect(Collectors.toList());
        fileGroups.addAll(tableView.getAllReplacedFileGroups(partition).collect(Collectors.toList()));

        fileGroups.forEach(g -> LogManager.getLogger(TestHoodieBackedMetadata.class).info(g));
        fileGroups.forEach(g -> g.getAllBaseFiles().forEach(b -> LogManager.getLogger(TestHoodieBackedMetadata.class).info(b)));
        fileGroups.forEach(g -> g.getAllFileSlices().forEach(s -> LogManager.getLogger(TestHoodieBackedMetadata.class).info(s)));

        long numFiles = fileGroups.stream()
            .mapToLong(g -> g.getAllBaseFiles().count() + g.getAllFileSlices().mapToLong(s -> s.getLogFiles().count()).sum())
            .sum();
        assertEquals(metadataFilenames.size(), numFiles);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        assertTrue(false, "Exception should not be raised: " + e);
      }
    });

    HoodieBackedTableMetadataWriter metadataWriter = metadataWriter(client);
    assertNotNull(metadataWriter, "MetadataWriter should have been initialized");

    // Validate write config for metadata table
    HoodieWriteConfig metadataWriteConfig = metadataWriter.getWriteConfig();
    assertFalse(metadataWriteConfig.useFileListingMetadata(), "No metadata table for metadata table");
    assertFalse(metadataWriteConfig.getFileListingMetadataVerify(), "No verify for metadata table");

    // Metadata table should be in sync with the dataset
    assertTrue(metadata(client).isInSync());
    HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(metadataTableBasePath).build();

    // Metadata table is MOR
    assertEquals(metadataMetaClient.getTableType(), HoodieTableType.MERGE_ON_READ, "Metadata Table should be MOR");

    // Metadata table is HFile format
    assertEquals(metadataMetaClient.getTableConfig().getBaseFileFormat(), HoodieFileFormat.HFILE,
        "Metadata Table base file format should be HFile");

    // Metadata table has a fixed number of partitions
    // Cannot use FSUtils.getAllFoldersWithPartitionMetaFile for this as that function filters all directory
    // in the .hoodie folder.
    List<String> metadataTablePartitions = FSUtils.getAllPartitionPaths(engineContext, HoodieTableMetadata.getMetadataTableBasePath(basePath),
        false, false, false);
    assertEquals(MetadataPartitionType.values().length, metadataTablePartitions.size());

    // Metadata table should automatically compact and clean
    // versions are +1 as autoclean / compaction happens end of commits
    int numFileVersions = metadataWriteConfig.getCleanerFileVersionsRetained() + 1;
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metadataMetaClient, metadataMetaClient.getActiveTimeline());
    metadataTablePartitions.forEach(partition -> {
      List<FileSlice> latestSlices = fsView.getLatestFileSlices(partition).collect(Collectors.toList());
      assertTrue(latestSlices.stream().map(FileSlice::getBaseFile).count() <= 1, "Should have a single latest base file");
      assertTrue(latestSlices.size() <= 1, "Should have a single latest file slice");
      assertTrue(latestSlices.size() <= numFileVersions, "Should limit file slice to "
          + numFileVersions + " but was " + latestSlices.size());
    });

    LOG.info("Validation time=" + timer.endTimer());
  }

  private HoodieBackedTableMetadataWriter metadataWriter(SparkRDDWriteClient client) {
    return (HoodieBackedTableMetadataWriter) SparkHoodieBackedTableMetadataWriter
        .create(hadoopConf, client.getConfig(), new HoodieSparkEngineContext(jsc));
  }

  private HoodieTableMetadata metadata(SparkRDDWriteClient client) {
    HoodieWriteConfig clientConfig = client.getConfig();
    return HoodieTableMetadata.create(client.getEngineContext(), clientConfig.getMetadataConfig(), clientConfig.getBasePath(),
        clientConfig.getSpillableMapBasePath());
  }

  // TODO: this can be moved to TestHarness after merge from master
  private void assertNoWriteErrors(List<WriteStatus> statuses) {
    // Verify there are no errors
    for (WriteStatus status : statuses) {
      assertFalse(status.hasErrors(), "Errors found in write of " + status.getFileId());
    }
  }

  private HoodieWriteConfig getWriteConfig(boolean autoCommit, boolean useFileListingMetadata) {
    return getWriteConfigBuilder(autoCommit, useFileListingMetadata, false).build();
  }

  private HoodieWriteConfig.Builder getWriteConfigBuilder(boolean autoCommit, boolean useFileListingMetadata, boolean enableMetrics) {
    return getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, autoCommit, useFileListingMetadata, enableMetrics);
  }

  private HoodieWriteConfig.Builder getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy policy, boolean autoCommit, boolean useFileListingMetadata, boolean enableMetrics) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withDeleteParallelism(2).withRollbackParallelism(2).withFinalizeWriteParallelism(2)
        .withAutoCommit(autoCommit)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1)
            .withFailedWritesCleaningPolicy(policy)
            .withAutoClean(false).retainCommits(1).retainFileVersions(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true).forTable("test-trip-table")
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(useFileListingMetadata)
            .enableMetrics(enableMetrics).build())
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().on(enableMetrics)
            .withExecutorMetrics(true).usePrefix("unit-test").build());
  }

  @Override
  protected HoodieTableType getTableType() {
    return tableType;
  }
}
