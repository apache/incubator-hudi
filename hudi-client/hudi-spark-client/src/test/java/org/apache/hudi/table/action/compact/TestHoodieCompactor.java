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

package org.apache.hudi.table.action.compact;

import java.util.Collections;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieMemoryConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bloom.SparkHoodieBloomIndex;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.FileCreateUtils.createDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedDeltaCommit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieCompactor extends HoodieClientTestHarness {

  private Configuration hadoopConf;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws Exception {
    // Initialize a local spark env
    initSparkContexts();

    // Create a temp folder as the base path
    initPath();
    hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
    fs = FSUtils.getFs(basePath, hadoopConf);
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.MERGE_ON_READ);
    initTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private HoodieWriteConfig getConfig() {
    return getConfigBuilder()
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
  }

  private HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
            .withInlineCompaction(false).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
        .withMemoryConfig(HoodieMemoryConfig.newBuilder().withMaxDFSStreamBufferSize(1 * 1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build());
  }

  @Test
  public void testCompactionOnCopyOnWriteFail() throws Exception {
    metaClient = HoodieTestUtils.init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTable table = HoodieSparkTable.create(getConfig(), context, metaClient);
    String compactionInstantTime = HoodieActiveTimeline.createNewInstantTime();
    assertThrows(HoodieNotSupportedException.class, () -> {
      table.scheduleCompaction(context, compactionInstantTime, Option.empty());
      table.compact(context, compactionInstantTime);
    });
  }

  @Test
  public void testCompactionEmpty() throws Exception {
    HoodieWriteConfig config = getConfig();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieSparkTable.create(getConfig(), context, metaClient);
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config);) {

      String newCommitTime = writeClient.startCommit();
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      String compactionInstantTime = HoodieActiveTimeline.createNewInstantTime();
      Option<HoodieCompactionPlan> plan = table.scheduleCompaction(context, compactionInstantTime, Option.empty());
      assertFalse(plan.isPresent(), "If there is nothing to compact, result will be empty");
    }
  }

  @Test
  public void testScheduleCompactionWithInflightInstant() {
    HoodieWriteConfig config = getConfig();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      // insert 100 records.
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // create one inflight instance.
      newCommitTime = "102";
      writeClient.startCommitWithTime(newCommitTime);
      metaClient.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(State.REQUESTED,
              HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime), Option.empty());

      // create one compaction instance before exist inflight instance.
      String compactionTime = "101";
      writeClient.scheduleCompactionAtInstant(compactionTime, Option.empty());
    }
  }

  @Test
  public void testWriteStatusContentsAfterCompaction() throws Exception {
    // insert 100 records
    HoodieWriteConfig config = getConfig();
    try (SparkRDDWriteClient writeClient = getHoodieWriteClient(config)) {
      String newCommitTime = "100";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
      JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);
      writeClient.insert(recordsRDD, newCommitTime).collect();

      // Update all the 100 records
      HoodieTable table = HoodieSparkTable.create(config, context);
      newCommitTime = "101";
      writeClient.startCommitWithTime(newCommitTime);

      List<HoodieRecord> updatedRecords = dataGen.generateUpdates(newCommitTime, records);
      JavaRDD<HoodieRecord> updatedRecordsRDD = jsc.parallelize(updatedRecords, 1);
      HoodieIndex index = new SparkHoodieBloomIndex<>(config);
      updatedRecords = ((JavaRDD<HoodieRecord>)index.tagLocation(updatedRecordsRDD, context, table)).collect();

      // Write them to corresponding avro logfiles. Also, set the state transition properly.
      HoodieSparkWriteableTestTable.of(table, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS)
          .withLogAppends(updatedRecords);
      metaClient.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(State.REQUESTED,
          HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime), Option.empty());
      writeClient.commit(newCommitTime, jsc.emptyRDD(), Option.empty());
      metaClient.reloadActiveTimeline();

      // Verify that all data file has one log file
      table = HoodieSparkTable.create(config, context);
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<FileSlice> groupedLogFiles =
            table.getSliceView().getLatestFileSlices(partitionPath).collect(Collectors.toList());
        for (FileSlice fileSlice : groupedLogFiles) {
          assertEquals(1, fileSlice.getLogFiles().count(), "There should be 1 log file written for every data file");
        }
      }
      createDeltaCommit(basePath, newCommitTime);
      createRequestedDeltaCommit(basePath, newCommitTime);
      createInflightDeltaCommit(basePath, newCommitTime);

      // Do a compaction
      table = HoodieSparkTable.create(config, context);
      String compactionInstantTime = "102";
      table.scheduleCompaction(context, compactionInstantTime, Option.empty());
      table.getMetaClient().reloadActiveTimeline();
      JavaRDD<WriteStatus> result = (JavaRDD<WriteStatus>) table.compact(context, compactionInstantTime).getWriteStatuses();

      // Verify that all partition paths are present in the WriteStatus result
      for (String partitionPath : dataGen.getPartitionPaths()) {
        List<WriteStatus> writeStatuses = result.collect();
        assertTrue(writeStatuses.stream()
            .filter(writeStatus -> writeStatus.getStat().getPartitionPath().contentEquals(partitionPath)).count() > 0);
      }
    }
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
