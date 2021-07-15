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

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.client.validator.SparkPreCommitValidator;
import org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator;
import org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieCorruptedDataException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.MarkerFiles;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.SparkWriteHelper;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion.VERSION_0;
import static org.apache.hudi.common.testutils.FileCreateUtils.getBaseFileCountsForPaths;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.NULL_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.Transformations.randomSelectAsHoodieKeys;
import static org.apache.hudi.common.testutils.Transformations.recordsToRecordKeySet;
import static org.apache.hudi.config.HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE_OPT_KEY;
import static org.apache.hudi.config.HoodieClusteringConfig.CLUSTERING_EXECUTION_STRATEGY_CLASS;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TestHoodieClientOnCopyOnWriteStorage extends HoodieClientTestBase {

  private static final Logger LOG = LogManager.getLogger(TestHoodieClientOnCopyOnWriteStorage.class);
  private static final Map<String, String> STRATEGY_PARAMS = new HashMap<String, String>() {
    {
      put("sortColumn", "record_key");
    }
  };

  private static Stream<Arguments> configParams() {
    return Arrays.stream(new Boolean[][] {{true},{false}}).map(Arguments::of);
  }

  private HoodieTestTable testTable;

  private static final String COUNT_SQL_QUERY_FOR_VALIDATION = "select count(*) from <TABLE_NAME>";

  @BeforeEach
  public void setUpTestTable() {
    testTable = HoodieSparkWriteableTestTable.of(metaClient);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient insert API.
   */
  @Test
  public void testAutoCommitOnInsert() throws Exception {
    testAutoCommit(SparkRDDWriteClient::insert, false);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient insertPrepped API.
   */
  @Test
  public void testAutoCommitOnInsertPrepped() throws Exception {
    testAutoCommit(SparkRDDWriteClient::insertPreppedRecords, true);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient upsert API.
   */
  @Test
  public void testAutoCommitOnUpsert() throws Exception {
    testAutoCommit(SparkRDDWriteClient::upsert, false);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient upsert Prepped API.
   */
  @Test
  public void testAutoCommitOnUpsertPrepped() throws Exception {
    testAutoCommit(SparkRDDWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert API.
   */
  @Test
  public void testAutoCommitOnBulkInsert() throws Exception {
    testAutoCommit(SparkRDDWriteClient::bulkInsert, false);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert prepped API.
   */
  @Test
  public void testAutoCommitOnBulkInsertPrepped() throws Exception {
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.bulkInsertPreppedRecords(recordRDD, instantTime,
        Option.empty()), true);
  }

  /**
   * Test auto-commit by applying write function.
   *
   * @param writeFn One of HoodieWriteClient Write API
   * @throws Exception in case of failure
   */
  private void testAutoCommit(Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
      boolean isPrepped) throws Exception {
    // Set autoCommit false
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {

      String prevCommitTime = "000";
      String newCommitTime = "001";
      int numRecords = 200;
      JavaRDD<WriteStatus> result = insertFirstBatch(cfg, client, newCommitTime, prevCommitTime, numRecords, writeFn,
          isPrepped, false, numRecords);

      assertFalse(testTable.commitExists(newCommitTime),
          "If Autocommit is false, then commit should not be made automatically");
      assertTrue(client.commit(newCommitTime, result), "Commit should succeed");
      assertTrue(testTable.commitExists(newCommitTime),
          "After explicit commit, commit file should be created");
    }
  }

  @Test
  public void testPreCommitValidatorsOnInsert() throws Exception {
    int numRecords = 200;
    HoodiePreCommitValidatorConfig validatorConfig = HoodiePreCommitValidatorConfig.newBuilder()
        .withPreCommitValidator(SqlQuerySingleResultPreCommitValidator.class.getName())
        .withPrecommitValidatorSingleResultSqlQueries(COUNT_SQL_QUERY_FOR_VALIDATION + "#" + numRecords)
        .build();
    HoodieWriteConfig config = getConfigBuilder().withAutoCommit(true)
        .withPreCommitValidatorConfig(validatorConfig).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn = (writeClient, recordRDD, instantTime) ->
          writeClient.bulkInsert(recordRDD, instantTime, Option.empty());
      String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
      JavaRDD<WriteStatus> result = insertFirstBatch(config, client, newCommitTime,
          "000", numRecords, writeFn, false, false, numRecords);
      assertTrue(testTable.commitExists(newCommitTime));
    }
  }

  @Test
  public void testPreCommitValidationFailureOnInsert() throws Exception {
    int numRecords = 200;
    HoodiePreCommitValidatorConfig validatorConfig = HoodiePreCommitValidatorConfig.newBuilder()
        .withPreCommitValidator(SqlQuerySingleResultPreCommitValidator.class.getName())
        //set wrong value for expected number of rows
        .withPrecommitValidatorSingleResultSqlQueries(COUNT_SQL_QUERY_FOR_VALIDATION + "#" + 500)
        .build();
    HoodieWriteConfig config = getConfigBuilder().withPreCommitValidatorConfig(validatorConfig).build();
    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn = (writeClient, recordRDD, instantTime) ->
          writeClient.bulkInsert(recordRDD, instantTime, Option.empty());
      JavaRDD<WriteStatus> result = insertFirstBatch(config, client, newCommitTime,
          "000", numRecords, writeFn, false, false, numRecords);
      fail("Expected validation to fail because we only insert 200 rows. Validation is configured to expect 500 rows");
    } catch (HoodieInsertException e) {
      if (e.getCause() instanceof HoodieValidationException) {
        // expected because wrong value passed
      } else {
        throw e;
      }
    }

    assertFalse(testTable.commitExists(newCommitTime));
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient insert API.
   */
  @Test
  public void testDeduplicationOnInsert() throws Exception {
    testDeduplication(SparkRDDWriteClient::insert);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient bulk-insert API.
   */
  @Test
  public void testDeduplicationOnBulkInsert() throws Exception {
    testDeduplication(SparkRDDWriteClient::bulkInsert);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient upsert API.
   */
  @Test
  public void testDeduplicationOnUpsert() throws Exception {
    testDeduplication(SparkRDDWriteClient::upsert);
  }

  /**
   * Test Deduplication Logic for write function.
   *
   * @param writeFn One of HoddieWriteClient non-prepped write APIs
   * @throws Exception in case of failure
   */
  private void testDeduplication(
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn) throws Exception {
    String newCommitTime = "001";

    String recordKey = UUID.randomUUID().toString();
    HoodieKey keyOne = new HoodieKey(recordKey, "2018-01-01");
    HoodieRecord<RawTripTestPayload> recordOne =
        new HoodieRecord(keyOne, dataGen.generateRandomValue(keyOne, newCommitTime));

    HoodieKey keyTwo = new HoodieKey(recordKey, "2018-02-01");
    HoodieRecord recordTwo =
        new HoodieRecord(keyTwo, dataGen.generateRandomValue(keyTwo, newCommitTime));

    // Same key and partition as keyTwo
    HoodieRecord recordThree =
        new HoodieRecord(keyTwo, dataGen.generateRandomValue(keyTwo, newCommitTime));

    JavaRDD<HoodieRecord<RawTripTestPayload>> records =
        jsc.parallelize(Arrays.asList(recordOne, recordTwo, recordThree), 1);

    // Global dedup should be done based on recordKey only
    HoodieIndex index = mock(HoodieIndex.class);
    when(index.isGlobal()).thenReturn(true);
    List<HoodieRecord<RawTripTestPayload>> dedupedRecs = SparkWriteHelper.newInstance().deduplicateRecords(records, index, 1).collect();
    assertEquals(1, dedupedRecs.size());
    assertEquals(dedupedRecs.get(0).getPartitionPath(), recordThree.getPartitionPath());
    assertNodupesWithinPartition(dedupedRecs);

    // non-Global dedup should be done based on both recordKey and partitionPath
    index = mock(HoodieIndex.class);
    when(index.isGlobal()).thenReturn(false);
    dedupedRecs = SparkWriteHelper.newInstance().deduplicateRecords(records, index, 1).collect();
    assertEquals(2, dedupedRecs.size());
    assertNodupesWithinPartition(dedupedRecs);

    // Perform write-action and check
    JavaRDD<HoodieRecord> recordList = jsc.parallelize(Arrays.asList(recordOne, recordTwo, recordThree), 1);
    try (SparkRDDWriteClient client = getHoodieWriteClient(getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .combineInput(true, true).build());) {
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> statuses = writeFn.apply(client, recordList, newCommitTime).collect();
      assertNoWriteErrors(statuses);
      assertEquals(2, statuses.size());
      assertNodupesInPartition(statuses.stream().map(WriteStatus::getWrittenRecords).flatMap(Collection::stream)
          .collect(Collectors.toList()));
    }
  }

  /**
   * Assert that there is no duplicate key at the partition level.
   *
   * @param records List of Hoodie records
   */
  void assertNodupesInPartition(List<HoodieRecord> records) {
    Map<String, Set<String>> partitionToKeys = new HashMap<>();
    for (HoodieRecord r : records) {
      String key = r.getRecordKey();
      String partitionPath = r.getPartitionPath();
      if (!partitionToKeys.containsKey(partitionPath)) {
        partitionToKeys.put(partitionPath, new HashSet<>());
      }
      assertFalse(partitionToKeys.get(partitionPath).contains(key), "key " + key + " is duplicate within partition " + partitionPath);
      partitionToKeys.get(partitionPath).add(key);
    }
  }

  /**
   * Test Upsert API.
   */
  @Test
  public void testUpserts() throws Exception {
    testUpsertsInternal(getConfig(), SparkRDDWriteClient::upsert, false);
  }

  /**
   * Test UpsertPrepped API.
   */
  @Test
  public void testUpsertsPrepped() throws Exception {
    testUpsertsInternal(getConfig(), SparkRDDWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test one of HoodieWriteClient upsert(Prepped) APIs.
   *
   * @param config Write Config
   * @param writeFn One of Hoodie Write Function API
   * @throws Exception in case of error
   */
  private void testUpsertsInternal(HoodieWriteConfig config,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPrepped)
      throws Exception {
    // Force using older timeline layout
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY)
        .withProps(config.getProps()).withTimelineLayoutVersion(
        VERSION_0).build();
    HoodieTableMetaClient.withPropertyBuilder()
      .fromMetaClient(metaClient)
      .setTimelineLayoutVersion(VERSION_0)
      .initTable(metaClient.getHadoopConf(), metaClient.getBasePath());

    SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig);

    // Write 1 (only inserts)
    String newCommitTime = "001";
    String initCommitTime = "000";
    int numRecords = 200;
    insertFirstBatch(hoodieWriteConfig, client, newCommitTime, initCommitTime, numRecords, SparkRDDWriteClient::insert,
        isPrepped, true, numRecords);

    // Write 2 (updates)
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    numRecords = 100;
    String commitTimeBetweenPrevAndNew = "002";
    updateBatch(hoodieWriteConfig, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), initCommitTime, numRecords, writeFn, isPrepped, true,
        numRecords, 200, 2);

    // Delete 1
    prevCommitTime = newCommitTime;
    newCommitTime = "005";
    numRecords = 50;

    deleteBatch(hoodieWriteConfig, client, newCommitTime, prevCommitTime,
        initCommitTime, numRecords, SparkRDDWriteClient::delete, isPrepped, true,
        0, 150);

    // Now simulate an upgrade and perform a restore operation
    HoodieWriteConfig newConfig = getConfigBuilder().withProps(config.getProps()).withTimelineLayoutVersion(
        TimelineLayoutVersion.CURR_VERSION).build();
    client = getHoodieWriteClient(newConfig);
    client.restoreToInstant("004");

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(200, HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain " + 200 + " records");

    // Perform Delete again on upgraded dataset.
    prevCommitTime = newCommitTime;
    newCommitTime = "006";
    numRecords = 50;

    deleteBatch(newConfig, client, newCommitTime, prevCommitTime,
        initCommitTime, numRecords, SparkRDDWriteClient::delete, isPrepped, true,
        0, 150);

    HoodieActiveTimeline activeTimeline = new HoodieActiveTimeline(metaClient, false);
    List<HoodieInstant> instants = activeTimeline.getCommitTimeline().getInstants().collect(Collectors.toList());
    assertEquals(5, instants.size());
    assertEquals(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001"),
        instants.get(0));
    assertEquals(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "004"),
        instants.get(1));
    // New Format should have all states of instants
    assertEquals(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "006"),
        instants.get(2));
    assertEquals(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "006"),
        instants.get(3));
    assertEquals(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "006"),
        instants.get(4));

    final HoodieWriteConfig cfg = hoodieWriteConfig;
    final String instantTime = "007";
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build();
    String basePathStr = basePath;
    HoodieTable table = getHoodieTable(metaClient, cfg);
    String extension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    jsc.parallelize(Arrays.asList(1)).map(e -> {
      HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
          .fromBytes(metaClient.getActiveTimeline().getInstantDetails(
              metaClient.getCommitsTimeline().filterCompletedInstants().lastInstant().get()).get(),
              HoodieCommitMetadata.class);
      String filePath = commitMetadata.getPartitionToWriteStats().values().stream()
          .flatMap(w -> w.stream()).filter(s -> s.getPath().endsWith(extension)).findAny()
          .map(ee -> ee.getPath()).orElse(null);
      String partitionPath = commitMetadata.getPartitionToWriteStats().values().stream()
          .flatMap(w -> w.stream()).filter(s -> s.getPath().endsWith(extension)).findAny()
          .map(ee -> ee.getPartitionPath()).orElse(null);
      Path baseFilePath = new Path(basePathStr, filePath);
      HoodieBaseFile baseFile = new HoodieBaseFile(baseFilePath.toString());

      try {
        HoodieMergeHandle handle = new HoodieMergeHandle(cfg, instantTime, table, new HashMap<>(),
            partitionPath, FSUtils.getFileId(baseFilePath.getName()), baseFile, new SparkTaskContextSupplier());
        WriteStatus writeStatus = new WriteStatus(false, 0.0);
        writeStatus.setStat(new HoodieWriteStat());
        writeStatus.getStat().setNumWrites(0);
        handle.performMergeDataValidationCheck(writeStatus);
      } catch (HoodieCorruptedDataException e1) {
        fail("Exception not expected because merge validation check is disabled");
      }

      try {
        final String newInstantTime = "006";
        cfg.getProps().setProperty("hoodie.merge.data.validation.enabled", "true");
        HoodieWriteConfig cfg2 = HoodieWriteConfig.newBuilder().withProps(cfg.getProps()).build();
        HoodieMergeHandle handle = new HoodieMergeHandle(cfg2, newInstantTime, table, new HashMap<>(),
            partitionPath, FSUtils.getFileId(baseFilePath.getName()), baseFile, new SparkTaskContextSupplier());
        WriteStatus writeStatus = new WriteStatus(false, 0.0);
        writeStatus.setStat(new HoodieWriteStat());
        writeStatus.getStat().setNumWrites(0);
        handle.performMergeDataValidationCheck(writeStatus);
        fail("The above line should have thrown an exception");
      } catch (HoodieCorruptedDataException e2) {
        // expected
      }
      return true;
    }).collect();
  }

  /**
   * Test Insert API for HoodieConcatHandle.
   */
  @Test
  public void testInsertsWithHoodieConcatHandle() throws Exception {
    testHoodieConcatHandle(getConfig(), false);
  }

  /**
   * Test InsertPrepped API for HoodieConcatHandle.
   */
  @Test
  public void testInsertsPreppedWithHoodieConcatHandle() throws Exception {
    testHoodieConcatHandle(getConfig(),  true);
  }

  /**
   * Test one of HoodieConcatHandle w/ {@link AbstractHoodieWriteClient#insert(Object, String)} API.
   *
   * @param config Write Config
   * @throws Exception in case of error
   */
  private void testHoodieConcatHandle(HoodieWriteConfig config, boolean isPrepped)
      throws Exception {
    // Force using older timeline layout
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder()
        .withProps(config.getProps()).withMergeAllowDuplicateOnInserts(true).withTimelineLayoutVersion(
            VERSION_0).build();
    HoodieTableMetaClient.withPropertyBuilder()
      .fromMetaClient(metaClient)
      .setTimelineLayoutVersion(VERSION_0)
      .initTable(metaClient.getHadoopConf(), metaClient.getBasePath());

    SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig);

    // Write 1 (only inserts)
    String newCommitTime = "001";
    String initCommitTime = "000";
    int numRecords = 200;
    insertFirstBatch(hoodieWriteConfig, client, newCommitTime, initCommitTime, numRecords, SparkRDDWriteClient::insert,
        isPrepped, true, numRecords);

    // Write 2 (updates)
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    numRecords = 100;
    String commitTimeBetweenPrevAndNew = "002";

    final Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        generateWrapRecordsFn(isPrepped, hoodieWriteConfig, dataGen::generateUniqueUpdates);

    writeBatch(client, newCommitTime, prevCommitTime, Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), initCommitTime,
        numRecords, recordGenFunction, SparkRDDWriteClient::insert, true, numRecords, 300,
        2, false);
  }

  /**
   * Tests deletion of records.
   */
  @Test
  public void testDeletes() throws Exception {
    SparkRDDWriteClient client = getHoodieWriteClient(getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY).build());
    /**
     * Write 1 (inserts and deletes) Write actual 200 insert records and ignore 100 delete records
     */
    String initCommitTime = "000";
    String newCommitTime = "001";

    final List<HoodieRecord> recordsInFirstBatch = new ArrayList<>();
    Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        (String instantTime, Integer numRecordsInThisCommit) -> {
          List<HoodieRecord> fewRecordsForInsert = dataGen.generateInserts(instantTime, 200);
          List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletes(instantTime, 100);

          recordsInFirstBatch.addAll(fewRecordsForInsert);
          recordsInFirstBatch.addAll(fewRecordsForDelete);
          return recordsInFirstBatch;
        };
    writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime,
        // unused as genFn uses hard-coded number of inserts/updates/deletes
        -1, recordGenFunction, SparkRDDWriteClient::upsert, true, 200, 200, 1, false);

    /**
     * Write 2 (deletes+writes).
     */
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    final List<HoodieRecord> recordsInSecondBatch = new ArrayList<>();

    recordGenFunction = (String instantTime, Integer numRecordsInThisCommit) -> {
      List<HoodieRecord> fewRecordsForDelete = recordsInFirstBatch.subList(0, 50);
      List<HoodieRecord> fewRecordsForUpdate = recordsInFirstBatch.subList(50, 100);
      recordsInSecondBatch.addAll(dataGen.generateDeletesFromExistingRecords(fewRecordsForDelete));
      recordsInSecondBatch.addAll(fewRecordsForUpdate);
      return recordsInSecondBatch;
    };
    writeBatch(client, newCommitTime, prevCommitTime, Option.empty(), initCommitTime, 100, recordGenFunction,
        SparkRDDWriteClient::upsert, true, 50, 150, 2, false);
  }

  /**
   * When records getting inserted are deleted in the same write batch, hudi should have deleted those records and
   * not be available in read path.
   * @throws Exception
   */
  @Test
  public void testDeletesForInsertsInSameBatch() throws Exception {
    SparkRDDWriteClient client = getHoodieWriteClient(getConfigBuilder(HoodieFailedWritesCleaningPolicy.LAZY).build());
    /**
     * Write 200 inserts and issue deletes to a subset(50) of inserts.
     */
    String initCommitTime = "000";
    String newCommitTime = "001";

    final List<HoodieRecord> recordsInFirstBatch = new ArrayList<>();
    Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        (String instantTime, Integer numRecordsInThisCommit) -> {
          List<HoodieRecord> fewRecordsForInsert = dataGen.generateInserts(instantTime, 200);
          List<HoodieRecord> fewRecordsForDelete = fewRecordsForInsert.subList(40, 90);

          recordsInFirstBatch.addAll(fewRecordsForInsert);
          recordsInFirstBatch.addAll(dataGen.generateDeletesFromExistingRecords(fewRecordsForDelete));
          return recordsInFirstBatch;
        };

    writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime,
        -1, recordGenFunction, SparkRDDWriteClient::upsert, true, 150, 150, 1, false);
  }

  /**
   * Test update of a record to different partition with Global Index.
   */
  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"GLOBAL_BLOOM", "GLOBAL_SIMPLE"})
  public void testUpsertsUpdatePartitionPathGlobalBloom(IndexType indexType) throws Exception {
    testUpsertsUpdatePartitionPath(indexType, getConfig(), SparkRDDWriteClient::upsert);
  }

  /**
   * This test ensures in a global bloom when update partition path is set to true in config, if an incoming record has mismatched partition
   * compared to whats in storage, then appropriate actions are taken. i.e. old record is deleted in old partition and new one is inserted
   * in the new partition.
   * test structure:
   * 1. insert 1 batch
   * 2. insert 2nd batch with larger no of records so that a new file group is created for partitions
   * 3. issue upserts to records from batch 1 with different partition path. This should ensure records from batch 1 are deleted and new
   * records are upserted to the new partition
   *
   * @param indexType index type to be tested for
   * @param config instance of {@link HoodieWriteConfig} to use
   * @param writeFn write function to be used for testing
   */
  private void testUpsertsUpdatePartitionPath(IndexType indexType, HoodieWriteConfig config,
      Function3<JavaRDD<WriteStatus>, SparkRDDWriteClient, JavaRDD<HoodieRecord>, String> writeFn)
      throws Exception {
    // instantiate client

    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder()
        .withProps(config.getProps())
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder().compactionSmallFileSize(10000).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType)
            .withBloomIndexUpdatePartitionPath(true)
            .withGlobalSimpleIndexUpdatePartitionPath(true)
            .build()).withTimelineLayoutVersion(VERSION_0).build();

    HoodieTableMetaClient.withPropertyBuilder()
      .fromMetaClient(metaClient)
      .setTimelineLayoutVersion(VERSION_0)
      .initTable(metaClient.getHadoopConf(), metaClient.getBasePath());
    // Set rollback to LAZY so no inflights are deleted
    hoodieWriteConfig.getProps().put(HoodieCompactionConfig.FAILED_WRITES_CLEANER_POLICY_PROP.key(),
        HoodieFailedWritesCleaningPolicy.LAZY.name());
    SparkRDDWriteClient client = getHoodieWriteClient(hoodieWriteConfig);

    // Write 1
    String newCommitTime = "001";
    int numRecords = 10;
    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, numRecords);
    Set<Pair<String, String>> expectedPartitionPathRecKeyPairs = new HashSet<>();
    // populate expected partition path and record keys
    for (HoodieRecord rec : records) {
      expectedPartitionPathRecKeyPairs.add(Pair.of(rec.getPartitionPath(), rec.getRecordKey()));
    }
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    JavaRDD<WriteStatus> result = writeFn.apply(client, writeRecords, newCommitTime);
    result.collect();

    // Check the entire dataset has all records
    String[] fullPartitionPaths = getFullPartitionPaths();
    assertPartitionPathRecordKeys(expectedPartitionPathRecKeyPairs, fullPartitionPaths);

    // verify one basefile per partition
    String[] fullExpectedPartitionPaths = getFullPartitionPaths(expectedPartitionPathRecKeyPairs.stream().map(Pair::getLeft).toArray(String[]::new));
    Map<String, Long> baseFileCounts = getBaseFileCountsForPaths(basePath, fs, fullExpectedPartitionPaths);
    for (Map.Entry<String, Long> entry : baseFileCounts.entrySet()) {
      assertEquals(1, entry.getValue());
    }
    assertTrue(baseFileCounts.entrySet().stream().allMatch(entry -> entry.getValue() == 1));

    // Write 2
    newCommitTime = "002";
    numRecords = 20; // so that a new file id is created
    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> recordsSecondBatch = dataGen.generateInserts(newCommitTime, numRecords);
    // populate expected partition path and record keys
    for (HoodieRecord rec : recordsSecondBatch) {
      expectedPartitionPathRecKeyPairs.add(Pair.of(rec.getPartitionPath(), rec.getRecordKey()));
    }
    writeRecords = jsc.parallelize(recordsSecondBatch, 1);
    result = writeFn.apply(client, writeRecords, newCommitTime);
    result.collect();

    // Check the entire dataset has all records
    fullPartitionPaths = getFullPartitionPaths();
    assertPartitionPathRecordKeys(expectedPartitionPathRecKeyPairs, fullPartitionPaths);

    // verify that there are more than 1 basefiles per partition
    // we can't guarantee randomness in partitions where records are distributed. So, verify atleast one partition has more than 1 basefile.
    baseFileCounts = getBaseFileCountsForPaths(basePath, fs, fullPartitionPaths);
    assertTrue(baseFileCounts.entrySet().stream().filter(entry -> entry.getValue() > 1).count() >= 1,
        "At least one partition should have more than 1 base file after 2nd batch of writes");

    // Write 3 (upserts to records from batch 1 with diff partition path)
    newCommitTime = "003";

    // update to diff partition paths
    List<HoodieRecord> recordsToUpsert = new ArrayList<>();
    for (HoodieRecord rec : records) {
      // remove older entry from expected partition path record key pairs
      expectedPartitionPathRecKeyPairs
          .remove(Pair.of(rec.getPartitionPath(), rec.getRecordKey()));
      String partitionPath = rec.getPartitionPath();
      String newPartitionPath = null;
      if (partitionPath.equalsIgnoreCase(DEFAULT_FIRST_PARTITION_PATH)) {
        newPartitionPath = DEFAULT_SECOND_PARTITION_PATH;
      } else if (partitionPath.equalsIgnoreCase(DEFAULT_SECOND_PARTITION_PATH)) {
        newPartitionPath = DEFAULT_THIRD_PARTITION_PATH;
      } else if (partitionPath.equalsIgnoreCase(DEFAULT_THIRD_PARTITION_PATH)) {
        newPartitionPath = DEFAULT_FIRST_PARTITION_PATH;
      } else {
        throw new IllegalStateException("Unknown partition path " + rec.getPartitionPath());
      }
      recordsToUpsert.add(
          new HoodieRecord(new HoodieKey(rec.getRecordKey(), newPartitionPath),
              rec.getData()));
      // populate expected partition path and record keys
      expectedPartitionPathRecKeyPairs.add(Pair.of(newPartitionPath, rec.getRecordKey()));
    }

    writeRecords = jsc.parallelize(recordsToUpsert, 1);
    result = writeFn.apply(client, writeRecords, newCommitTime);
    result.collect();

    // Check the entire dataset has all records
    fullPartitionPaths = getFullPartitionPaths();
    assertPartitionPathRecordKeys(expectedPartitionPathRecKeyPairs, fullPartitionPaths);
  }

  private void assertPartitionPathRecordKeys(Set<Pair<String, String>> expectedPartitionPathRecKeyPairs, String[] fullPartitionPaths) {
    Dataset<Row> rows = getAllRows(fullPartitionPaths);
    List<Pair<String, String>> actualPartitionPathRecKeyPairs = getActualPartitionPathAndRecordKeys(rows);
    // verify all partitionpath, record key matches
    assertActualAndExpectedPartitionPathRecordKeyMatches(expectedPartitionPathRecKeyPairs, actualPartitionPathRecKeyPairs);
  }

  private List<Pair<String, String>> getActualPartitionPathAndRecordKeys(Dataset<org.apache.spark.sql.Row> rows) {
    List<Pair<String, String>> actualPartitionPathRecKeyPairs = new ArrayList<>();
    for (Row row : rows.collectAsList()) {
      actualPartitionPathRecKeyPairs
          .add(Pair.of(row.getAs("_hoodie_partition_path"), row.getAs("_row_key")));
    }
    return actualPartitionPathRecKeyPairs;
  }

  private Dataset<org.apache.spark.sql.Row> getAllRows(String[] fullPartitionPaths) {
    return HoodieClientTestUtils
        .read(jsc, basePath, sqlContext, fs, fullPartitionPaths);
  }

  private String[] getFullPartitionPaths() {
    return getFullPartitionPaths(dataGen.getPartitionPaths());
  }

  private String[] getFullPartitionPaths(String[] relativePartitionPaths) {
    String[] fullPartitionPaths = new String[relativePartitionPaths.length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, relativePartitionPaths[i]);
    }
    return fullPartitionPaths;
  }

  private void assertActualAndExpectedPartitionPathRecordKeyMatches(Set<Pair<String, String>> expectedPartitionPathRecKeyPairs,
      List<Pair<String, String>> actualPartitionPathRecKeyPairs) {
    // verify all partitionpath, record key matches
    assertEquals(expectedPartitionPathRecKeyPairs.size(), actualPartitionPathRecKeyPairs.size());
    for (Pair<String, String> entry : actualPartitionPathRecKeyPairs) {
      assertTrue(expectedPartitionPathRecKeyPairs.contains(entry));
    }

    for (Pair<String, String> entry : expectedPartitionPathRecKeyPairs) {
      assertTrue(actualPartitionPathRecKeyPairs.contains(entry));
    }
  }

  private Pair<List<WriteStatus>, List<HoodieRecord>> insertBatchRecords(SparkRDDWriteClient client, String commitTime,
                                                                         Integer recordNum, int expectStatueSize) {
    client.startCommitWithTime(commitTime);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime, recordNum);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
    List<WriteStatus> statuses = client.upsert(insertRecordsRDD1, commitTime).collect();
    assertNoWriteErrors(statuses);
    assertEquals(expectStatueSize, statuses.size(), "check expect statue size.");
    return Pair.of(statuses, inserts1);
  }

  @Test
  public void testUpdateRejectForClustering() throws IOException {
    final String testPartitionPath = "2016/09/26";
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    Properties props = new Properties();
    props.setProperty(ASYNC_CLUSTERING_ENABLE_OPT_KEY.key(), "true");
    HoodieWriteConfig config = getSmallInsertWriteConfig(100,
        TRIP_EXAMPLE_SCHEMA, dataGen.getEstimatedFileSizeInBytes(150), props);
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);

    //1. insert to generate 2 file group
    String commitTime1 = "001";
    Pair<List<WriteStatus>, List<HoodieRecord>> upsertResult = insertBatchRecords(client, commitTime1, 600, 2);
    List<HoodieRecord> inserts1 = upsertResult.getValue();
    List<String> fileGroupIds1 = table.getFileSystemView().getAllFileGroups(testPartitionPath)
        .map(fileGroup -> fileGroup.getFileGroupId().getFileId()).collect(Collectors.toList());
    assertEquals(2, fileGroupIds1.size());

    // 2. generate clustering plan for fileGroupIds1 file groups
    String commitTime2 = "002";
    List<List<FileSlice>> firstInsertFileSlicesList = table.getFileSystemView().getAllFileGroups(testPartitionPath)
        .map(fileGroup -> fileGroup.getAllFileSlices().collect(Collectors.toList())).collect(Collectors.toList());
    List<FileSlice>[] fileSlices = (List<FileSlice>[])firstInsertFileSlicesList.toArray(new List[firstInsertFileSlicesList.size()]);
    createRequestedReplaceInstant(this.metaClient, commitTime2, fileSlices);

    // 3. insert one record with no updating reject exception, and not merge the small file, just generate a new file group
    String commitTime3 = "003";
    insertBatchRecords(client, commitTime3, 1, 1).getKey();
    List<String> fileGroupIds2 = table.getFileSystemView().getAllFileGroups(testPartitionPath)
        .map(fileGroup -> fileGroup.getFileGroupId().getFileId()).collect(Collectors.toList());
    assertEquals(3, fileGroupIds2.size());

    // 4. update one record for the clustering two file groups, throw reject update exception
    String commitTime4 = "004";
    client.startCommitWithTime(commitTime4);
    List<HoodieRecord> insertsAndUpdates3 = new ArrayList<>();
    insertsAndUpdates3.addAll(dataGen.generateUpdates(commitTime4, inserts1));
    String assertMsg = String.format("Not allowed to update the clustering files in partition: %s "
        + "For pending clustering operations, we are not going to support update for now.", testPartitionPath);
    assertThrows(HoodieUpsertException.class, () -> {
      writeClient.upsert(jsc.parallelize(insertsAndUpdates3, 1), commitTime3).collect(); }, assertMsg);

    // 5. insert one record with no updating reject exception, will merge the small file
    String commitTime5 = "005";
    List<WriteStatus> statuses = insertBatchRecords(client, commitTime5, 1, 1).getKey();
    fileGroupIds2.removeAll(fileGroupIds1);
    assertEquals(fileGroupIds2.get(0), statuses.get(0).getFileId());
    List<String> firstInsertFileGroupIds4 = table.getFileSystemView().getAllFileGroups(testPartitionPath)
        .map(fileGroup -> fileGroup.getFileGroupId().getFileId()).collect(Collectors.toList());
    assertEquals(3, firstInsertFileGroupIds4.size());
  }

  /**
   * Test scenario of new file-group getting added during upsert().
   */
  @Test
  public void testSmallInsertHandlingForUpserts() throws Exception {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    BaseFileUtils fileUtils = BaseFileUtils.getInstance(metaClient);

    // Inserts => will write file1
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, insertSplitLimit); // this writes ~500kb
    Set<String> keys1 = recordsToRecordKeySet(inserts1);

    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
    List<WriteStatus> statuses = client.upsert(insertRecordsRDD1, commitTime1).collect();

    assertNoWriteErrors(statuses);

    assertEquals(1, statuses.size(), "Just 1 file needs to be added.");
    String file1 = statuses.get(0).getFileId();
    assertEquals(100,
        fileUtils.readRowKeys(hadoopConf, new Path(basePath, statuses.get(0).getStat().getPath()))
            .size(), "file should contain 100 records");

    // Update + Inserts such that they just expand file1
    String commitTime2 = "002";
    client.startCommitWithTime(commitTime2);
    List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime2, 40);
    Set<String> keys2 = recordsToRecordKeySet(inserts2);
    List<HoodieRecord> insertsAndUpdates2 = new ArrayList<>();
    insertsAndUpdates2.addAll(inserts2);
    insertsAndUpdates2.addAll(dataGen.generateUpdates(commitTime2, inserts1));

    JavaRDD<HoodieRecord> insertAndUpdatesRDD2 = jsc.parallelize(insertsAndUpdates2, 1);
    statuses = client.upsert(insertAndUpdatesRDD2, commitTime2).collect();
    assertNoWriteErrors(statuses);

    assertEquals(1, statuses.size(), "Just 1 file needs to be updated.");
    assertEquals(file1, statuses.get(0).getFileId(), "Existing file should be expanded");
    assertEquals(commitTime1, statuses.get(0).getStat().getPrevCommit(), "Existing file should be expanded");
    Path newFile = new Path(basePath, statuses.get(0).getStat().getPath());
    assertEquals(140, fileUtils.readRowKeys(hadoopConf, newFile).size(),
        "file should contain 140 records");

    List<GenericRecord> records = fileUtils.readAvroRecords(hadoopConf, newFile);
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      assertEquals(commitTime2, record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString(), "only expect commit2");
      assertTrue(keys2.contains(recordKey) || keys1.contains(recordKey), "key expected to be part of commit2");
    }

    // update + inserts such that file1 is updated and expanded, a new file2 is created.
    String commitTime3 = "003";
    client.startCommitWithTime(commitTime3);
    List<HoodieRecord> insertsAndUpdates3 = dataGen.generateInserts(commitTime3, 200);
    Set<String> keys3 = recordsToRecordKeySet(insertsAndUpdates3);
    List<HoodieRecord> updates3 = dataGen.generateUpdates(commitTime3, inserts2);
    insertsAndUpdates3.addAll(updates3);

    JavaRDD<HoodieRecord> insertAndUpdatesRDD3 = jsc.parallelize(insertsAndUpdates3, 1);
    statuses = client.upsert(insertAndUpdatesRDD3, commitTime3).collect();
    assertNoWriteErrors(statuses);

    assertEquals(2, statuses.size(), "2 files needs to be committed.");
    HoodieTableMetaClient metadata = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).build();

    HoodieTable table = getHoodieTable(metadata, config);
    BaseFileOnlyView fileSystemView = table.getBaseFileOnlyView();
    List<HoodieBaseFile> files =
        fileSystemView.getLatestBaseFilesBeforeOrOn(testPartitionPath, commitTime3).collect(Collectors.toList());
    int numTotalInsertsInCommit3 = 0;
    int numTotalUpdatesInCommit3 = 0;
    for (HoodieBaseFile file : files) {
      if (file.getFileName().contains(file1)) {
        assertEquals(commitTime3, file.getCommitTime(), "Existing file should be expanded");
        records = fileUtils.readAvroRecords(hadoopConf, new Path(file.getPath()));
        for (GenericRecord record : records) {
          String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
          String recordCommitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
          if (recordCommitTime.equals(commitTime3)) {
            if (keys2.contains(recordKey)) {
              keys2.remove(recordKey);
              numTotalUpdatesInCommit3++;
            } else {
              numTotalInsertsInCommit3++;
            }
          }
        }
        assertEquals(0, keys2.size(), "All keys added in commit 2 must be updated in commit3 correctly");
      } else {
        assertEquals(commitTime3, file.getCommitTime(), "New file must be written for commit 3");
        records = fileUtils.readAvroRecords(hadoopConf, new Path(file.getPath()));
        for (GenericRecord record : records) {
          String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
          assertEquals(commitTime3, record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString(),
              "only expect commit3");
          assertTrue(keys3.contains(recordKey), "key expected to be part of commit3");
        }
        numTotalInsertsInCommit3 += records.size();
      }
    }
    assertEquals(numTotalUpdatesInCommit3, inserts2.size(), "Total updates in commit3 must add up");
    assertEquals(numTotalInsertsInCommit3, keys3.size(), "Total inserts in commit3 must add up");
  }

  /**
   * Test scenario of new file-group getting added during insert().
   */
  @ParameterizedTest
  @MethodSource("configParams")
  public void testSmallInsertHandlingForInserts(boolean mergeAllowDuplicateInserts) throws Exception {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit, false, mergeAllowDuplicateInserts); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    BaseFileUtils fileUtils = BaseFileUtils.getInstance(metaClient);

    // Inserts => will write file1
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, insertSplitLimit); // this writes ~500kb
    Set<String> keys1 = recordsToRecordKeySet(inserts1);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
    List<WriteStatus> statuses = client.insert(insertRecordsRDD1, commitTime1).collect();
    assertNoWriteErrors(statuses);
    assertPartitionMetadata(new String[] {testPartitionPath}, fs);
    assertEquals(1, statuses.size(), "Just 1 file needs to be added.");
    String file1 = statuses.get(0).getFileId();
    assertEquals(100,
        fileUtils.readRowKeys(hadoopConf, new Path(basePath, statuses.get(0).getStat().getPath()))
            .size(), "file should contain 100 records");

    // Second, set of Inserts should just expand file1
    String commitTime2 = "002";
    client.startCommitWithTime(commitTime2);
    List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime2, 40);
    Set<String> keys2 = recordsToRecordKeySet(inserts2);
    JavaRDD<HoodieRecord> insertRecordsRDD2 = jsc.parallelize(inserts2, 1);
    statuses = client.insert(insertRecordsRDD2, commitTime2).collect();
    assertNoWriteErrors(statuses);
    assertEquals(1, statuses.size(), "Just 1 file needs to be updated.");
    assertEquals(file1, statuses.get(0).getFileId(), "Existing file should be expanded");
    assertEquals(commitTime1, statuses.get(0).getStat().getPrevCommit(), "Existing file should be expanded");

    Path newFile = new Path(basePath, statuses.get(0).getStat().getPath());
    assertEquals(140, fileUtils.readRowKeys(hadoopConf, newFile).size(),
        "file should contain 140 records");
    List<GenericRecord> records = fileUtils.readAvroRecords(hadoopConf, newFile);
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      String recCommitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
      assertTrue(commitTime1.equals(recCommitTime) || commitTime2.equals(recCommitTime),
          "Record expected to be part of commit 1 or commit2");
      assertTrue(keys2.contains(recordKey) || keys1.contains(recordKey),
          "key expected to be part of commit 1 or commit2");
    }

    // Lots of inserts such that file1 is updated and expanded, a new file2 is created.
    String commitTime3 = "003";
    client.startCommitWithTime(commitTime3);
    List<HoodieRecord> inserts3 = dataGen.generateInserts(commitTime3, 200);
    JavaRDD<HoodieRecord> insertRecordsRDD3 = jsc.parallelize(inserts3, 1);
    statuses = client.insert(insertRecordsRDD3, commitTime3).collect();
    assertNoWriteErrors(statuses);
    assertEquals(2, statuses.size(), "2 files needs to be committed.");
    assertEquals(340,
        fileUtils.readRowKeys(hadoopConf, new Path(basePath, statuses.get(0).getStat().getPath())).size()
            + fileUtils.readRowKeys(hadoopConf, new Path(basePath, statuses.get(1).getStat().getPath())).size(),
        "file should contain 340 records");

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).build();
    HoodieTable table = getHoodieTable(metaClient, config);
    List<HoodieBaseFile> files = table.getBaseFileOnlyView()
        .getLatestBaseFilesBeforeOrOn(testPartitionPath, commitTime3).collect(Collectors.toList());
    assertEquals(2, files.size(), "Total of 2 valid data files");

    int totalInserts = 0;
    for (HoodieBaseFile file : files) {
      assertEquals(commitTime3, file.getCommitTime(), "All files must be at commit 3");
      totalInserts += fileUtils.readAvroRecords(hadoopConf, new Path(file.getPath())).size();
    }
    assertEquals(totalInserts, inserts1.size() + inserts2.size() + inserts3.size(), "Total number of records must add up");
  }

  /**
   * Test delete with delete api.
   */
  @Test
  public void testDeletesWithDeleteApi() throws Exception {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});

    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // Inserts => will write file1
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, insertSplitLimit); // this writes ~500kb
    Set<String> keys1 = recordsToRecordKeySet(inserts1);
    List<String> keysSoFar = new ArrayList<>(keys1);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
    List<WriteStatus> statuses = client.upsert(insertRecordsRDD1, commitTime1).collect();

    assertNoWriteErrors(statuses);

    assertEquals(1, statuses.size(), "Just 1 file needs to be added.");
    String file1 = statuses.get(0).getFileId();
    assertEquals(100,
        BaseFileUtils.getInstance(metaClient).readRowKeys(hadoopConf, new Path(basePath, statuses.get(0).getStat().getPath()))
            .size(), "file should contain 100 records");

    // Delete 20 among 100 inserted
    testDeletes(client, inserts1, 20, file1, "002", 80, keysSoFar);

    // Insert and update 40 records
    Pair<Set<String>, List<HoodieRecord>> updateBatch2 = testUpdates("003", client, 40, 120);
    keysSoFar.addAll(updateBatch2.getLeft());

    // Delete 10 records among 40 updated
    testDeletes(client, updateBatch2.getRight(), 10, file1, "004", 110, keysSoFar);

    // do another batch of updates
    Pair<Set<String>, List<HoodieRecord>> updateBatch3 = testUpdates("005", client, 40, 150);
    keysSoFar.addAll(updateBatch3.getLeft());

    // delete non existent keys
    String commitTime6 = "006";
    client.startCommitWithTime(commitTime6);

    List<HoodieRecord> dummyInserts3 = dataGen.generateInserts(commitTime6, 20);
    List<HoodieKey> hoodieKeysToDelete3 = randomSelectAsHoodieKeys(dummyInserts3, 20);
    JavaRDD<HoodieKey> deleteKeys3 = jsc.parallelize(hoodieKeysToDelete3, 1);
    statuses = client.delete(deleteKeys3, commitTime6).collect();
    assertNoWriteErrors(statuses);
    assertEquals(0, statuses.size(), "Just 0 write status for delete.");

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(150,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain " + 150 + " records");

    // delete another batch. previous delete commit should have persisted the schema. If not,
    // this will throw exception
    testDeletes(client, updateBatch3.getRight(), 10, file1, "007", 140, keysSoFar);
  }

  @Test
  public void testSimpleClustering() throws Exception {
    // setup clustering config
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1).build();
    testInsertAndClustering(clusteringConfig, SqlQueryEqualityPreCommitValidator.class.getName(), COUNT_SQL_QUERY_FOR_VALIDATION, "");
  }

  @Test
  public void testClusteringWithSortColumns() throws Exception {
    // setup clustering config
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringSortColumns("_hoodie_record_key")
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1).build();
    testInsertAndClustering(clusteringConfig, SqlQueryEqualityPreCommitValidator.class.getName(), COUNT_SQL_QUERY_FOR_VALIDATION, "");
  }

  @Test
  public void testClusteringWithFailingValidator() throws Exception {
    // setup clustering config
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringSortColumns("_hoodie_record_key")
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1).build();
    try {
      testInsertAndClustering(clusteringConfig, FailingPreCommitValidator.class.getName(), COUNT_SQL_QUERY_FOR_VALIDATION, "");
      fail("expected pre-commit clustering validation to fail");
    } catch (HoodieValidationException e) {
      // expected
    }
  }

  @Test
  public void testClusteringInvalidConfigForSqlQueryValidator() throws Exception {
    // setup clustering config
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1).build();
    try {
      testInsertAndClustering(clusteringConfig, SqlQueryEqualityPreCommitValidator.class.getName(), "", "");
      fail("expected pre-commit clustering validation to fail because sql query is not configured");
    } catch (HoodieValidationException e) {
      // expected
    }
  }

  @Test
  public void testClusteringInvalidConfigForSqlQuerySingleResultValidator() throws Exception {
    // setup clustering config
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1).build();

    testInsertAndClustering(clusteringConfig, SqlQuerySingleResultPreCommitValidator.class.getName(),
        "", COUNT_SQL_QUERY_FOR_VALIDATION + "#400");
  }

  @Test
  public void testClusteringInvalidConfigForSqlQuerySingleResultValidatorFailure() throws Exception {
    // setup clustering config
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1).build();

    try {
      testInsertAndClustering(clusteringConfig, SqlQuerySingleResultPreCommitValidator.class.getName(),
          "", COUNT_SQL_QUERY_FOR_VALIDATION + "#802");
      fail("expected pre-commit clustering validation to fail because of count mismatch. expect 400 rows, not 802");
    } catch (HoodieValidationException e) {
      // expected
    }
  }

  @Test
  public void testPendingClusteringRollback() throws Exception {
    // setup clustering config
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(1).build();

    List<HoodieRecord> initialRecords = testInsertTwoBatches();
    try {
      testClustering(clusteringConfig, SqlQuerySingleResultPreCommitValidator.class.getName(),
          "", COUNT_SQL_QUERY_FOR_VALIDATION + "#802", initialRecords);
      fail("expected pre-commit clustering validation to fail because of count mismatch. expect 400 rows, not 802");
    } catch (HoodieValidationException e) {
      // expected
    }

    String pendingClusteringInstant = ClusteringUtils.getAllPendingClusteringPlans(metaClient).findFirst().get().getLeft().getTimestamp();
    getHoodieWriteClient(getSmallInsertWriteConfig(100)).rollback(pendingClusteringInstant);
    List<HoodieRecord> newRecords = testInsertTwoBatches();
    List<HoodieRecord> expectedRecords = Stream.concat(initialRecords.stream(), newRecords.stream()).collect(Collectors.toList());
    testClustering(clusteringConfig, SqlQuerySingleResultPreCommitValidator.class.getName(),
        "", COUNT_SQL_QUERY_FOR_VALIDATION + "#" + expectedRecords.size(), expectedRecords);
  }

  private void testInsertAndClustering(HoodieClusteringConfig clusteringConfig, String validatorClasses,
                                       String sqlQueryForEqualityValidation, String sqlQueryForSingleResultValidation) throws Exception {
    List<HoodieRecord> allRecords = testInsertTwoBatches();
    testClustering(clusteringConfig, validatorClasses, sqlQueryForEqualityValidation, sqlQueryForSingleResultValidation, allRecords);

  }

  private List<HoodieRecord> testInsertTwoBatches() {
    // create config to not update small files.
    HoodieWriteConfig config = getSmallInsertWriteConfig(2000, false, 10);
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    dataGen = new HoodieTestDataGenerator(new String[] {"2015/03/16"});
    String commitTime = HoodieActiveTimeline.createNewInstantTime();
    List<HoodieRecord> records1 = dataGen.generateInserts(commitTime, 200);
    List<WriteStatus> statuses1 = writeAndVerifyBatch(client, records1, commitTime);
    Set<HoodieFileGroupId> fileIds1 = getFileGroupIdsFromWriteStatus(statuses1);

    commitTime = HoodieActiveTimeline.createNewInstantTime();
    List<HoodieRecord> records2 = dataGen.generateInserts(commitTime, 200);
    List<WriteStatus> statuses2 = writeAndVerifyBatch(client, records2, commitTime);
    Set<HoodieFileGroupId> fileIds2 = getFileGroupIdsFromWriteStatus(statuses2);
    //verify new files are created for 2nd write
    Set<HoodieFileGroupId> fileIdIntersection = new HashSet<>(fileIds1);
    fileIdIntersection.retainAll(fileIds2);
    assertEquals(0, fileIdIntersection.size());
    return Stream.concat(records1.stream(), records2.stream()).collect(Collectors.toList());
  }
  
  private String testClustering(HoodieClusteringConfig clusteringConfig, String validatorClasses,
                              String sqlQueryForEqualityValidation, String sqlQueryForSingleResultValidation,
                              List<HoodieRecord> allRecords) {

    HoodieWriteMetadata<JavaRDD<WriteStatus>> clusterMetadata =
        performClustering(clusteringConfig, validatorClasses, sqlQueryForEqualityValidation, sqlQueryForSingleResultValidation, allRecords);

    String clusteringCommitTime = metaClient.reloadActiveTimeline().getCompletedReplaceTimeline()
        .getReverseOrderedInstants().findFirst().get().getTimestamp();
    verifyRecordsWritten(clusteringCommitTime, allRecords, clusterMetadata.getWriteStatuses().collect());
    return clusteringCommitTime;
  }
  
  private HoodieWriteMetadata<JavaRDD<WriteStatus>> performClustering(HoodieClusteringConfig clusteringConfig, String validatorClasses,
                                                                      String sqlQueryForEqualityValidation, String sqlQueryForSingleResultValidation,
                                                                      List<HoodieRecord> allRecords) {
    HoodiePreCommitValidatorConfig validatorConfig = HoodiePreCommitValidatorConfig.newBuilder()
        .withPreCommitValidator(StringUtils.nullToEmpty(validatorClasses))
        .withPrecommitValidatorEqualitySqlQueries(sqlQueryForEqualityValidation)
        .withPrecommitValidatorSingleResultSqlQueries(sqlQueryForSingleResultValidation)
        .build();
    
    HoodieWriteConfig config = getConfigBuilder().withAutoCommit(false)
        .withPreCommitValidatorConfig(validatorConfig)
        .withClusteringConfig(clusteringConfig).build();

    // create client with new config.
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    String clusteringCommitTime = client.scheduleClustering(Option.empty()).get().toString();
    HoodieWriteMetadata<JavaRDD<WriteStatus>> clusterMetadata = client.cluster(clusteringCommitTime, true);
    return clusterMetadata;
  }

  private Set<HoodieFileGroupId> getFileGroupIdsFromWriteStatus(List<WriteStatus> statuses) {
    return statuses.stream().map(s -> new HoodieFileGroupId(s.getPartitionPath(), s.getFileId())).collect(Collectors.toSet());
  }

  /**
   * Test scenario of writing more file groups than existing number of file groups in partition.
   */
  @Test
  public void testInsertOverwritePartitionHandlingWithMoreRecords() throws Exception {
    verifyInsertOverwritePartitionHandling(1000, 3000);
  }

  /**
   * Test scenario of writing fewer file groups than existing number of file groups in partition.
   */
  @Test
  public void testInsertOverwritePartitionHandlingWithFewerRecords() throws Exception {
    verifyInsertOverwritePartitionHandling(3000, 1000);
  }

  /**
   * Test scenario of writing similar number file groups in partition.
   */
  @Test
  public void testInsertOverwritePartitionHandlingWithSimilarNumberOfRecords() throws Exception {
    verifyInsertOverwritePartitionHandling(3000, 3000);
  }

  /**
   *  1) Do write1 (upsert) with 'batch1RecordsCount' number of records.
   *  2) Do write2 (insert overwrite) with 'batch2RecordsCount' number of records.
   *
   *  Verify that all records in step1 are overwritten
   */
  private void verifyInsertOverwritePartitionHandling(int batch1RecordsCount, int batch2RecordsCount) throws Exception {
    final String testPartitionPath = "americas";
    HoodieWriteConfig config = getSmallInsertWriteConfig(2000, false);
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});

    // Do Inserts
    String commit1 = "001";
    List<WriteStatus> statuses = writeAndVerifyBatch(client, dataGen.generateInserts(commit1, batch1RecordsCount), commit1);
    Set<String> batch1Buckets = getFileIdsFromWriteStatus(statuses);

    // Do Insert Overwrite
    String commitTime2 = "002";
    client.startCommitWithTime(commitTime2, HoodieTimeline.REPLACE_COMMIT_ACTION);
    List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime2, batch2RecordsCount);
    List<HoodieRecord> insertsAndUpdates2 = new ArrayList<>();
    insertsAndUpdates2.addAll(inserts2);
    JavaRDD<HoodieRecord> insertAndUpdatesRDD2 = jsc.parallelize(insertsAndUpdates2, 2);
    HoodieWriteResult writeResult = client.insertOverwrite(insertAndUpdatesRDD2, commitTime2);
    statuses = writeResult.getWriteStatuses().collect();
    assertNoWriteErrors(statuses);

    assertEquals(batch1Buckets, new HashSet<>(writeResult.getPartitionToReplaceFileIds().get(testPartitionPath)));
    verifyRecordsWritten(commitTime2, inserts2, statuses);
  }

  private Set<String> getFileIdsFromWriteStatus(List<WriteStatus> statuses) {
    return statuses.stream().map(s -> s.getFileId()).collect(Collectors.toSet());
  }

  /**
   * Test scenario of writing fewer file groups for first partition than second an third partition.
   */
  @Test
  public void verifyDeletePartitionsHandlingWithFewerRecordsFirstPartition() throws Exception {
    verifyDeletePartitionsHandling(1000, 3000, 3000);
  }

  /**
   * Test scenario of writing similar number file groups in partition.
   */
  @Test
  public void verifyDeletePartitionsHandlingWithSimilarNumberOfRecords() throws Exception {
    verifyDeletePartitionsHandling(3000, 3000, 3000);
  }

  /**
   * Test scenario of writing more file groups for first partition than second an third partition.
   */
  @Test
  public void verifyDeletePartitionsHandlingHandlingWithFewerRecordsSecondThirdPartition() throws Exception {
    verifyDeletePartitionsHandling(3000, 1000, 1000);
  }

  private Set<String> insertPartitionRecordsWithCommit(SparkRDDWriteClient client, int recordsCount, String commitTime1, String partitionPath) {
    client.startCommitWithTime(commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInsertsForPartition(commitTime1, recordsCount, partitionPath);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 2);
    List<WriteStatus> statuses = client.upsert(insertRecordsRDD1, commitTime1).collect();
    assertNoWriteErrors(statuses);
    Set<String> batchBuckets = statuses.stream().map(s -> s.getFileId()).collect(Collectors.toSet());
    verifyRecordsWritten(commitTime1, inserts1, statuses);
    return batchBuckets;
  }

  private Set<String> deletePartitionWithCommit(SparkRDDWriteClient client, String commitTime, List<String> deletePartitionPath) {
    client.startCommitWithTime(commitTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
    HoodieWriteResult writeResult = client.deletePartitions(deletePartitionPath, commitTime);
    Set<String> deletePartitionReplaceFileIds =
        writeResult.getPartitionToReplaceFileIds().entrySet()
            .stream().flatMap(entry -> entry.getValue().stream()).collect(Collectors.toSet());
    return deletePartitionReplaceFileIds;
  }

  /**
   *  1) Do write1 (upsert) with 'batch1RecordsCount' number of records for first partition.
   *  2) Do write2 (upsert) with 'batch2RecordsCount' number of records for second partition.
   *  3) Do write3 (upsert) with 'batch3RecordsCount' number of records for third partition.
   *  4) delete first partition and check result.
   *  5) delete second and third partition and check result.
   *
   */
  private void verifyDeletePartitionsHandling(int batch1RecordsCount, int batch2RecordsCount, int batch3RecordsCount) throws Exception {
    HoodieWriteConfig config = getSmallInsertWriteConfig(2000, false);
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    dataGen = new HoodieTestDataGenerator();

    // Do Inserts for DEFAULT_FIRST_PARTITION_PATH
    String commitTime1 = "001";
    Set<String> batch1Buckets =
        this.insertPartitionRecordsWithCommit(client, batch1RecordsCount, commitTime1, DEFAULT_FIRST_PARTITION_PATH);

    // Do Inserts for DEFAULT_SECOND_PARTITION_PATH
    String commitTime2 = "002";
    Set<String> batch2Buckets =
        this.insertPartitionRecordsWithCommit(client, batch2RecordsCount, commitTime2, DEFAULT_SECOND_PARTITION_PATH);

    // Do Inserts for DEFAULT_THIRD_PARTITION_PATH
    String commitTime3 = "003";
    Set<String> batch3Buckets =
        this.insertPartitionRecordsWithCommit(client, batch3RecordsCount, commitTime3, DEFAULT_THIRD_PARTITION_PATH);

    // delete DEFAULT_FIRST_PARTITION_PATH
    String commitTime4 = "004";
    Set<String> deletePartitionReplaceFileIds1 =
        deletePartitionWithCommit(client, commitTime4, Arrays.asList(DEFAULT_FIRST_PARTITION_PATH));
    assertEquals(batch1Buckets, deletePartitionReplaceFileIds1);
    List<HoodieBaseFile> baseFiles = HoodieClientTestUtils.getLatestBaseFiles(basePath, fs,
        String.format("%s/%s/*", basePath, DEFAULT_FIRST_PARTITION_PATH));
    assertEquals(0, baseFiles.size());
    baseFiles = HoodieClientTestUtils.getLatestBaseFiles(basePath, fs,
        String.format("%s/%s/*", basePath, DEFAULT_SECOND_PARTITION_PATH));
    assertTrue(baseFiles.size() > 0);
    baseFiles = HoodieClientTestUtils.getLatestBaseFiles(basePath, fs,
        String.format("%s/%s/*", basePath, DEFAULT_THIRD_PARTITION_PATH));
    assertTrue(baseFiles.size() > 0);

    // delete DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH
    String commitTime5 = "005";
    Set<String> deletePartitionReplaceFileIds2 =
        deletePartitionWithCommit(client, commitTime5, Arrays.asList(DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH));
    Set<String> expectedFileId = new HashSet<>();
    expectedFileId.addAll(batch2Buckets);
    expectedFileId.addAll(batch3Buckets);
    assertEquals(expectedFileId, deletePartitionReplaceFileIds2);

    baseFiles = HoodieClientTestUtils.getLatestBaseFiles(basePath, fs,
        String.format("%s/%s/*", basePath, DEFAULT_FIRST_PARTITION_PATH),
        String.format("%s/%s/*", basePath, DEFAULT_SECOND_PARTITION_PATH),
        String.format("%s/%s/*", basePath, DEFAULT_THIRD_PARTITION_PATH));
    assertEquals(0, baseFiles.size());
  }

  /**
   * Verify data in base files matches expected records and commit time.
   */
  private void verifyRecordsWritten(String commitTime, List<HoodieRecord> expectedRecords, List<WriteStatus> allStatus) {
    List<GenericRecord> records = new ArrayList<>();
    for (WriteStatus status : allStatus) {
      Path filePath = new Path(basePath, status.getStat().getPath());
      records.addAll(BaseFileUtils.getInstance(metaClient).readAvroRecords(jsc.hadoopConfiguration(), filePath));
    }

    Set<String> expectedKeys = recordsToRecordKeySet(expectedRecords);
    assertEquals(records.size(), expectedKeys.size());
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      assertEquals(commitTime,
          record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
      assertTrue(expectedKeys.contains(recordKey));
    }
  }

  private List<WriteStatus> writeAndVerifyBatch(SparkRDDWriteClient client, List<HoodieRecord> inserts, String commitTime) {
    client.startCommitWithTime(commitTime);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts, 2);
    List<WriteStatus> statuses = client.upsert(insertRecordsRDD1, commitTime).collect();
    assertNoWriteErrors(statuses);
    verifyRecordsWritten(commitTime, inserts, statuses);
    return statuses;
  }

  private Pair<Set<String>, List<HoodieRecord>> testUpdates(String instantTime, SparkRDDWriteClient client,
      int sizeToInsertAndUpdate, int expectedTotalRecords)
      throws IOException {
    client.startCommitWithTime(instantTime);
    List<HoodieRecord> inserts = dataGen.generateInserts(instantTime, sizeToInsertAndUpdate);
    Set<String> keys = recordsToRecordKeySet(inserts);
    List<HoodieRecord> insertsAndUpdates = new ArrayList<>();
    insertsAndUpdates.addAll(inserts);
    insertsAndUpdates.addAll(dataGen.generateUpdates(instantTime, inserts));

    JavaRDD<HoodieRecord> insertAndUpdatesRDD = jsc.parallelize(insertsAndUpdates, 1);
    List<WriteStatus> statuses = client.upsert(insertAndUpdatesRDD, instantTime).collect();
    assertNoWriteErrors(statuses);

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(expectedTotalRecords,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain " + expectedTotalRecords + " records");
    return Pair.of(keys, inserts);
  }

  private void testDeletes(SparkRDDWriteClient client, List<HoodieRecord> previousRecords, int sizeToDelete,
      String existingFile, String instantTime, int expectedRecords, List<String> keys) {
    client.startCommitWithTime(instantTime);

    List<HoodieKey> hoodieKeysToDelete = randomSelectAsHoodieKeys(previousRecords, sizeToDelete);
    JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(hoodieKeysToDelete, 1);
    List<WriteStatus> statuses = client.delete(deleteKeys, instantTime).collect();

    assertNoWriteErrors(statuses);

    assertEquals(1, statuses.size(), "Just 1 file needs to be added.");
    assertEquals(existingFile, statuses.get(0).getFileId(), "Existing file should be expanded");

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(expectedRecords,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain " + expectedRecords + " records");

    Path newFile = new Path(basePath, statuses.get(0).getStat().getPath());
    assertEquals(expectedRecords,
        BaseFileUtils.getInstance(metaClient).readRowKeys(hadoopConf, newFile).size(),
        "file should contain 110 records");

    List<GenericRecord> records = BaseFileUtils.getInstance(metaClient).readAvroRecords(hadoopConf, newFile);
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      assertTrue(keys.contains(recordKey), "key expected to be part of " + instantTime);
      assertFalse(hoodieKeysToDelete.contains(recordKey), "Key deleted");
    }
  }

  /**
   * Test delete with delete api.
   */
  @Test
  public void testDeletesWithoutInserts() {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit, true); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    SparkRDDWriteClient client = getHoodieWriteClient(config);

    // delete non existent keys
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);

    List<HoodieRecord> dummyInserts = dataGen.generateInserts(commitTime1, 20);
    List<HoodieKey> hoodieKeysToDelete = randomSelectAsHoodieKeys(dummyInserts, 20);
    JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(hoodieKeysToDelete, 1);
    assertThrows(HoodieIOException.class, () -> {
      client.delete(deleteKeys, commitTime1).collect();
    }, "Should have thrown Exception");
  }

  /**
   * Test to ensure commit metadata points to valid files.
   */
  @Test
  public void testCommitWritesRelativePaths() throws Exception {

    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg);) {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).build();
      HoodieSparkTable table = HoodieSparkTable.create(cfg, context, metaClient);

      String instantTime = "000";
      client.startCommitWithTime(instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, instantTime);

      assertTrue(client.commit(instantTime, result), "Commit should succeed");
      assertTrue(testTable.commitExists(instantTime),
          "After explicit commit, commit file should be created");

      // Get base file paths from commit metadata
      String actionType = metaClient.getCommitActionType();
      HoodieInstant commitInstant = new HoodieInstant(false, actionType, instantTime);
      HoodieTimeline commitTimeline = metaClient.getCommitTimeline().filterCompletedInstants();
      HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
          .fromBytes(commitTimeline.getInstantDetails(commitInstant).get(), HoodieCommitMetadata.class);
      String basePath = table.getMetaClient().getBasePath();
      Collection<String> commitPathNames = commitMetadata.getFileIdAndFullPaths(basePath).values();

      // Read from commit file
      try (FSDataInputStream inputStream = fs.open(testTable.getCommitFilePath(instantTime))) {
        String everything = FileIOUtils.readAsUTFString(inputStream);
        HoodieCommitMetadata metadata = HoodieCommitMetadata.fromJsonString(everything, HoodieCommitMetadata.class);
        HashMap<String, String> paths = metadata.getFileIdAndFullPaths(basePath);
        // Compare values in both to make sure they are equal.
        for (String pathName : paths.values()) {
          assertTrue(commitPathNames.contains(pathName));
        }
      }
    }
  }

  /**
   * Test to ensure commit metadata points to valid files.10.
   */
  @Test
  public void testMetadataStatsOnCommit() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    String instantTime0 = "000";
    client.startCommitWithTime(instantTime0);

    List<HoodieRecord> records0 = dataGen.generateInserts(instantTime0, 200);
    JavaRDD<HoodieRecord> writeRecords0 = jsc.parallelize(records0, 1);
    JavaRDD<WriteStatus> result0 = client.bulkInsert(writeRecords0, instantTime0);

    assertTrue(client.commit(instantTime0, result0), "Commit should succeed");
    assertTrue(testTable.commitExists(instantTime0),
        "After explicit commit, commit file should be created");

    // Read from commit file
    try (FSDataInputStream inputStream = fs.open(testTable.getCommitFilePath(instantTime0))) {
      String everything = FileIOUtils.readAsUTFString(inputStream);
      HoodieCommitMetadata metadata =
          HoodieCommitMetadata.fromJsonString(everything, HoodieCommitMetadata.class);
      int inserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          inserts += stat.getNumInserts();
        }
      }
      assertEquals(200, inserts);
    }

    // Update + Inserts such that they just expand file1
    String instantTime1 = "001";
    client.startCommitWithTime(instantTime1);

    List<HoodieRecord> records1 = dataGen.generateUpdates(instantTime1, records0);
    JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(records1, 1);
    JavaRDD<WriteStatus> result1 = client.upsert(writeRecords1, instantTime1);

    assertTrue(client.commit(instantTime1, result1), "Commit should succeed");
    assertTrue(testTable.commitExists(instantTime1),
        "After explicit commit, commit file should be created");

    // Read from commit file
    try (FSDataInputStream inputStream = fs.open(testTable.getCommitFilePath(instantTime1))) {
      String everything = FileIOUtils.readAsUTFString(inputStream);
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromJsonString(everything, HoodieCommitMetadata.class);
      int inserts = 0;
      int upserts = 0;
      for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat stat : pstat.getValue()) {
          inserts += stat.getNumInserts();
          upserts += stat.getNumUpdateWrites();
        }
      }
      assertEquals(0, inserts);
      assertEquals(200, upserts);
    }
  }

  /**
   * Tests behavior of committing only when consistency is verified.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testConsistencyCheckDuringFinalize(boolean enableOptimisticConsistencyGuard) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).build();
    String instantTime = "000";
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder()
        .withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard).build()).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    Pair<Path, JavaRDD<WriteStatus>> result = testConsistencyCheck(metaClient, instantTime, enableOptimisticConsistencyGuard);

    // Delete orphan marker and commit should succeed
    metaClient.getFs().delete(result.getKey(), false);
    if (!enableOptimisticConsistencyGuard) {
      assertTrue(client.commit(instantTime, result.getRight()), "Commit should succeed");
      assertTrue(testTable.commitExists(instantTime),
          "After explicit commit, commit file should be created");
      // Marker directory must be removed
      assertFalse(metaClient.getFs().exists(new Path(metaClient.getMarkerFolderPath(instantTime))));
    } else {
      // with optimistic, first client.commit should have succeeded.
      assertTrue(testTable.commitExists(instantTime),
          "After explicit commit, commit file should be created");
      // Marker directory must be removed
      assertFalse(metaClient.getFs().exists(new Path(metaClient.getMarkerFolderPath(instantTime))));
    }
  }

  private void testRollbackAfterConsistencyCheckFailureUsingFileList(boolean rollbackUsingMarkers, boolean enableOptimisticConsistencyGuard) throws Exception {
    String instantTime = "000";
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).build();
    HoodieWriteConfig cfg = !enableOptimisticConsistencyGuard ? getConfigBuilder().withRollbackUsingMarkers(rollbackUsingMarkers).withAutoCommit(false)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
            .withMaxConsistencyCheckIntervalMs(1).withInitialConsistencyCheckIntervalMs(1).withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard).build()).build() :
        getConfigBuilder().withRollbackUsingMarkers(rollbackUsingMarkers).withAutoCommit(false)
            .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder()
                .withConsistencyCheckEnabled(true)
                .withOptimisticConsistencyGuardSleepTimeMs(1).build()).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    testConsistencyCheck(metaClient, instantTime, enableOptimisticConsistencyGuard);

    if (!enableOptimisticConsistencyGuard) {
      // Rollback of this commit should succeed with FailSafeCG
      client.rollback(instantTime);
      assertFalse(testTable.commitExists(instantTime),
          "After explicit rollback, commit file should not be present");
      // Marker directory must be removed after rollback
      assertFalse(metaClient.getFs().exists(new Path(metaClient.getMarkerFolderPath(instantTime))));
    } else {
      // if optimistic CG is enabled, commit should have succeeded.
      assertTrue(testTable.commitExists(instantTime),
          "With optimistic CG, first commit should succeed. commit file should be present");
      // Marker directory must be removed after rollback
      assertFalse(metaClient.getFs().exists(new Path(metaClient.getMarkerFolderPath(instantTime))));
      if (rollbackUsingMarkers) {
        // rollback of a completed commit should fail if marked based rollback is used.
        try {
          client.rollback(instantTime);
          fail("Rollback of completed commit should throw exception");
        } catch (HoodieRollbackException e) {
          // ignore
        }
      } else {
        // rollback of a completed commit should succeed if using list based rollback
        client.rollback(instantTime);
        assertFalse(testTable.commitExists(instantTime),
            "After explicit rollback, commit file should not be present");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRollbackAfterConsistencyCheckFailureUsingFileList(boolean enableOptimisticConsistencyGuard) throws Exception {
    testRollbackAfterConsistencyCheckFailureUsingFileList(false, enableOptimisticConsistencyGuard);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRollbackAfterConsistencyCheckFailureUsingMarkers(boolean enableOptimisticConsistencyGuard) throws Exception {
    testRollbackAfterConsistencyCheckFailureUsingFileList(true, enableOptimisticConsistencyGuard);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieFailedWritesCleaningPolicy.class, names = {"LAZY", "NEVER"})
  public void testRollbackFailedCommits(HoodieFailedWritesCleaningPolicy cleaningPolicy) throws Exception {
    HoodieTestUtils.init(hadoopConf, basePath);
    // Perform 2 failed writes to table
    SparkRDDWriteClient client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    writeBatch(client, "100", "100", Option.of(Arrays.asList("100")), "100",
            100, dataGen::generateInserts, SparkRDDWriteClient::bulkInsert, false, 100, 300,
            0, false);
    client.close();
    client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    writeBatch(client, "200", "100", Option.of(Arrays.asList("200")), "200",
            100, dataGen::generateInserts, SparkRDDWriteClient::bulkInsert, false, 100, 300,
            0, false);
    client.close();
    // refresh data generator to delete records generated from failed commits
    dataGen = new HoodieTestDataGenerator();
    // Perform 1 successful write
    client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    writeBatch(client, "300", "200", Option.of(Arrays.asList("300")), "300",
            100, dataGen::generateInserts, SparkRDDWriteClient::bulkInsert, false, 100, 300,
            0, true);
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).build();

    assertTrue(metaClient.getActiveTimeline().getTimelineOfActions(
            CollectionUtils.createSet(HoodieTimeline.ROLLBACK_ACTION)).countInstants() == 0);
    assertTrue(metaClient.getActiveTimeline().filterInflights().countInstants() == 2);
    assertTrue(metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants() == 1);
    // Await till enough time passes such that the first 2 failed commits heartbeats are expired
    boolean conditionMet = false;
    while (!conditionMet) {
      conditionMet = client.getHeartbeatClient().isHeartbeatExpired("200");
      Thread.sleep(2000);
    }
    client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    // Perform 1 successful write
    writeBatch(client, "400", "300", Option.of(Arrays.asList("400")), "400",
            100, dataGen::generateInserts, SparkRDDWriteClient::bulkInsert, false, 100, 300,
            0, true);
    client.clean();
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline().reload();
    if (cleaningPolicy.isLazy()) {
      assertTrue(
          timeline
                  .getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.ROLLBACK_ACTION))
                  .countInstants()
              == 2);
      // Since we write rollbacks not clean, there should be no clean action on the timeline
      assertTrue(
          timeline
                  .getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION))
                  .countInstants()
              == 0);
      assertTrue(timeline.getCommitsTimeline().filterCompletedInstants().countInstants() == 2);
    } else if (cleaningPolicy.isNever()) {
      assertTrue(
              timeline
                      .getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.ROLLBACK_ACTION))
                      .countInstants()
                      == 0);
      // There should be no clean or rollback action on the timeline
      assertTrue(
              timeline
                      .getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION))
                      .countInstants()
                      == 0);
      assertTrue(timeline.getCommitsTimeline().filterCompletedInstants().countInstants() == 2);
    }
  }

  @Test
  public void testRollbackFailedCommitsToggleCleaningPolicy() throws Exception {
    HoodieTestUtils.init(hadoopConf, basePath);
    HoodieFailedWritesCleaningPolicy cleaningPolicy = HoodieFailedWritesCleaningPolicy.EAGER;
    SparkRDDWriteClient client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    // Perform 1 failed writes to table
    writeBatch(client, "100", "100", Option.of(Arrays.asList("100")), "100",
            100, dataGen::generateInserts, SparkRDDWriteClient::bulkInsert, false, 100, 300,
            0, false);
    client.close();
    // Toggle cleaning policy to LAZY
    cleaningPolicy = HoodieFailedWritesCleaningPolicy.LAZY;
    // Perform 2 failed writes to table
    client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    writeBatch(client, "200", "100", Option.of(Arrays.asList("200")), "200",
            100, dataGen::generateInserts, SparkRDDWriteClient::bulkInsert, false, 100, 300,
            0, false);
    client.close();
    client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    writeBatch(client, "300", "200", Option.of(Arrays.asList("300")), "300",
            100, dataGen::generateInserts, SparkRDDWriteClient::bulkInsert, false, 100, 300,
            0, false);
    client.close();
    // Await till enough time passes such that the first 2 failed commits heartbeats are expired
    boolean conditionMet = false;
    while (!conditionMet) {
      conditionMet = client.getHeartbeatClient().isHeartbeatExpired("300");
      Thread.sleep(2000);
    }
    client.clean();
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline().reload();
    assertTrue(timeline.getTimelineOfActions(
            CollectionUtils.createSet(HoodieTimeline.ROLLBACK_ACTION)).countInstants() == 3);
    // Perform 2 failed commits
    client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    writeBatch(client, "400", "300", Option.of(Arrays.asList("400")), "400",
            100, dataGen::generateInserts, SparkRDDWriteClient::bulkInsert, false, 100, 300,
            0, false);
    client.close();
    client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    writeBatch(client, "500", "400", Option.of(Arrays.asList("500")), "500",
            100, dataGen::generateInserts, SparkRDDWriteClient::bulkInsert, false, 100, 300,
            0, false);
    client.close();
    // Toggle cleaning policy to EAGER
    cleaningPolicy = HoodieFailedWritesCleaningPolicy.EAGER;
    client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    client.startCommit();
    timeline = metaClient.getActiveTimeline().reload();
    assertTrue(timeline.getTimelineOfActions(
            CollectionUtils.createSet(HoodieTimeline.ROLLBACK_ACTION)).countInstants() == 5);
    assertTrue(timeline.getCommitsTimeline().filterCompletedInstants().countInstants() == 0);
  }

  @Test
  public void testParallelInsertAndCleanPreviousFailedCommits() throws Exception {
    HoodieFailedWritesCleaningPolicy cleaningPolicy = HoodieFailedWritesCleaningPolicy.LAZY;
    ExecutorService service = Executors.newFixedThreadPool(2);
    HoodieTestUtils.init(hadoopConf, basePath);
    // Perform 2 failed writes to table
    SparkRDDWriteClient client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    writeBatch(client, "100", "100", Option.of(Arrays.asList("100")), "100",
            100, dataGen::generateInserts, SparkRDDWriteClient::bulkInsert, false, 100, 100,
            0, false);
    client.close();
    client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    writeBatch(client, "200", "200", Option.of(Arrays.asList("200")), "200",
            100, dataGen::generateInserts, SparkRDDWriteClient::bulkInsert, false, 100, 100,
            0, false);
    client.close();
    // refresh data generator to delete records generated from failed commits
    dataGen = new HoodieTestDataGenerator();
    // Create a succesful commit
    Future<JavaRDD<WriteStatus>> commit3 = service.submit(() -> writeBatch(new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy)),
            "300", "200", Option.of(Arrays.asList("300")), "200", 100, dataGen::generateInserts,
            SparkRDDWriteClient::bulkInsert, false, 100, 100, 0, true));
    commit3.get();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(basePath).build();

    assertTrue(metaClient.getActiveTimeline().getTimelineOfActions(
        CollectionUtils.createSet(HoodieTimeline.ROLLBACK_ACTION)).countInstants() == 0);
    assertTrue(metaClient.getActiveTimeline().filterInflights().countInstants() == 2);
    assertTrue(metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants() == 1);
    client = new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy));
    // Await till enough time passes such that the first 2 failed commits heartbeats are expired
    boolean conditionMet = false;
    while (!conditionMet) {
      conditionMet = client.getHeartbeatClient().isHeartbeatExpired("200");
      Thread.sleep(2000);
    }
    Future<JavaRDD<WriteStatus>> commit4 = service.submit(() -> writeBatch(new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy)),
            "400", "300", Option.of(Arrays.asList("400")), "400", 100, dataGen::generateInserts,
            SparkRDDWriteClient::bulkInsert, false, 100, 100, 0, true));
    Future<HoodieCleanMetadata> clean1 = service.submit(() -> new SparkRDDWriteClient(context, getParallelWritingWriteConfig(cleaningPolicy)).clean());
    commit4.get();
    clean1.get();
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline().reload();
    assertTrue(timeline.getTimelineOfActions(
        CollectionUtils.createSet(HoodieTimeline.ROLLBACK_ACTION)).countInstants() == 2);
    // Since we write rollbacks not clean, there should be no clean action on the timeline
    assertTrue(timeline.getTimelineOfActions(
        CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION)).countInstants() == 0);
    assertTrue(timeline.getCommitsTimeline().filterCompletedInstants().countInstants() == 2);
  }

  private Pair<Path, JavaRDD<WriteStatus>> testConsistencyCheck(HoodieTableMetaClient metaClient, String instantTime, boolean enableOptimisticConsistencyGuard)
      throws Exception {
    HoodieWriteConfig cfg = !enableOptimisticConsistencyGuard ? (getConfigBuilder().withAutoCommit(false)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
            .withMaxConsistencyCheckIntervalMs(1).withInitialConsistencyCheckIntervalMs(1).withEnableOptimisticConsistencyGuard(enableOptimisticConsistencyGuard).build())
        .build()) : (getConfigBuilder().withAutoCommit(false)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
            .withOptimisticConsistencyGuardSleepTimeMs(1).build())
        .build());
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);

    client.startCommitWithTime(instantTime);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(dataGen.generateInserts(instantTime, 200), 1);
    JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, instantTime);
    result.collect();

    // Create a dummy marker file to simulate the case that a marker file was created without data file.
    // This should fail the commit
    String partitionPath = Arrays
        .stream(fs.globStatus(new Path(String.format("%s/*/*/*/*", metaClient.getMarkerFolderPath(instantTime))),
            path -> path.toString().contains(HoodieTableMetaClient.MARKER_EXTN)))
        .limit(1).map(status -> status.getPath().getParent().toString()).collect(Collectors.toList()).get(0);

    Path markerFilePath = new MarkerFiles(fs, basePath, metaClient.getMarkerFolderPath(instantTime), instantTime)
        .create(partitionPath,
            FSUtils.makeDataFileName(instantTime, "1-0-1", UUID.randomUUID().toString()),
            IOType.MERGE);
    LOG.info("Created a dummy marker path=" + markerFilePath);

    if (!enableOptimisticConsistencyGuard) {
      Exception e = assertThrows(HoodieCommitException.class, () -> {
        client.commit(instantTime, result);
      }, "Commit should fail due to consistency check");
      assertTrue(e.getCause() instanceof HoodieIOException);
    } else {
      // with optimistic CG, commit should succeed
      client.commit(instantTime, result);
    }
    return Pair.of(markerFilePath, result);
  }

  @Test
  public void testMultiOperationsPerCommit() throws IOException {
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false)
        .withAllowMultiWriteOnSameInstant(true)
        .build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    String firstInstantTime = "0000";
    client.startCommitWithTime(firstInstantTime);
    int numRecords = 200;
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(dataGen.generateInserts(firstInstantTime, numRecords), 1);
    JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, firstInstantTime);
    assertTrue(client.commit(firstInstantTime, result), "Commit should succeed");
    assertTrue(testTable.commitExists(firstInstantTime),
        "After explicit commit, commit file should be created");

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(numRecords,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain " + numRecords + " records");

    String nextInstantTime = "0001";
    client.startCommitWithTime(nextInstantTime);
    JavaRDD<HoodieRecord> updateRecords = jsc.parallelize(dataGen.generateUpdates(nextInstantTime, numRecords), 1);
    JavaRDD<HoodieRecord> insertRecords = jsc.parallelize(dataGen.generateInserts(nextInstantTime, numRecords), 1);
    JavaRDD<WriteStatus> inserts = client.bulkInsert(insertRecords, nextInstantTime);
    JavaRDD<WriteStatus> upserts = client.upsert(updateRecords, nextInstantTime);
    assertTrue(client.commit(nextInstantTime, inserts.union(upserts)), "Commit should succeed");
    assertTrue(testTable.commitExists(firstInstantTime),
        "After explicit commit, commit file should be created");
    int totalRecords = 2 * numRecords;
    assertEquals(totalRecords, HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain " + totalRecords + " records");
  }

  /**
   * Build Hoodie Write Config for small data file sizes.
   */
  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize) {
    return getSmallInsertWriteConfig(insertSplitSize, false);
  }

  /**
   * Build Hoodie Write Config for small data file sizes.
   */
  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, boolean useNullSchema) {
    return getSmallInsertWriteConfig(insertSplitSize, useNullSchema, false);
  }

  /**
   * Build Hoodie Write Config for small data file sizes.
   */
  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, boolean useNullSchema, boolean mergeAllowDuplicateInserts) {
    return getSmallInsertWriteConfig(insertSplitSize, useNullSchema, dataGen.getEstimatedFileSizeInBytes(150), mergeAllowDuplicateInserts);
  }

  /**
   * Build Hoodie Write Config for specified small file sizes.
   */
  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, boolean useNullSchema, long smallFileSize) {
    return getSmallInsertWriteConfig(insertSplitSize, useNullSchema, smallFileSize, false);
  }

  /**
   * Build Hoodie Write Config for specified small file sizes.
   */
  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, boolean useNullSchema, long smallFileSize, boolean mergeAllowDuplicateInserts) {
    String schemaStr = useNullSchema ? NULL_SCHEMA : TRIP_EXAMPLE_SCHEMA;
    return getSmallInsertWriteConfig(insertSplitSize, schemaStr, smallFileSize, mergeAllowDuplicateInserts);
  }

  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, String schemaStr, long smallFileSize) {
    return getSmallInsertWriteConfig(insertSplitSize, schemaStr, smallFileSize, false);
  }

  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, String schemaStr, long smallFileSize, boolean mergeAllowDuplicateInserts) {
    return getSmallInsertWriteConfig(insertSplitSize, schemaStr, smallFileSize, mergeAllowDuplicateInserts, new Properties());
  }

  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, String schemaStr, long smallFileSize, Properties props) {
    return getSmallInsertWriteConfig(insertSplitSize, schemaStr, smallFileSize, false, props);
  }

  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, String schemaStr, long smallFileSize, boolean mergeAllowDuplicateInserts,
      Properties props) {
    HoodieWriteConfig.Builder builder = getConfigBuilder(schemaStr);
    return builder
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder()
                .compactionSmallFileSize(smallFileSize)
                // Set rollback to LAZY so no inflights are deleted
                .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
                .insertSplitSize(insertSplitSize).build())
        .withStorageConfig(
            HoodieStorageConfig.newBuilder()
                .hfileMaxFileSize(dataGen.getEstimatedFileSizeInBytes(200))
                .parquetMaxFileSize(dataGen.getEstimatedFileSizeInBytes(200)).build())
        .withMergeAllowDuplicateOnInserts(mergeAllowDuplicateInserts)
        .withProps(props)
        .build();
  }

  protected HoodieInstant createRequestedReplaceInstant(HoodieTableMetaClient metaClient, String clusterTime, List<FileSlice>[] fileSlices) throws IOException {
    HoodieClusteringPlan clusteringPlan =
        ClusteringUtils.createClusteringPlan(CLUSTERING_EXECUTION_STRATEGY_CLASS.defaultValue(), STRATEGY_PARAMS, fileSlices, Collections.emptyMap());

    HoodieInstant clusteringInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, clusterTime);
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setClusteringPlan(clusteringPlan).setOperationType(WriteOperationType.CLUSTER.name()).build();
    metaClient.getActiveTimeline().saveToPendingReplaceCommit(clusteringInstant, TimelineMetadataUtils.serializeRequestedReplaceMetadata(requestedReplaceMetadata));
    return clusteringInstant;
  }

  private HoodieWriteConfig getParallelWritingWriteConfig(HoodieFailedWritesCleaningPolicy cleaningPolicy) {
    return getConfigBuilder()
        .withEmbeddedTimelineServerEnabled(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withFailedWritesCleaningPolicy(cleaningPolicy)
            .withAutoClean(false).build())
        .withTimelineLayoutVersion(1)
        .withHeartbeatIntervalInMs(3 * 1000)
        .withAutoCommit(false).build();
  }

  public static class FailingPreCommitValidator<T extends HoodieRecordPayload, I, K, O extends JavaRDD<WriteStatus>> extends SparkPreCommitValidator<T, I, K, O> {

    public FailingPreCommitValidator(HoodieSparkTable table, HoodieEngineContext context, HoodieWriteConfig config) {
      super(table, context, config);
    }

    @Override
    protected void validateRecordsBeforeAndAfter(final Dataset<Row> before, final Dataset<Row> after, final Set<String> partitionsAffected) {
      throw new HoodieValidationException("simulate failure");
    }
  }

}
