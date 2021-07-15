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

package org.apache.hudi.table;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;
import org.apache.hudi.utils.factory.CollectSinkTableFactory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.utils.TestData.assertRowsContains;
import static org.apache.hudi.utils.TestData.assertRowsEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IT cases for Hoodie table source and sink.
 *
 * Note: should add more SQL cases when batch write is supported.
 */
public class HoodieDataSourceITCase extends AbstractTestBase {
  private TableEnvironment streamTableEnv;
  private TableEnvironment batchTableEnv;

  @BeforeEach
  void beforeEach() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    streamTableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
    streamTableEnv.getConfig().getConfiguration()
        .setString("execution.checkpointing.interval", "2s");

    settings = EnvironmentSettings.newInstance().inBatchMode().build();
    batchTableEnv = TableEnvironmentImpl.create(settings);
    batchTableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
  }

  @TempDir
  File tempFile;

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testStreamWriteAndRead(HoodieTableType tableType) throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.READ_AS_STREAMING.key(), "true");
    options.put(FlinkOptions.TABLE_TYPE.key(), tableType.name());
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> rows = execSelectSql(streamTableEnv, "select * from t1", 10);
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);

    // insert another batch of data
    execInsertSql(streamTableEnv, insertInto);
    List<Row> rows2 = execSelectSql(streamTableEnv, "select * from t1", 10);
    assertRowsEquals(rows2, TestData.DATA_SET_SOURCE_INSERT);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testStreamReadAppendData(HoodieTableType tableType) throws Exception {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    String createSource2 = TestConfigurations.getFileSourceDDL("source2", "test_source_2.data");
    streamTableEnv.executeSql(createSource);
    streamTableEnv.executeSql(createSource2);

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.READ_AS_STREAMING.key(), "true");
    options.put(FlinkOptions.TABLE_TYPE.key(), tableType.name());
    String createHoodieTable = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(createHoodieTable);
    String insertInto = "insert into t1 select * from source";
    // execute 2 times
    execInsertSql(streamTableEnv, insertInto);
    // remember the commit
    String specifiedCommit = TestUtils.getFirstCommit(tempFile.getAbsolutePath());
    // another update batch
    String insertInto2 = "insert into t1 select * from source2";
    execInsertSql(streamTableEnv, insertInto2);
    // now we consume starting from the oldest commit
    options.put(FlinkOptions.READ_STREAMING_START_COMMIT.key(), specifiedCommit);
    String createHoodieTable2 = TestConfigurations.getCreateHoodieTableDDL("t2", options);
    streamTableEnv.executeSql(createHoodieTable2);
    List<Row> rows = execSelectSql(streamTableEnv, "select * from t2", 10);
    // all the data with same keys are appended within one data bucket and one log file,
    // so when consume, the same keys are merged
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_MERGED);
  }

  @Test
  void testStreamWriteBatchRead() {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  @Test
  void testStreamWriteBatchReadOptimized() {
    // create filesystem table named source
    String createSource = TestConfigurations.getFileSourceDDL("source");
    streamTableEnv.executeSql(createSource);

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    // read optimized is supported for both MOR and COR table,
    // test MOR streaming write with compaction then reads as
    // query type 'read_optimized'.
    options.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    options.put(FlinkOptions.QUERY_TYPE.key(), FlinkOptions.QUERY_TYPE_READ_OPTIMIZED);
    options.put(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), "1");
    options.put(FlinkOptions.COMPACTION_TASKS.key(), "1");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    List<Row> rows = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(rows, TestData.DATA_SET_SOURCE_INSERT);
  }

  @Test
  void testStreamWriteWithCleaning() {
    // create filesystem table named source

    // the source generates 4 commits but the cleaning task
    // would always try to keep the remaining commits number as 1
    String createSource = TestConfigurations.getFileSourceDDL(
        "source", "test_source_3.data", 4);
    streamTableEnv.executeSql(createSource);

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.CLEAN_RETAIN_COMMITS.key(), "1"); // only keep 1 commits
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 select * from source";
    execInsertSql(streamTableEnv, insertInto);

    Configuration defaultConf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    Map<String, String> options1 = new HashMap<>(defaultConf.toMap());
    options1.put(FlinkOptions.TABLE_NAME.key(), "t1");
    Configuration conf = Configuration.fromMap(options1);
    HoodieTimeline timeline = StreamerUtil.createWriteClient(conf, null)
        .getHoodieTable().getActiveTimeline();
    assertTrue(timeline.filterCompletedInstants()
            .getInstants().anyMatch(instant -> instant.getAction().equals("clean")),
        "some commits should be cleaned");
  }

  @Test
  void testStreamReadWithDeletes() throws Exception {
    // create filesystem table named source

    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(FlinkOptions.TABLE_NAME, "t1");
    conf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);

    // write one commit
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    // write another commit with deletes
    TestData.writeData(TestData.DATA_SET_UPDATE_DELETE, conf);

    String latestCommit = StreamerUtil.createWriteClient(conf, null)
        .getLastCompletedInstant(HoodieTableType.MERGE_ON_READ);

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    options.put(FlinkOptions.READ_AS_STREAMING.key(), "true");
    options.put(FlinkOptions.READ_STREAMING_CHECK_INTERVAL.key(), "2");
    options.put(FlinkOptions.READ_STREAMING_START_COMMIT.key(), latestCommit);
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(hoodieTableDDL);

    List<Row> result = execSelectSql(streamTableEnv, "select * from t1", 10);
    final String expected = "["
        + "id1,Danny,24,1970-01-01T00:00:00.001,par1, "
        + "id2,Stephen,34,1970-01-01T00:00:00.002,par1, "
        + "id3,null,null,null,null, "
        + "id5,null,null,null,null, "
        + "id9,null,null,null,null]";
    assertRowsEquals(result, expected);
  }

  @ParameterizedTest
  @MethodSource("configParams")
  void testWriteAndRead(ExecMode execMode, boolean hiveStylePartitioning) {
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    if (hiveStylePartitioning) {
      options.put(FlinkOptions.HIVE_STYLE_PARTITIONING.key(), "true");
    }
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),\n"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
        + "('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),\n"
        + "('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),\n"
        + "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),\n"
        + "('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),\n"
        + "('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),\n"
        + "('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4')";

    execInsertSql(tableEnv, insertInto);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT);
    // apply filters
    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1 where uuid > 'id5'").execute().collect());
    assertRowsEquals(result2, "["
        + "id6,Emma,20,1970-01-01T00:00:06,par3, "
        + "id7,Bob,44,1970-01-01T00:00:07,par4, "
        + "id8,Han,56,1970-01-01T00:00:08,par4]");
  }

  @ParameterizedTest
  @EnumSource(value = ExecMode.class)
  void testWriteAndReadWithInsertMode(ExecMode execMode) throws Exception {
    boolean streaming = execMode == ExecMode.STREAM;
    String hoodieTableDDL = "create table t1(\n"
        + "  uuid varchar(20),\n"
        + "  name varchar(10),\n"
        + "  age int,\n"
        + "  `partition` varchar(20),\n" // test streaming read with partition field in the middle
        + "  ts timestamp(3),\n"
        + "  PRIMARY KEY(uuid) NOT ENFORCED\n"
        + ")\n"
        + "PARTITIONED BY (`partition`)\n"
        + "with (\n"
        + "  'connector' = 'hudi',\n"
        + "  'write.operation' = 'insert',\n"
        + "  'path' = '" + tempFile.getAbsolutePath() + "',\n"
        + "  'read.streaming.enabled' = '" + streaming + "'\n"
        + ")";
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 values\n"
        + "('id1','Danny',23,'par1',TIMESTAMP '1970-01-01 00:00:01'),\n"
        + "('id2','Stephen',33,'par1',TIMESTAMP '1970-01-01 00:00:02'),\n"
        + "('id3','Julian',53,'par2',TIMESTAMP '1970-01-01 00:00:03'),\n"
        + "('id4','Fabian',31,'par2',TIMESTAMP '1970-01-01 00:00:04'),\n"
        + "('id5','Sophia',18,'par3',TIMESTAMP '1970-01-01 00:00:05'),\n"
        + "('id6','Emma',20,'par3',TIMESTAMP '1970-01-01 00:00:06'),\n"
        + "('id7','Bob',44,'par4',TIMESTAMP '1970-01-01 00:00:07'),\n"
        + "('id8','Han',56,'par4',TIMESTAMP '1970-01-01 00:00:08')";
    execInsertSql(streamTableEnv, insertInto);

    final String expected = "["
        + "id1,Danny,23,par1,1970-01-01T00:00:01, "
        + "id2,Stephen,33,par1,1970-01-01T00:00:02, "
        + "id3,Julian,53,par2,1970-01-01T00:00:03, "
        + "id4,Fabian,31,par2,1970-01-01T00:00:04, "
        + "id5,Sophia,18,par3,1970-01-01T00:00:05, "
        + "id6,Emma,20,par3,1970-01-01T00:00:06, "
        + "id7,Bob,44,par4,1970-01-01T00:00:07, "
        + "id8,Han,56,par4,1970-01-01T00:00:08]";

    List<Row> result = execSelectSql(streamTableEnv, "select * from t1", execMode);

    assertRowsEquals(result, expected);

    // insert another batch of data
    execInsertSql(streamTableEnv, insertInto);
    List<Row> result2 = execSelectSql(streamTableEnv, "select * from t1", execMode);

    final List<String> contains = new ArrayList<>();
    contains.add("id1,Danny,23,par1,1970-01-01T00:00:01");
    contains.add("id2,Stephen,33,par1,1970-01-01T00:00:02");
    contains.add("id3,Julian,53,par2,1970-01-01T00:00:03");
    contains.add("id4,Fabian,31,par2,1970-01-01T00:00:04");
    contains.add("id5,Sophia,18,par3,1970-01-01T00:00:05");
    contains.add("id6,Emma,20,par3,1970-01-01T00:00:06");
    contains.add("id7,Bob,44,par4,1970-01-01T00:00:07");
    contains.add("id8,Han,56,par4,1970-01-01T00:00:08");

    assertRowsContains(result2, contains, 0);
  }

  @ParameterizedTest
  @EnumSource(value = ExecMode.class)
  void testWriteAndReadParMiddle(ExecMode execMode) throws Exception {
    boolean streaming = execMode == ExecMode.STREAM;
    String hoodieTableDDL = "create table t1(\n"
            + "  uuid varchar(20),\n"
            + "  name varchar(10),\n"
            + "  age int,\n"
            + "  `partition` varchar(20),\n" // test streaming read with partition field in the middle
            + "  ts timestamp(3),\n"
            + "  PRIMARY KEY(uuid) NOT ENFORCED\n"
            + ")\n"
            + "PARTITIONED BY (`partition`)\n"
            + "with (\n"
            + "  'connector' = 'hudi',\n"
            + "  'path' = '" + tempFile.getAbsolutePath() + "',\n"
            + "  'read.streaming.enabled' = '" + streaming + "'\n"
            + ")";
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into t1 values\n"
            + "('id1','Danny',23,'par1',TIMESTAMP '1970-01-01 00:00:01'),\n"
            + "('id2','Stephen',33,'par1',TIMESTAMP '1970-01-01 00:00:02'),\n"
            + "('id3','Julian',53,'par2',TIMESTAMP '1970-01-01 00:00:03'),\n"
            + "('id4','Fabian',31,'par2',TIMESTAMP '1970-01-01 00:00:04'),\n"
            + "('id5','Sophia',18,'par3',TIMESTAMP '1970-01-01 00:00:05'),\n"
            + "('id6','Emma',20,'par3',TIMESTAMP '1970-01-01 00:00:06'),\n"
            + "('id7','Bob',44,'par4',TIMESTAMP '1970-01-01 00:00:07'),\n"
            + "('id8','Han',56,'par4',TIMESTAMP '1970-01-01 00:00:08')";
    execInsertSql(streamTableEnv, insertInto);

    final String expected = "["
            + "id1,Danny,23,par1,1970-01-01T00:00:01, "
            + "id2,Stephen,33,par1,1970-01-01T00:00:02, "
            + "id3,Julian,53,par2,1970-01-01T00:00:03, "
            + "id4,Fabian,31,par2,1970-01-01T00:00:04, "
            + "id5,Sophia,18,par3,1970-01-01T00:00:05, "
            + "id6,Emma,20,par3,1970-01-01T00:00:06, "
            + "id7,Bob,44,par4,1970-01-01T00:00:07, "
            + "id8,Han,56,par4,1970-01-01T00:00:08]";

    List<Row> result = execSelectSql(streamTableEnv, "select * from t1", execMode);

    assertRowsEquals(result, expected);

    // insert another batch of data
    execInsertSql(streamTableEnv, insertInto);
    List<Row> result2 = execSelectSql(streamTableEnv, "select * from t1", execMode);
    assertRowsEquals(result2, expected);
  }

  @ParameterizedTest
  @EnumSource(value = ExecMode.class)
  void testInsertOverwrite(ExecMode execMode) {
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);

    final String insertInto1 = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),\n"
        + "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
        + "('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),\n"
        + "('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),\n"
        + "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),\n"
        + "('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),\n"
        + "('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),\n"
        + "('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4')";

    execInsertSql(tableEnv, insertInto1);

    // overwrite partition 'par1' and increase in age by 1
    final String insertInto2 = "insert overwrite t1 partition(`partition`='par1') values\n"
        + "('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01'),\n"
        + "('id2','Stephen',34,TIMESTAMP '1970-01-01 00:00:02')\n";

    execInsertSql(tableEnv, insertInto2);

    List<Row> result1 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result1, TestData.DATA_SET_SOURCE_INSERT_OVERWRITE);

    // overwrite the whole table
    final String insertInto3 = "insert overwrite t1 values\n"
        + "('id1','Danny',24,TIMESTAMP '1970-01-01 00:00:01', 'par1'),\n"
        + "('id2','Stephen',34,TIMESTAMP '1970-01-01 00:00:02', 'par2')\n";

    execInsertSql(tableEnv, insertInto3);

    List<Row> result2 = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "id1,Danny,24,1970-01-01T00:00:01,par1, "
        + "id2,Stephen,34,1970-01-01T00:00:02,par2]";
    assertRowsEquals(result2, expected);
  }

  @ParameterizedTest
  @EnumSource(value = ExecMode.class)
  void testUpsertWithMiniBatches(ExecMode execMode) {
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.WRITE_BATCH_SIZE.key(), "0.001");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    tableEnv.executeSql(hoodieTableDDL);

    final String insertInto1 = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1')";

    execInsertSql(tableEnv, insertInto1);

    final String insertInto2 = "insert into t1 values\n"
        + "('id1','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n"
        + "('id1','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par1'),\n"
        + "('id1','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par1'),\n"
        + "('id1','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par1')";

    execInsertSql(tableEnv, insertInto2);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, "[id1,Sophia,18,1970-01-01T00:00:05,par1]");
  }

  @ParameterizedTest
  @EnumSource(value = ExecMode.class)
  void testWriteNonPartitionedTable(ExecMode execMode) {
    TableEnvironment tableEnv = execMode == ExecMode.BATCH ? batchTableEnv : streamTableEnv;
    String hoodieTableDDL = "create table t1(\n"
        + "  uuid varchar(20),\n"
        + "  name varchar(10),\n"
        + "  age int,\n"
        + "  ts timestamp(3),\n"
        + "  `partition` varchar(20),\n"
        + "  PRIMARY KEY(uuid) NOT ENFORCED\n"
        + ")\n"
        + "with (\n"
        + "  'connector' = 'hudi',\n"
        + "  'path' = '" + tempFile.getAbsolutePath() + "'\n"
        + ")";
    tableEnv.executeSql(hoodieTableDDL);

    final String insertInto1 = "insert into t1 values\n"
        + "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1')";

    execInsertSql(tableEnv, insertInto1);

    final String insertInto2 = "insert into t1 values\n"
        + "('id1','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par2'),\n"
        + "('id1','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par3'),\n"
        + "('id1','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par4'),\n"
        + "('id1','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par5')";

    execInsertSql(tableEnv, insertInto2);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, "[id1,Sophia,18,1970-01-01T00:00:05,par5]");
  }

  @Test
  void testWriteGlobalIndex() {
    // the source generates 4 commits
    String createSource = TestConfigurations.getFileSourceDDL(
        "source", "test_source_4.data", 4);
    streamTableEnv.executeSql(createSource);

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.INDEX_GLOBAL_ENABLED.key(), "true");
    options.put(FlinkOptions.INSERT_DROP_DUPS.key(), "true");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(hoodieTableDDL);

    final String insertInto2 = "insert into t1 select * from source";

    execInsertSql(streamTableEnv, insertInto2);

    List<Row> result = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    assertRowsEquals(result, "[id1,Phoebe,52,1970-01-01T00:00:08,par4]");
  }

  @Test
  void testWriteLocalIndex() {
    // the source generates 4 commits
    String createSource = TestConfigurations.getFileSourceDDL(
        "source", "test_source_4.data", 4);
    streamTableEnv.executeSql(createSource);

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.INDEX_GLOBAL_ENABLED.key(), "false");
    options.put(FlinkOptions.INSERT_DROP_DUPS.key(), "true");
    String hoodieTableDDL = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(hoodieTableDDL);

    final String insertInto2 = "insert into t1 select * from source";

    execInsertSql(streamTableEnv, insertInto2);

    List<Row> result = CollectionUtil.iterableToList(
        () -> streamTableEnv.sqlQuery("select * from t1").execute().collect());
    final String expected = "["
        + "id1,Stephen,34,1970-01-01T00:00:02,par1, "
        + "id1,Fabian,32,1970-01-01T00:00:04,par2, "
        + "id1,Jane,19,1970-01-01T00:00:06,par3, "
        + "id1,Phoebe,52,1970-01-01T00:00:08,par4]";
    assertRowsEquals(result, expected, 3);
  }

  @Test
  void testStreamReadEmptyTablePath() throws Exception {
    // create an empty table
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    StreamerUtil.initTableIfNotExists(conf);

    // create a flink source table
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.READ_AS_STREAMING.key(), "true");
    options.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    String createHoodieTable = TestConfigurations.getCreateHoodieTableDDL("t1", options);
    streamTableEnv.executeSql(createHoodieTable);

    // execute query and assert throws exception
    assertThrows(HoodieException.class, () -> execSelectSql(streamTableEnv, "select * from t1", 10),
            "No successful commits under path " + tempFile.getAbsolutePath());

  }

  @ParameterizedTest
  @EnumSource(value = ExecMode.class)
  void testWriteAndReadDebeziumJson(ExecMode execMode) throws Exception {
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("debezium_json.data")).toString();
    String sourceDDL = ""
        + "CREATE TABLE debezium_source(\n"
        + "  id INT NOT NULL,\n"
        + "  ts BIGINT,\n"
        + "  name STRING,\n"
        + "  description STRING,\n"
        + "  weight DOUBLE\n"
        + ") WITH (\n"
        + "  'connector' = 'filesystem',\n"
        + "  'path' = '" + sourcePath + "',\n"
        + "  'format' = 'debezium-json'\n"
        + ")";
    streamTableEnv.executeSql(sourceDDL);
    String hoodieTableDDL = ""
        + "CREATE TABLE hoodie_sink(\n"
        + "  id INT NOT NULL,\n"
        + "  ts BIGINT,\n"
        + "  name STRING,"
        + "  weight DOUBLE,"
        + "  PRIMARY KEY (id) NOT ENFORCED"
        + ") with (\n"
        + "  'connector' = 'hudi',\n"
        + "  'path' = '" + tempFile.getAbsolutePath() + "',\n"
        + "  'read.streaming.enabled' = '" + (execMode == ExecMode.STREAM) + "',\n"
        + "  'write.insert.drop.duplicates' = 'true'"
        + ")";
    streamTableEnv.executeSql(hoodieTableDDL);
    String insertInto = "insert into hoodie_sink select id, ts, name, weight from debezium_source";
    execInsertSql(streamTableEnv, insertInto);

    final String expected = "["
        + "101,1000,scooter,3.140000104904175, "
        + "102,2000,car battery,8.100000381469727, "
        + "103,3000,12-pack drill bits,0.800000011920929, "
        + "104,4000,hammer,0.75, "
        + "105,5000,hammer,0.875, "
        + "106,10000,hammer,1.0, "
        + "107,11000,rocks,5.099999904632568, "
        + "108,8000,jacket,0.10000000149011612, "
        + "109,9000,spare tire,22.200000762939453, "
        + "110,14000,jacket,0.5]";

    List<Row> result = execSelectSql(streamTableEnv, "select * from hoodie_sink", execMode);

    assertRowsEquals(result, expected);
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------
  private enum ExecMode {
    BATCH, STREAM
  }

  /**
   * Return test params => (execution mode, hive style partitioning).
   */
  private static Stream<Arguments> configParams() {
    Object[][] data =
        new Object[][] {
            {ExecMode.BATCH, false},
            {ExecMode.BATCH, true},
            {ExecMode.STREAM, false},
            {ExecMode.STREAM, true}};
    return Stream.of(data).map(Arguments::of);
  }

  private void execInsertSql(TableEnvironment tEnv, String insert) {
    TableResult tableResult = tEnv.executeSql(insert);
    // wait to finish
    try {
      tableResult.getJobClient().get().getJobExecutionResult().get();
    } catch (InterruptedException | ExecutionException ex) {
      throw new RuntimeException(ex);
    }
  }

  private List<Row> execSelectSql(TableEnvironment tEnv, String select, ExecMode execMode)
          throws TableNotExistException, InterruptedException {
    final String[] splits = select.split(" ");
    final String tableName = splits[splits.length - 1];
    switch (execMode) {
      case STREAM:
        return execSelectSql(tEnv, select, 10, tableName);
      case BATCH:
        return CollectionUtil.iterableToList(
            () -> tEnv.sqlQuery("select * from " + tableName).execute().collect());
      default:
        throw new AssertionError();
    }
  }

  private List<Row> execSelectSql(TableEnvironment tEnv, String select, long timeout)
          throws InterruptedException, TableNotExistException {
    return execSelectSql(tEnv, select, timeout, null);
  }

  private List<Row> execSelectSql(TableEnvironment tEnv, String select, long timeout, String sourceTable)
          throws InterruptedException, TableNotExistException {
    final String sinkDDL;
    if (sourceTable != null) {
      // use the source table schema as the sink schema if the source table was specified, .
      ObjectPath objectPath = new ObjectPath(tEnv.getCurrentDatabase(), sourceTable);
      TableSchema schema = tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTable(objectPath).getSchema();
      sinkDDL = TestConfigurations.getCollectSinkDDL("sink", schema);
    } else {
      sinkDDL = TestConfigurations.getCollectSinkDDL("sink");
    }
    tEnv.executeSql(sinkDDL);
    TableResult tableResult = tEnv.executeSql("insert into sink " + select);
    // wait for the timeout then cancels the job
    TimeUnit.SECONDS.sleep(timeout);
    tableResult.getJobClient().ifPresent(JobClient::cancel);
    tEnv.executeSql("DROP TABLE IF EXISTS sink");
    return CollectSinkTableFactory.RESULT.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }
}
