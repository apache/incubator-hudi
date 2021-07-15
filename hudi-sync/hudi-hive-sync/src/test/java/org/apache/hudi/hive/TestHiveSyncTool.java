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

package org.apache.hudi.hive;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.NetworkTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.testutils.HiveTestUtil;
import org.apache.hudi.hive.util.ConfigUtils;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient.PartitionEvent;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient.PartitionEvent.PartitionEventType;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHiveSyncTool {

  private static Stream<Boolean> useJdbc() {
    return Stream.of(false, true);
  }

  private static Iterable<Object[]> useJdbcAndSchemaFromCommitMetadata() {
    return Arrays.asList(new Object[][] {{true, true}, {true, false}, {false, true}, {false, false}});
  }

  private static Iterable<Object[]> useJdbcAndSchemaFromCommitMetadataAndManagedTable() {
    return Arrays.asList(new Object[][] {{true, true, true}, {true, false, false}, {false, true, true}, {false, false, false}});
  }

  // (useJdbc, useSchemaFromCommitMetadata, syncAsDataSource)
  private static Iterable<Object[]> syncDataSourceTableParams() {
    return Arrays.asList(new Object[][] {{true, true, true}, {true, false, false}, {false, true, true}, {false, false, false}});
  }

  @BeforeEach
  public void setUp() throws Exception {
    HiveTestUtil.setUp();
  }

  @AfterEach
  public void teardown() throws Exception {
    HiveTestUtil.clear();
  }

  @AfterAll
  public static void cleanUpClass() {
    HiveTestUtil.shutdown();
  }

  @ParameterizedTest
  @MethodSource({"useJdbcAndSchemaFromCommitMetadata"})
  public void testBasicSync(boolean useJdbc, boolean useSchemaFromCommitMetadata) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 3;
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, useSchemaFromCommitMetadata);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist initially");
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    // we need renew the hiveclient after tool.syncHoodieTable(), because it will close hive
    // session, then lead to connection retry, we can see there is a exception at log.
    hiveClient =
            new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertTrue(hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).size(),
        hiveClient.getDataSchema().getColumns().size() + 1,
        "Hive Schema should match the table schema + partition field");
    assertEquals(5, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.hiveSyncConfig.tableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Adding of new partitions
    List<String> newPartition = Arrays.asList("2050/01/01");
    hiveClient.addPartitionsToTable(HiveTestUtil.hiveSyncConfig.tableName, Arrays.asList());
    assertEquals(5, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "No new partition should be added");
    hiveClient.addPartitionsToTable(HiveTestUtil.hiveSyncConfig.tableName, newPartition);
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "New partition should be added");

    // Update partitions
    hiveClient.updatePartitionsToTable(HiveTestUtil.hiveSyncConfig.tableName, Arrays.asList());
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "Partition count should remain the same");
    hiveClient.updatePartitionsToTable(HiveTestUtil.hiveSyncConfig.tableName, newPartition);
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "Partition count should remain the same");

    // Alter partitions
    // Manually change a hive partition location to check if the sync will detect
    // it and generate a partition update event for it.
    hiveClient.updateHiveSQL("ALTER TABLE `" + HiveTestUtil.hiveSyncConfig.tableName
        + "` PARTITION (`datestr`='2050-01-01') SET LOCATION '/some/new/location'");

    hiveClient = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    List<Partition> hivePartitions = hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName);
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.empty());
    //writtenPartitionsSince.add(newPartition.get(0));
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince);
    assertEquals(1, partitionEvents.size(), "There should be only one partition event");
    assertEquals(PartitionEventType.UPDATE, partitionEvents.iterator().next().eventType,
        "The one partition event must of type UPDATE");

    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    hiveClient =
            new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    // Sync should update the changed partition to correct path
    List<Partition> tablePartitions = hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName);
    assertEquals(6, tablePartitions.size(), "The one partition we wrote should be added to hive");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(HiveTestUtil.hiveSyncConfig.tableName).get(),
        "The last commit that was synced should be 100");
  }

  @ParameterizedTest
  @MethodSource({"syncDataSourceTableParams"})
  public void testSyncCOWTableWithProperties(boolean useJdbc,
                                             boolean useSchemaFromCommitMetadata,
                                             boolean syncAsDataSourceTable) throws Exception {
    HiveSyncConfig hiveSyncConfig = HiveTestUtil.hiveSyncConfig;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 3;
    Map<String, String> serdeProperties = new HashMap<String, String>() {
      {
        put("path", hiveSyncConfig.basePath);
      }
    };

    Map<String, String> tableProperties = new HashMap<String, String>() {
      {
        put("tp_0", "p0");
        put("tp_1", "p1");
      }
    };
    hiveSyncConfig.syncAsSparkDataSourceTable = syncAsDataSourceTable;
    hiveSyncConfig.useJdbc = useJdbc;
    hiveSyncConfig.serdeProperties = ConfigUtils.configToString(serdeProperties);
    hiveSyncConfig.tableProperties = ConfigUtils.configToString(tableProperties);
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, useSchemaFromCommitMetadata);

    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    SessionState.start(HiveTestUtil.getHiveConf());
    Driver hiveDriver = new org.apache.hadoop.hive.ql.Driver(HiveTestUtil.getHiveConf());
    String dbTableName = hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName;
    hiveDriver.run("SHOW TBLPROPERTIES " + dbTableName);
    List<String> results = new ArrayList<>();
    hiveDriver.getResults(results);

    String tblPropertiesWithoutDdlTime = String.join("\n",
        results.subList(0, results.size() - 1));

    String sparkTableProperties = getSparkTableProperties(syncAsDataSourceTable, useSchemaFromCommitMetadata);
    assertEquals(
        "EXTERNAL\tTRUE\n"
        + "last_commit_time_sync\t100\n"
        + sparkTableProperties
        + "tp_0\tp0\n"
        + "tp_1\tp1", tblPropertiesWithoutDdlTime);
    assertTrue(results.get(results.size() - 1).startsWith("transient_lastDdlTime"));

    results.clear();
    // validate serde properties
    hiveDriver.run("SHOW CREATE TABLE " + dbTableName);
    hiveDriver.getResults(results);
    String ddl = String.join("\n", results);
    assertTrue(ddl.contains("'path'='" + hiveSyncConfig.basePath + "'"));
    if (syncAsDataSourceTable) {
      assertTrue(ddl.contains("'" + ConfigUtils.IS_QUERY_AS_RO_TABLE + "'='false'"));
    }
  }

  private String getSparkTableProperties(boolean syncAsDataSourceTable, boolean useSchemaFromCommitMetadata) {
    if (syncAsDataSourceTable) {
      if (useSchemaFromCommitMetadata) {
        return  "spark.sql.sources.provider\thudi\n"
                + "spark.sql.sources.schema.numPartCols\t1\n"
                + "spark.sql.sources.schema.numParts\t1\n"
                + "spark.sql.sources.schema.part.0\t{\"type\":\"struct\",\"fields\":"
                + "[{\"name\":\"_hoodie_commit_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"_hoodie_commit_seqno\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"_hoodie_record_key\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"_hoodie_partition_path\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"_hoodie_file_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
                + "{\"name\":\"name\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},"
                + "{\"name\":\"favorite_number\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},"
                + "{\"name\":\"favorite_color\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},"
                + "{\"name\":\"datestr\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}\n"
                + "spark.sql.sources.schema.partCol.0\tdatestr\n";
      } else {
        return "spark.sql.sources.provider\thudi\n"
                + "spark.sql.sources.schema.numPartCols\t1\n"
                + "spark.sql.sources.schema.numParts\t1\n"
                + "spark.sql.sources.schema.part.0\t{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":"
                + "\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"favorite_number\",\"type\":\"integer\","
                + "\"nullable\":false,\"metadata\":{}},{\"name\":\"favorite_color\",\"type\":\"string\",\"nullable\":false,"
                + "\"metadata\":{}}]}\n"
                + "{\"name\":\"datestr\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}\n"
                + "spark.sql.sources.schema.partCol.0\tdatestr\n";
      }
    } else {
      return  "";
    }
  }

  @ParameterizedTest
  @MethodSource({"syncDataSourceTableParams"})
  public void testSyncMORTableWithProperties(boolean useJdbc,
                                             boolean useSchemaFromCommitMetadata,
                                             boolean syncAsDataSourceTable) throws Exception {
    HiveSyncConfig hiveSyncConfig = HiveTestUtil.hiveSyncConfig;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 3;
    Map<String, String> serdeProperties = new HashMap<String, String>() {
      {
        put("path", hiveSyncConfig.basePath);
      }
    };

    Map<String, String> tableProperties = new HashMap<String, String>() {
      {
        put("tp_0", "p0");
        put("tp_1", "p1");
      }
    };
    hiveSyncConfig.syncAsSparkDataSourceTable = syncAsDataSourceTable;
    hiveSyncConfig.useJdbc = useJdbc;
    hiveSyncConfig.serdeProperties = ConfigUtils.configToString(serdeProperties);
    hiveSyncConfig.tableProperties = ConfigUtils.configToString(tableProperties);
    String instantTime = "100";
    String deltaCommitTime = "101";
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true,
        useSchemaFromCommitMetadata);

    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    String roTableName = hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE;
    String rtTableName = hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;

    String[] tableNames = new String[] {roTableName, rtTableName};
    String[] readAsOptimizedResults = new String[] {"true", "false"};

    SessionState.start(HiveTestUtil.getHiveConf());
    Driver hiveDriver = new org.apache.hadoop.hive.ql.Driver(HiveTestUtil.getHiveConf());

    String sparkTableProperties = getSparkTableProperties(syncAsDataSourceTable, useSchemaFromCommitMetadata);
    for (int i = 0;i < 2; i++) {
      String dbTableName = hiveSyncConfig.databaseName + "." + tableNames[i];
      String readAsOptimized = readAsOptimizedResults[i];

      hiveDriver.run("SHOW TBLPROPERTIES " + dbTableName);
      List<String> results = new ArrayList<>();
      hiveDriver.getResults(results);

      String tblPropertiesWithoutDdlTime = String.join("\n",
          results.subList(0, results.size() - 1));
      assertEquals(
          "EXTERNAL\tTRUE\n"
          + "last_commit_time_sync\t101\n"
          + sparkTableProperties
          + "tp_0\tp0\n"
          + "tp_1\tp1", tblPropertiesWithoutDdlTime);
      assertTrue(results.get(results.size() - 1).startsWith("transient_lastDdlTime"));

      results.clear();
      // validate serde properties
      hiveDriver.run("SHOW CREATE TABLE " + dbTableName);
      hiveDriver.getResults(results);
      String ddl = String.join("\n", results);
      assertTrue(ddl.contains("'path'='" + hiveSyncConfig.basePath + "'"));
      assertTrue(ddl.toLowerCase().contains("create external table"));
      if (syncAsDataSourceTable) {
        assertTrue(ddl.contains("'" + ConfigUtils.IS_QUERY_AS_RO_TABLE + "'='" + readAsOptimized + "'"));
      }
    }
  }

  @ParameterizedTest
  @MethodSource({"useJdbcAndSchemaFromCommitMetadataAndManagedTable"})
  public void testSyncManagedTable(boolean useJdbc,
                                   boolean useSchemaFromCommitMetadata,
                                   boolean isManagedTable) throws Exception {
    HiveSyncConfig hiveSyncConfig = HiveTestUtil.hiveSyncConfig;

    hiveSyncConfig.useJdbc = useJdbc;
    hiveSyncConfig.createManagedTable = isManagedTable;
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, useSchemaFromCommitMetadata);

    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    SessionState.start(HiveTestUtil.getHiveConf());
    Driver hiveDriver = new org.apache.hadoop.hive.ql.Driver(HiveTestUtil.getHiveConf());
    String dbTableName = hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName;
    hiveDriver.run("SHOW TBLPROPERTIES " + dbTableName);

    List<String> results = new ArrayList<>();
    hiveDriver.run("SHOW CREATE TABLE " + dbTableName);
    hiveDriver.getResults(results);
    String ddl = String.join("\n", results).toLowerCase();
    if (isManagedTable) {
      assertTrue(ddl.contains("create table"));
    } else {
      assertTrue(ddl.contains("create external table"));
    }
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testSyncIncremental(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 2;
    String commitTime1 = "100";
    HiveTestUtil.createCOWTable(commitTime1, 5, true);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    assertEquals(5, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime1, hiveClient.getLastCommitTimeSynced(HiveTestUtil.hiveSyncConfig.tableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartitions(1, true, true, dateTime, commitTime2);

    // Lets do the sync
    hiveClient = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.of(commitTime1));
    assertEquals(1, writtenPartitionsSince.size(), "We should have one partition written after 100 commit");
    List<Partition> hivePartitions = hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName);
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince);
    assertEquals(1, partitionEvents.size(), "There should be only one partition event");
    assertEquals(PartitionEventType.ADD, partitionEvents.iterator().next().eventType, "The one partition event must of type ADD");

    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "The one partition we wrote should be added to hive");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.hiveSyncConfig.tableName).get(),
        "The last commit that was synced should be 101");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testSyncIncrementalWithSchemaEvolution(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 2;
    String commitTime1 = "100";
    HiveTestUtil.createCOWTable(commitTime1, 5, true);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    int fields = hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).size();

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartitions(1, false, true, dateTime, commitTime2);

    // Lets do the sync
    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertEquals(fields + 3, hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "Hive Schema has evolved and should not be 3 more field");
    assertEquals("BIGINT", hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).get("favorite_number"),
        "Hive Schema has evolved - Field favorite_number has evolved from int to long");
    assertTrue(hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).containsKey("favorite_movie"),
        "Hive Schema has evolved - Field favorite_movie was added");

    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),
        "The one partition we wrote should be added to hive");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(HiveTestUtil.hiveSyncConfig.tableName).get(),
        "The last commit that was synced should be 101");
  }

  @ParameterizedTest
  @MethodSource("useJdbcAndSchemaFromCommitMetadata")
  public void testSyncMergeOnRead(boolean useJdbc, boolean useSchemaFromCommitMetadata) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 2;
    String instantTime = "100";
    String deltaCommitTime = "101";
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true,
        useSchemaFromCommitMetadata);

    String roTableName = HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE;
    HoodieHiveClient hiveClient = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(hiveClient.doesTableExist(roTableName), "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist initially");
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertTrue(hiveClient.doesTableExist(roTableName), "Table " + roTableName + " should exist after sync completes");

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size(),
          "Hive Schema should match the table schema + partition field");
    }

    assertEquals(5, hiveClient.scanTablePartitions(roTableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(deltaCommitTime, hiveClient.getLastCommitTimeSynced(roTableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addCOWPartitions(1, true, useSchemaFromCommitMetadata, dateTime, commitTime2);
    HiveTestUtil.addMORPartitions(1, true, false,
        useSchemaFromCommitMetadata, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    hiveClient = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the evolved table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size(),
          "Hive Schema should match the evolved table schema + partition field");
    }
    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(roTableName).size(),
        "The 2 partitions we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(roTableName).get(),
        "The last commit that was synced should be 103");
  }

  @ParameterizedTest
  @MethodSource("useJdbcAndSchemaFromCommitMetadata")
  public void testSyncMergeOnReadRT(boolean useJdbc, boolean useSchemaFromCommitMetadata) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 2;
    String instantTime = "100";
    String deltaCommitTime = "101";
    String snapshotTableName = HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true, useSchemaFromCommitMetadata);
    HoodieHiveClient hiveClientRT =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    assertFalse(hiveClientRT.doesTableExist(snapshotTableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should not exist initially");

    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertTrue(hiveClientRT.doesTableExist(snapshotTableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should exist after sync completes");

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size(),
          "Hive Schema should match the table schema + partition field");
    }

    assertEquals(5, hiveClientRT.scanTablePartitions(snapshotTableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(deltaCommitTime, hiveClientRT.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addCOWPartitions(1, true, useSchemaFromCommitMetadata, dateTime, commitTime2);
    HiveTestUtil.addMORPartitions(1, true, false, useSchemaFromCommitMetadata, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    hiveClientRT = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the evolved table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size(),
          "Hive Schema should match the evolved table schema + partition field");
    }
    // Sync should add the one partition
    assertEquals(6, hiveClientRT.scanTablePartitions(snapshotTableName).size(),
        "The 2 partitions we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClientRT.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be 103");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testMultiPartitionKeySync(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 2;
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, true);

    HiveSyncConfig hiveSyncConfig = HiveSyncConfig.copy(HiveTestUtil.hiveSyncConfig);
    hiveSyncConfig.partitionValueExtractorClass = MultiPartKeysValueExtractor.class.getCanonicalName();
    hiveSyncConfig.tableName = "multi_part_key";
    hiveSyncConfig.partitionFields = Arrays.asList("year", "month", "day");
    HiveTestUtil.getCreatedTablesSet().add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);

    HoodieHiveClient hiveClient = new HoodieHiveClient(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(hiveClient.doesTableExist(hiveSyncConfig.tableName),
        "Table " + hiveSyncConfig.tableName + " should not exist initially");
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    assertTrue(hiveClient.doesTableExist(hiveSyncConfig.tableName),
        "Table " + hiveSyncConfig.tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(hiveSyncConfig.tableName).size(),
        hiveClient.getDataSchema().getColumns().size() + 3,
        "Hive Schema should match the table schema + partition fields");
    assertEquals(5, hiveClient.scanTablePartitions(hiveSyncConfig.tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(hiveSyncConfig.tableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // HoodieHiveClient had a bug where partition vals were sorted
    // and stored as keys in a map. The following tests this particular case.
    // Now lets create partition "2010/01/02" and followed by "2010/02/01".
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartition("2010/01/02", true, true, commitTime2);

    hiveClient = new HoodieHiveClient(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.of(instantTime));
    assertEquals(1, writtenPartitionsSince.size(), "We should have one partition written after 100 commit");
    List<Partition> hivePartitions = hiveClient.scanTablePartitions(hiveSyncConfig.tableName);
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince);
    assertEquals(1, partitionEvents.size(), "There should be only one partition event");
    assertEquals(PartitionEventType.ADD, partitionEvents.iterator().next().eventType, "The one partition event must of type ADD");

    tool = new HiveSyncTool(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(hiveSyncConfig.tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(hiveSyncConfig.tableName).get(),
        "The last commit that was synced should be 101");

    // create partition "2010/02/01" and ensure sync works
    String commitTime3 = "102";
    HiveTestUtil.addCOWPartition("2010/02/01", true, true, commitTime3);
    HiveTestUtil.getCreatedTablesSet().add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);

    hiveClient = new HoodieHiveClient(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    tool = new HiveSyncTool(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertTrue(hiveClient.doesTableExist(hiveSyncConfig.tableName),
        "Table " + hiveSyncConfig.tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(hiveSyncConfig.tableName).size(),
        hiveClient.getDataSchema().getColumns().size() + 3,
        "Hive Schema should match the table schema + partition fields");
    assertEquals(7, hiveClient.scanTablePartitions(hiveSyncConfig.tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime3, hiveClient.getLastCommitTimeSynced(hiveSyncConfig.tableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");
    assertEquals(1, hiveClient.getPartitionsWrittenToSince(Option.of(commitTime2)).size());
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testNonPartitionedSync(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 2;
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, true);

    HiveSyncConfig hiveSyncConfig = HiveSyncConfig.copy(HiveTestUtil.hiveSyncConfig);
    // Set partition value extractor to NonPartitionedExtractor
    hiveSyncConfig.partitionValueExtractorClass = NonPartitionedExtractor.class.getCanonicalName();
    hiveSyncConfig.tableName = "non_partitioned";
    hiveSyncConfig.partitionFields = Arrays.asList("year", "month", "day");
    HiveTestUtil.getCreatedTablesSet().add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);

    HoodieHiveClient hiveClient = new HoodieHiveClient(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(hiveClient.doesTableExist(hiveSyncConfig.tableName),
        "Table " + hiveSyncConfig.tableName + " should not exist initially");
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    assertTrue(hiveClient.doesTableExist(hiveSyncConfig.tableName),
        "Table " + hiveSyncConfig.tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(hiveSyncConfig.tableName).size(),
        hiveClient.getDataSchema().getColumns().size(),
        "Hive Schema should match the table schema，ignoring the partition fields");
    assertEquals(0, hiveClient.scanTablePartitions(hiveSyncConfig.tableName).size(),
        "Table should not have partitions because of the NonPartitionedExtractor");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testReadSchemaForMOR(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 2;
    String commitTime = "100";
    String snapshotTableName = HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    HiveTestUtil.createMORTable(commitTime, "", 5, false, true);
    HoodieHiveClient hiveClientRT =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    assertFalse(hiveClientRT.doesTableExist(snapshotTableName), "Table " + HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
        + " should not exist initially");

    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertTrue(hiveClientRT.doesTableExist(snapshotTableName), "Table " + HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
        + " should exist after sync completes");

    // Schema being read from compacted base files
    assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
        SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
            + HoodieRecord.HOODIE_META_COLUMNS.size(),
        "Hive Schema should match the table schema + partition field");
    assertEquals(5, hiveClientRT.scanTablePartitions(snapshotTableName).size(), "Table partitions should match the number of partitions we wrote");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addMORPartitions(1, true, false, true, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    hiveClientRT = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    // Schema being read from the log files
    assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
        SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
            + HoodieRecord.HOODIE_META_COLUMNS.size(),
        "Hive Schema should match the evolved table schema + partition field");
    // Sync should add the one partition
    assertEquals(6, hiveClientRT.scanTablePartitions(snapshotTableName).size(), "The 1 partition we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClientRT.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be 103");
  }

  @Test
  public void testConnectExceptionIgnoreConfigSet() throws IOException, URISyntaxException {
    HiveTestUtil.hiveSyncConfig.useJdbc = true;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 2;
    String instantTime = "100";
    HiveTestUtil.createCOWTable(instantTime, 5, false);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist initially");
    // Lets do the sync

    HiveSyncConfig syncToolConfig = HiveSyncConfig.copy(HiveTestUtil.hiveSyncConfig);
    syncToolConfig.ignoreExceptions = true;
    syncToolConfig.jdbcUrl = HiveTestUtil.hiveSyncConfig.jdbcUrl
        .replace(String.valueOf(HiveTestUtil.hiveTestService.getHiveServerPort()), String.valueOf(NetworkTestUtils.nextFreePort()));
    HiveSyncTool tool = new HiveSyncTool(syncToolConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertFalse(hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist initially");
  }

  private void verifyOldParquetFileTest(HoodieHiveClient hiveClient, String emptyCommitTime) throws Exception {
    assertTrue(hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName), "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).size(),
        hiveClient.getDataSchema().getColumns().size() + 1,
        "Hive Schema should match the table schema + partition field");
    assertEquals(1, hiveClient.scanTablePartitions(HiveTestUtil.hiveSyncConfig.tableName).size(),"Table partitions should match the number of partitions we wrote");
    assertEquals(emptyCommitTime,
        hiveClient.getLastCommitTimeSynced(HiveTestUtil.hiveSyncConfig.tableName).get(),"The last commit that was sycned should be updated in the TBLPROPERTIES");

    // make sure correct schema is picked
    Schema schema = SchemaTestUtil.getSimpleSchema();
    for (Field field : schema.getFields()) {
      assertEquals(field.schema().getType().getName(),
          hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).get(field.name()).toLowerCase(),
          String.format("Hive Schema Field %s was added", field));
    }
    assertEquals("string",
        hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).get("datestr").toLowerCase(), "Hive Schema Field datestr was added");
    assertEquals(schema.getFields().size() + 1 + HoodieRecord.HOODIE_META_COLUMNS.size(),
        hiveClient.getTableSchema(HiveTestUtil.hiveSyncConfig.tableName).size(),"Hive Schema fields size");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testPickingOlderParquetFileIfLatestIsEmptyCommit(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 2;
    final String commitTime = "100";
    HiveTestUtil.createCOWTable(commitTime, 1, true);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    // create empty commit
    final String emptyCommitTime = "200";
    HiveTestUtil.createCommitFileWithSchema(commitMetadata, emptyCommitTime, true);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName),"Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist initially");

    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    verifyOldParquetFileTest(hiveClient, emptyCommitTime);
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testNotPickingOlderParquetFileWhenLatestCommitReadFails(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 2;
    final String commitTime = "100";
    HiveTestUtil.createCOWTable(commitTime, 1, true);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();

    // evolve the schema
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "101";
    HiveTestUtil.addCOWPartitions(1, false, true, dateTime, commitTime2);

    // create empty commit
    final String emptyCommitTime = "200";
    HiveTestUtil.createCommitFile(commitMetadata, emptyCommitTime);

    HoodieHiveClient hiveClient =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(
        hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName),"Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist initially");

    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    // now delete the evolved commit instant
    Path fullPath = new Path(HiveTestUtil.hiveSyncConfig.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + hiveClient.getActiveTimeline().getInstants()
        .filter(inst -> inst.getTimestamp().equals(commitTime2))
        .findFirst().get().getFileName());
    assertTrue(HiveTestUtil.fileSystem.delete(fullPath, false));

    try {
      tool.syncHoodieTable();
    } catch (RuntimeException e) {
      // we expect the table sync to fail
    }

    // table should not be synced yet
    assertFalse(
        hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName),"Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist at all");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testNotPickingOlderParquetFileWhenLatestCommitReadFailsForExistingTable(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 2;
    final String commitTime = "100";
    HiveTestUtil.createCOWTable(commitTime, 1, true);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    // create empty commit
    final String emptyCommitTime = "200";
    HiveTestUtil.createCommitFileWithSchema(commitMetadata, emptyCommitTime, true);
    //HiveTestUtil.createCommitFile(commitMetadata, emptyCommitTime);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(
        hiveClient.doesTableExist(HiveTestUtil.hiveSyncConfig.tableName), "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist initially");

    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    verifyOldParquetFileTest(hiveClient, emptyCommitTime);

    // evolve the schema
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "301";
    HiveTestUtil.addCOWPartitions(1, false, true, dateTime, commitTime2);
    //HiveTestUtil.createCommitFileWithSchema(commitMetadata, "400", false); // create another empty commit
    //HiveTestUtil.createCommitFile(commitMetadata, "400"); // create another empty commit

    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    HoodieHiveClient hiveClientLatest = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    // now delete the evolved commit instant
    Path fullPath = new Path(HiveTestUtil.hiveSyncConfig.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + hiveClientLatest.getActiveTimeline().getInstants()
            .filter(inst -> inst.getTimestamp().equals(commitTime2))
            .findFirst().get().getFileName());
    assertTrue(HiveTestUtil.fileSystem.delete(fullPath, false));
    try {
      tool.syncHoodieTable();
    } catch (RuntimeException e) {
      // we expect the table sync to fail
    }

    // old sync values should be left intact
    verifyOldParquetFileTest(hiveClient, emptyCommitTime);
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testTypeConverter(boolean useJdbc) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    HiveTestUtil.hiveSyncConfig.batchSyncNum = 2;
    HiveTestUtil.createCOWTable("100", 5, true);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    String tableName = HiveTestUtil.hiveSyncConfig.tableName;
    String tableAbsoluteName = String.format(" `%s.%s` ", HiveTestUtil.hiveSyncConfig.databaseName, tableName);
    String dropTableSql = String.format("DROP TABLE IF EXISTS %s ", tableAbsoluteName);
    String createTableSqlPrefix = String.format("CREATE TABLE IF NOT EXISTS %s ", tableAbsoluteName);
    String errorMsg = "An error occurred in decimal type converting.";
    hiveClient.updateHiveSQL(dropTableSql);

    // test one column in DECIMAL
    String oneTargetColumnSql = createTableSqlPrefix + "(`decimal_col` DECIMAL(9,8), `bigint_col` BIGINT)";
    hiveClient.updateHiveSQL(oneTargetColumnSql);
    System.out.println(hiveClient.getTableSchema(tableName));
    assertTrue(hiveClient.getTableSchema(tableName).containsValue("DECIMAL(9,8)"), errorMsg);
    hiveClient.updateHiveSQL(dropTableSql);

    // test multiple columns in DECIMAL
    String multipleTargetColumnSql =
        createTableSqlPrefix + "(`decimal_col1` DECIMAL(9,8), `bigint_col` BIGINT, `decimal_col2` DECIMAL(7,4))";
    hiveClient.updateHiveSQL(multipleTargetColumnSql);
    System.out.println(hiveClient.getTableSchema(tableName));
    assertTrue(hiveClient.getTableSchema(tableName).containsValue("DECIMAL(9,8)")
        && hiveClient.getTableSchema(tableName).containsValue("DECIMAL(7,4)"), errorMsg);
    hiveClient.updateHiveSQL(dropTableSql);

    // test no columns in DECIMAL
    String noTargetColumnsSql = createTableSqlPrefix + "(`bigint_col` BIGINT)";
    hiveClient.updateHiveSQL(noTargetColumnsSql);
    System.out.println(hiveClient.getTableSchema(tableName));
    assertTrue(hiveClient.getTableSchema(tableName).size() == 1 && hiveClient.getTableSchema(tableName)
        .containsValue("BIGINT"), errorMsg);
    hiveClient.updateHiveSQL(dropTableSql);
  }
}
