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

package org.apache.hudi.hive.testutils;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.minicluster.ZookeeperTestService;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.hive.HoodieHiveClient;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.platform.commons.JUnitException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("SameParameterValue")
public class HiveTestUtil {

  private static ZooKeeperServer zkServer;
  private static HiveServer2 hiveServer;
  public static HiveTestService hiveTestService;
  private static ZookeeperTestService zkService;
  private static Configuration configuration;
  public static HiveSyncConfig hiveSyncConfig;
  private static DateTimeFormatter dtfOut;
  public static FileSystem fileSystem;
  private static Set<String> createdTablesSet = new HashSet<>();

  public static void setUp() throws IOException, InterruptedException {
    configuration = new Configuration();
    if (zkServer == null) {
      zkService = new ZookeeperTestService(configuration);
      zkServer = zkService.start();
    }
    if (hiveServer == null) {
      hiveTestService = new HiveTestService(configuration);
      hiveServer = hiveTestService.start();
    }
    fileSystem = FileSystem.get(configuration);

    hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.jdbcUrl = hiveTestService.getJdbcHive2Url();
    hiveSyncConfig.hiveUser = "";
    hiveSyncConfig.hivePass = "";
    hiveSyncConfig.databaseName = "testdb";
    hiveSyncConfig.tableName = "test1";
    hiveSyncConfig.basePath = Files.createTempDirectory("hivesynctest" + Instant.now().toEpochMilli()).toUri().toString();
    hiveSyncConfig.assumeDatePartitioning = true;
    hiveSyncConfig.usePreApacheInputFormat = false;
    hiveSyncConfig.partitionFields = Collections.singletonList("datestr");

    dtfOut = DateTimeFormatter.ofPattern("yyyy/MM/dd");

    clear();
  }

  public static void clear() throws IOException {
    fileSystem.delete(new Path(hiveSyncConfig.basePath), true);
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(hiveSyncConfig.tableName)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(configuration, hiveSyncConfig.basePath);

    HoodieHiveClient client = new HoodieHiveClient(hiveSyncConfig, hiveServer.getHiveConf(), fileSystem);
    for (String tableName : createdTablesSet) {
      client.updateHiveSQL("drop table if exists " + tableName);
    }
    createdTablesSet.clear();
    client.updateHiveSQL("drop database if exists " + hiveSyncConfig.databaseName);
    client.updateHiveSQL("create database " + hiveSyncConfig.databaseName);
  }

  public static HiveConf getHiveConf() {
    return hiveServer.getHiveConf();
  }

  public static void shutdown() {
    if (hiveServer != null) {
      hiveServer.stop();
    }
    if (hiveTestService != null) {
      hiveTestService.stop();
    }
    if (zkServer != null) {
      zkServer.shutdown();
    }
  }

  public static void createCOWTable(String instantTime, int numberOfPartitions, boolean useSchemaFromCommitMetadata)
      throws IOException, URISyntaxException {
    Path path = new Path(hiveSyncConfig.basePath);
    FileIOUtils.deleteDirectory(new File(hiveSyncConfig.basePath));
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(hiveSyncConfig.tableName)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(configuration, hiveSyncConfig.basePath);

    boolean result = fileSystem.mkdirs(path);
    checkResult(result);
    ZonedDateTime dateTime = ZonedDateTime.now();
    HoodieCommitMetadata commitMetadata = createPartitions(numberOfPartitions, true,
        useSchemaFromCommitMetadata, dateTime, instantTime);
    createdTablesSet.add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);
    createCommitFile(commitMetadata, instantTime);
  }

  public static void createMORTable(String commitTime, String deltaCommitTime, int numberOfPartitions,
                                    boolean createDeltaCommit, boolean useSchemaFromCommitMetadata)
      throws IOException, URISyntaxException, InterruptedException {
    Path path = new Path(hiveSyncConfig.basePath);
    FileIOUtils.deleteDirectory(new File(hiveSyncConfig.basePath));
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setTableName(hiveSyncConfig.tableName)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(configuration, hiveSyncConfig.basePath);

    boolean result = fileSystem.mkdirs(path);
    checkResult(result);
    ZonedDateTime dateTime = ZonedDateTime.now();
    HoodieCommitMetadata commitMetadata = createPartitions(numberOfPartitions, true,
        useSchemaFromCommitMetadata, dateTime, commitTime);
    createdTablesSet
        .add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE);
    createdTablesSet
        .add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE);
    HoodieCommitMetadata compactionMetadata = new HoodieCommitMetadata();
    commitMetadata.getPartitionToWriteStats()
        .forEach((key, value) -> value.forEach(l -> compactionMetadata.addWriteStat(key, l)));
    addSchemaToCommitMetadata(compactionMetadata, commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY),
        useSchemaFromCommitMetadata);
    createCompactionCommitFile(compactionMetadata, commitTime);
    if (createDeltaCommit) {
      // Write a delta commit
      HoodieCommitMetadata deltaMetadata = createLogFiles(commitMetadata.getPartitionToWriteStats(), true,
          useSchemaFromCommitMetadata);
      createDeltaCommitFile(deltaMetadata, deltaCommitTime);
    }
  }

  public static void addCOWPartitions(int numberOfPartitions, boolean isParquetSchemaSimple,
      boolean useSchemaFromCommitMetadata, ZonedDateTime startFrom, String instantTime) throws IOException, URISyntaxException {
    HoodieCommitMetadata commitMetadata =
        createPartitions(numberOfPartitions, isParquetSchemaSimple, useSchemaFromCommitMetadata, startFrom, instantTime);
    createdTablesSet.add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);
    createCommitFile(commitMetadata, instantTime);
  }

  public static void addCOWPartition(String partitionPath, boolean isParquetSchemaSimple,
                                     boolean useSchemaFromCommitMetadata, String instantTime) throws IOException, URISyntaxException {
    HoodieCommitMetadata commitMetadata =
        createPartition(partitionPath, isParquetSchemaSimple, useSchemaFromCommitMetadata, instantTime);
    createdTablesSet.add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);
    createCommitFile(commitMetadata, instantTime);
  }

  public static void addMORPartitions(int numberOfPartitions, boolean isParquetSchemaSimple, boolean isLogSchemaSimple,
      boolean useSchemaFromCommitMetadata, ZonedDateTime startFrom, String instantTime, String deltaCommitTime)
      throws IOException, URISyntaxException, InterruptedException {
    HoodieCommitMetadata commitMetadata = createPartitions(numberOfPartitions, isParquetSchemaSimple,
        useSchemaFromCommitMetadata, startFrom, instantTime);
    createdTablesSet.add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE);
    createdTablesSet.add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE);
    HoodieCommitMetadata compactionMetadata = new HoodieCommitMetadata();
    commitMetadata.getPartitionToWriteStats()
        .forEach((key, value) -> value.forEach(l -> compactionMetadata.addWriteStat(key, l)));
    addSchemaToCommitMetadata(compactionMetadata, commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY),
        useSchemaFromCommitMetadata);
    createCompactionCommitFile(compactionMetadata, instantTime);
    HoodieCommitMetadata deltaMetadata = createLogFiles(commitMetadata.getPartitionToWriteStats(), isLogSchemaSimple,
        useSchemaFromCommitMetadata);
    createDeltaCommitFile(deltaMetadata, deltaCommitTime);
  }

  private static HoodieCommitMetadata createLogFiles(Map<String, List<HoodieWriteStat>> partitionWriteStats,
                                                     boolean isLogSchemaSimple, boolean useSchemaFromCommitMetadata)
      throws InterruptedException, IOException, URISyntaxException {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    for (Entry<String, List<HoodieWriteStat>> wEntry : partitionWriteStats.entrySet()) {
      String partitionPath = wEntry.getKey();
      for (HoodieWriteStat wStat : wEntry.getValue()) {
        Path path = new Path(wStat.getPath());
        HoodieBaseFile dataFile = new HoodieBaseFile(fileSystem.getFileStatus(path));
        HoodieLogFile logFile = generateLogData(path, isLogSchemaSimple);
        HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
        writeStat.setFileId(dataFile.getFileId());
        writeStat.setPath(logFile.getPath().toString());
        commitMetadata.addWriteStat(partitionPath, writeStat);
      }
    }
    addSchemaToCommitMetadata(commitMetadata, isLogSchemaSimple, useSchemaFromCommitMetadata);
    return commitMetadata;
  }

  private static HoodieCommitMetadata createPartitions(int numberOfPartitions, boolean isParquetSchemaSimple,
      boolean useSchemaFromCommitMetadata, ZonedDateTime startFrom, String instantTime) throws IOException, URISyntaxException {
    startFrom = startFrom.truncatedTo(ChronoUnit.DAYS);

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    for (int i = 0; i < numberOfPartitions; i++) {
      String partitionPath = startFrom.format(dtfOut);
      Path partPath = new Path(hiveSyncConfig.basePath + "/" + partitionPath);
      fileSystem.makeQualified(partPath);
      fileSystem.mkdirs(partPath);
      List<HoodieWriteStat> writeStats = createTestData(partPath, isParquetSchemaSimple, instantTime);
      startFrom = startFrom.minusDays(1);
      writeStats.forEach(s -> commitMetadata.addWriteStat(partitionPath, s));
    }
    addSchemaToCommitMetadata(commitMetadata, isParquetSchemaSimple, useSchemaFromCommitMetadata);
    return commitMetadata;
  }

  private static HoodieCommitMetadata createPartition(String partitionPath, boolean isParquetSchemaSimple,
                                                      boolean useSchemaFromCommitMetadata, String instantTime) throws IOException, URISyntaxException {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    Path partPath = new Path(hiveSyncConfig.basePath + "/" + partitionPath);
    fileSystem.makeQualified(partPath);
    fileSystem.mkdirs(partPath);
    List<HoodieWriteStat> writeStats = createTestData(partPath, isParquetSchemaSimple, instantTime);
    writeStats.forEach(s -> commitMetadata.addWriteStat(partitionPath, s));
    addSchemaToCommitMetadata(commitMetadata, isParquetSchemaSimple, useSchemaFromCommitMetadata);
    return commitMetadata;
  }

  private static List<HoodieWriteStat> createTestData(Path partPath, boolean isParquetSchemaSimple, String instantTime)
      throws IOException, URISyntaxException {
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      // Create 5 files
      String fileId = UUID.randomUUID().toString();
      Path filePath = new Path(partPath.toString() + "/" + FSUtils.makeDataFileName(instantTime, "1-0-1", fileId));
      generateParquetData(filePath, isParquetSchemaSimple);
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setFileId(fileId);
      writeStat.setPath(filePath.toString());
      writeStats.add(writeStat);
    }
    return writeStats;
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  private static void generateParquetData(Path filePath, boolean isParquetSchemaSimple)
      throws IOException, URISyntaxException {
    Schema schema = getTestDataSchema(isParquetSchemaSimple);
    org.apache.parquet.schema.MessageType parquetSchema = new AvroSchemaConverter().convert(schema);
    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.0001, -1,
        BloomFilterTypeCode.SIMPLE.name());
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(parquetSchema, schema, filter);
    ParquetWriter writer = new ParquetWriter(filePath, writeSupport, CompressionCodecName.GZIP, 120 * 1024 * 1024,
        ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED, ParquetWriter.DEFAULT_WRITER_VERSION, fileSystem.getConf());

    List<IndexedRecord> testRecords = (isParquetSchemaSimple ? SchemaTestUtil.generateTestRecords(0, 100)
        : SchemaTestUtil.generateEvolvedTestRecords(100, 100));
    testRecords.forEach(s -> {
      try {
        writer.write(s);
      } catch (IOException e) {
        fail("IOException while writing test records as parquet" + e.toString());
      }
    });
    writer.close();
  }

  private static HoodieLogFile generateLogData(Path parquetFilePath, boolean isLogSchemaSimple)
      throws IOException, InterruptedException, URISyntaxException {
    Schema schema = getTestDataSchema(isLogSchemaSimple);
    HoodieBaseFile dataFile = new HoodieBaseFile(fileSystem.getFileStatus(parquetFilePath));
    // Write a log file for this parquet file
    Writer logWriter = HoodieLogFormat.newWriterBuilder().onParentPath(parquetFilePath.getParent())
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(dataFile.getFileId())
        .overBaseCommit(dataFile.getCommitTime()).withFs(fileSystem).build();
    List<IndexedRecord> records = (isLogSchemaSimple ? SchemaTestUtil.generateTestRecords(0, 100)
        : SchemaTestUtil.generateEvolvedTestRecords(100, 100));
    Map<HeaderMetadataType, String> header = new HashMap<>(2);
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, dataFile.getCommitTime());
    header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(records, header);
    logWriter.appendBlock(dataBlock);
    logWriter.close();
    return logWriter.getLogFile();
  }

  private static Schema getTestDataSchema(boolean isSimpleSchema) throws IOException {
    return isSimpleSchema ? SchemaTestUtil.getSimpleSchema() : SchemaTestUtil.getEvolvedSchema();
  }

  private static void addSchemaToCommitMetadata(HoodieCommitMetadata commitMetadata, boolean isSimpleSchema,
                                                boolean useSchemaFromCommitMetadata) throws IOException {
    if (useSchemaFromCommitMetadata) {
      Schema dataSchema = getTestDataSchema(isSimpleSchema);
      commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, dataSchema.toString());
    }
  }

  private static void addSchemaToCommitMetadata(HoodieCommitMetadata commitMetadata, String schema,
                                                boolean useSchemaFromCommitMetadata) {
    if (useSchemaFromCommitMetadata) {
      commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, schema);
    }
  }

  private static void checkResult(boolean result) {
    if (!result) {
      throw new JUnitException("Could not initialize");
    }
  }

  public static void createCommitFile(HoodieCommitMetadata commitMetadata, String instantTime) throws IOException {
    byte[] bytes = commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8);
    Path fullPath = new Path(hiveSyncConfig.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeCommitFileName(instantTime));
    FSDataOutputStream fsout = fileSystem.create(fullPath, true);
    fsout.write(bytes);
    fsout.close();
  }

  public static void createCommitFileWithSchema(HoodieCommitMetadata commitMetadata, String instantTime, boolean isSimpleSchema) throws IOException {
    addSchemaToCommitMetadata(commitMetadata, isSimpleSchema, true);
    createCommitFile(commitMetadata, instantTime);
  }

  private static void createCompactionCommitFile(HoodieCommitMetadata commitMetadata, String instantTime)
      throws IOException {
    byte[] bytes = commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8);
    Path fullPath = new Path(hiveSyncConfig.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeCommitFileName(instantTime));
    FSDataOutputStream fsout = fileSystem.create(fullPath, true);
    fsout.write(bytes);
    fsout.close();
  }

  private static void createDeltaCommitFile(HoodieCommitMetadata deltaCommitMetadata, String deltaCommitTime)
      throws IOException {
    byte[] bytes = deltaCommitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8);
    Path fullPath = new Path(hiveSyncConfig.basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/"
        + HoodieTimeline.makeDeltaFileName(deltaCommitTime));
    FSDataOutputStream fsout = fileSystem.create(fullPath, true);
    fsout.write(bytes);
    fsout.close();
  }

  public static Set<String> getCreatedTablesSet() {
    return createdTablesSet;
  }
}
