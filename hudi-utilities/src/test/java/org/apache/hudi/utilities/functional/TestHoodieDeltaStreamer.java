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

package org.apache.hudi.utilities.functional;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ConcurrentModificationException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveClient;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.utilities.DummySchemaProvider;
import org.apache.hudi.utilities.HoodieClusteringJob;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.CsvDFSSource;
import org.apache.hudi.utilities.sources.HoodieIncrSource;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.JdbcSource;
import org.apache.hudi.utilities.sources.JsonKafkaSource;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.sources.TestDataSource;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config;
import org.apache.hudi.utilities.testutils.JdbcTestUtils;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.testutils.sources.DistributedTestDataSource;
import org.apache.hudi.utilities.testutils.sources.config.SourceConfigs;
import org.apache.hudi.utilities.transform.SqlQueryBasedTransformer;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.CHECKPOINT_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Basic tests against {@link HoodieDeltaStreamer}, by issuing bulk_inserts, upserts, inserts. Check counts at the end.
 */
public class TestHoodieDeltaStreamer extends UtilitiesTestBase {

  private static final Random RANDOM = new Random();
  private static final String PROPS_FILENAME_TEST_SOURCE = "test-source.properties";
  public static final String PROPS_FILENAME_TEST_SOURCE1 = "test-source1.properties";
  public static final String PROPS_INVALID_HIVE_SYNC_TEST_SOURCE1 = "test-invalid-hive-sync-source1.properties";
  public static final String PROPS_INVALID_FILE = "test-invalid-props.properties";
  public static final String PROPS_INVALID_TABLE_CONFIG_FILE = "test-invalid-table-config.properties";
  private static final String PROPS_FILENAME_TEST_INVALID = "test-invalid.properties";
  private static final String PROPS_FILENAME_TEST_CSV = "test-csv-dfs-source.properties";
  protected static final String PROPS_FILENAME_TEST_PARQUET = "test-parquet-dfs-source.properties";
  private static final String PROPS_FILENAME_TEST_JSON_KAFKA = "test-json-kafka-dfs-source.properties";
  private static final String PROPS_FILENAME_TEST_MULTI_WRITER = "test-multi-writer.properties";
  private static final String FIRST_PARQUET_FILE_NAME = "1.parquet";
  private static String PARQUET_SOURCE_ROOT;
  private static String JSON_KAFKA_SOURCE_ROOT;
  private static final int PARQUET_NUM_RECORDS = 5;
  private static final int CSV_NUM_RECORDS = 3;
  private static final int JSON_KAFKA_NUM_RECORDS = 5;
  // Required fields
  private static final String TGT_BASE_PATH_PARAM = "--target-base-path";
  private static final String TGT_BASE_PATH_VALUE = "s3://mybucket/blah";
  private static final String TABLE_TYPE_PARAM = "--table-type";
  private static final String TABLE_TYPE_VALUE = "COPY_ON_WRITE";
  private static final String TARGET_TABLE_PARAM = "--target-table";
  private static final String TARGET_TABLE_VALUE = "test";
  private static final String BASE_FILE_FORMAT_PARAM = "--base-file-format";
  private static final String BASE_FILE_FORMAT_VALUE = "PARQUET";
  private static final String SOURCE_LIMIT_PARAM = "--source-limit";
  private static final String SOURCE_LIMIT_VALUE = "500";
  private static final String ENABLE_HIVE_SYNC_PARAM = "--enable-hive-sync";
  private static final String HOODIE_CONF_PARAM = "--hoodie-conf";
  private static final String HOODIE_CONF_VALUE1 = "hoodie.datasource.hive_sync.table=test_table";
  private static final String HOODIE_CONF_VALUE2 = "hoodie.datasource.write.recordkey.field=Field1,Field2,Field3";
  private static final Logger LOG = LogManager.getLogger(TestHoodieDeltaStreamer.class);
  public static KafkaTestUtils testUtils;
  protected static String topicName;

  protected static int testNum = 1;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass(true);
    PARQUET_SOURCE_ROOT = dfsBasePath + "/parquetFiles";
    JSON_KAFKA_SOURCE_ROOT = dfsBasePath + "/jsonKafkaFiles";
    testUtils = new KafkaTestUtils();
    testUtils.setup();
    topicName = "topic" + testNum;

    // prepare the configs.
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/base.properties", dfs, dfsBasePath + "/base.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/base.properties", dfs, dfsBasePath + "/config/base.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/sql-transformer.properties", dfs,
        dfsBasePath + "/sql-transformer.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source.avsc", dfs, dfsBasePath + "/source.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source-flattened.avsc", dfs, dfsBasePath + "/source-flattened.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/target.avsc", dfs, dfsBasePath + "/target.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/target-flattened.avsc", dfs, dfsBasePath + "/target-flattened.avsc");

    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source_short_trip_uber.avsc", dfs, dfsBasePath + "/source_short_trip_uber.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source_uber.avsc", dfs, dfsBasePath + "/source_uber.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/target_short_trip_uber.avsc", dfs, dfsBasePath + "/target_short_trip_uber.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/target_uber.avsc", dfs, dfsBasePath + "/target_uber.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/invalid_hive_sync_uber_config.properties", dfs, dfsBasePath + "/config/invalid_hive_sync_uber_config.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/uber_config.properties", dfs, dfsBasePath + "/config/uber_config.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/short_trip_uber_config.properties", dfs, dfsBasePath + "/config/short_trip_uber_config.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/clusteringjob.properties", dfs, dfsBasePath + "/clusteringjob.properties");

    TypedProperties props = new TypedProperties();
    props.setProperty("include", "sql-transformer.properties");
    props.setProperty("hoodie.datasource.write.keygenerator.class", TestGenerator.class.getName());
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "not_there");
    props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source.avsc");
    props.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target.avsc");

    // Hive Configs
    props.setProperty(DataSourceWriteOptions.HIVE_URL_OPT_KEY().key(), "jdbc:hive2://127.0.0.1:9999/");
    props.setProperty(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY().key(), "testdb1");
    props.setProperty(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY().key(), "hive_trips");
    props.setProperty(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY().key(), "datestr");
    props.setProperty(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY().key(),
        MultiPartKeysValueExtractor.class.getName());
    UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_SOURCE);

    // Properties used for the delta-streamer which incrementally pulls from upstream Hudi source table and writes to
    // downstream hudi table
    TypedProperties downstreamProps = new TypedProperties();
    downstreamProps.setProperty("include", "base.properties");
    downstreamProps.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    downstreamProps.setProperty("hoodie.datasource.write.partitionpath.field", "not_there");

    // Source schema is the target schema of upstream table
    downstreamProps.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/target.avsc");
    downstreamProps.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target.avsc");
    UtilitiesTestBase.Helpers.savePropsToDFS(downstreamProps, dfs, dfsBasePath + "/test-downstream-source.properties");

    // Properties used for testing invalid key generator
    TypedProperties invalidProps = new TypedProperties();
    invalidProps.setProperty("include", "sql-transformer.properties");
    invalidProps.setProperty("hoodie.datasource.write.keygenerator.class", "invalid");
    invalidProps.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    invalidProps.setProperty("hoodie.datasource.write.partitionpath.field", "not_there");
    invalidProps.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source.avsc");
    invalidProps.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target.avsc");
    UtilitiesTestBase.Helpers.savePropsToDFS(invalidProps, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_INVALID);

    TypedProperties props1 = new TypedProperties();
    populateAllCommonProps(props1);
    UtilitiesTestBase.Helpers.savePropsToDFS(props1, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_SOURCE1);

    TypedProperties properties = new TypedProperties();
    populateInvalidTableConfigFilePathProps(properties);
    UtilitiesTestBase.Helpers.savePropsToDFS(properties, dfs, dfsBasePath + "/" + PROPS_INVALID_TABLE_CONFIG_FILE);

    TypedProperties invalidHiveSyncProps = new TypedProperties();
    invalidHiveSyncProps.setProperty("hoodie.deltastreamer.ingestion.tablesToBeIngested", "uber_db.dummy_table_uber");
    invalidHiveSyncProps.setProperty("hoodie.deltastreamer.ingestion.uber_db.dummy_table_uber.configFile", dfsBasePath + "/config/invalid_hive_sync_uber_config.properties");
    UtilitiesTestBase.Helpers.savePropsToDFS(invalidHiveSyncProps, dfs, dfsBasePath + "/" + PROPS_INVALID_HIVE_SYNC_TEST_SOURCE1);

    prepareParquetDFSFiles(PARQUET_NUM_RECORDS, PARQUET_SOURCE_ROOT);
  }

  @AfterAll
  public static void release() {
    if (testUtils != null) {
      testUtils.teardown();
    }
  }

  private static void populateInvalidTableConfigFilePathProps(TypedProperties props) {
    props.setProperty("hoodie.datasource.write.keygenerator.class", TestHoodieDeltaStreamer.TestGenerator.class.getName());
    props.setProperty("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyyMMdd");
    props.setProperty("hoodie.deltastreamer.ingestion.tablesToBeIngested", "uber_db.dummy_table_uber");
    props.setProperty("hoodie.deltastreamer.ingestion.uber_db.dummy_table_uber.configFile", dfsBasePath + "/config/invalid_uber_config.properties");
  }

  private static void populateAllCommonProps(TypedProperties props) {
    populateCommonProps(props);
    populateCommonKafkaProps(props);
    populateCommonHiveProps(props);
  }

  protected static void populateCommonProps(TypedProperties props) {
    props.setProperty("hoodie.datasource.write.keygenerator.class", TestHoodieDeltaStreamer.TestGenerator.class.getName());
    props.setProperty("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyyMMdd");
    props.setProperty("hoodie.deltastreamer.ingestion.tablesToBeIngested", "short_trip_db.dummy_table_short_trip,uber_db.dummy_table_uber");
    props.setProperty("hoodie.deltastreamer.ingestion.uber_db.dummy_table_uber.configFile", dfsBasePath + "/config/uber_config.properties");
    props.setProperty("hoodie.deltastreamer.ingestion.short_trip_db.dummy_table_short_trip.configFile", dfsBasePath + "/config/short_trip_uber_config.properties");
  }

  protected static void populateCommonKafkaProps(TypedProperties props) {
    //Kafka source properties
    props.setProperty("bootstrap.servers", testUtils.brokerAddress());
    props.setProperty(Config.KAFKA_AUTO_OFFSET_RESET, "earliest");
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents", String.valueOf(5000));
  }

  protected static void populateCommonHiveProps(TypedProperties props) {
    // Hive Configs
    props.setProperty(DataSourceWriteOptions.HIVE_URL_OPT_KEY().key(), "jdbc:hive2://127.0.0.1:9999/");
    props.setProperty(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY().key(), "testdb2");
    props.setProperty(DataSourceWriteOptions.HIVE_ASSUME_DATE_PARTITION_OPT_KEY().key(), "false");
    props.setProperty(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY().key(), "datestr");
    props.setProperty(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY().key(),
        MultiPartKeysValueExtractor.class.getName());
  }

  protected static TypedProperties prepareMultiWriterProps(String propsFileName) throws IOException {
    TypedProperties props = new TypedProperties();
    populateAllCommonProps(props);

    props.setProperty("include", "sql-transformer.properties");
    props.setProperty("hoodie.datasource.write.keygenerator.class", TestGenerator.class.getName());
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "not_there");
    props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source.avsc");
    props.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target.avsc");

    props.setProperty("include", "base.properties");
    props.setProperty("hoodie.write.concurrency.mode", "optimistic_concurrency_control");
    props.setProperty("hoodie.cleaner.policy.failed.writes", "LAZY");
    props.setProperty("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider");
    props.setProperty("hoodie.write.lock.hivemetastore.database", "testdb1");
    props.setProperty("hoodie.write.lock.hivemetastore.table", "table1");
    props.setProperty("hoodie.write.lock.zookeeper.url", "127.0.0.1");
    props.setProperty("hoodie.write.lock.zookeeper.port", "2828");
    props.setProperty("hoodie.write.lock.wait_time_ms", "1200000");
    props.setProperty("hoodie.write.lock.num_retries", "10");
    props.setProperty("hoodie.write.lock.zookeeper.lock_key", "test_table");
    props.setProperty("hoodie.write.lock.zookeeper.base_path", "/test");

    UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/" + propsFileName);
    return props;
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
    if (testUtils != null) {
      testUtils.teardown();
    }
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
  }

  static class TestHelpers {

    static HoodieDeltaStreamer.Config makeDropAllConfig(String basePath, WriteOperationType op) {
      return makeConfig(basePath, op, Collections.singletonList(DropAllTransformer.class.getName()));
    }

    static HoodieDeltaStreamer.Config makeConfig(String basePath, WriteOperationType op) {
      return makeConfig(basePath, op, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
    }

    static HoodieDeltaStreamer.Config makeConfig(String basePath, WriteOperationType op, List<String> transformerClassNames) {
      return makeConfig(basePath, op, transformerClassNames, PROPS_FILENAME_TEST_SOURCE, false);
    }

    static HoodieDeltaStreamer.Config makeConfig(String basePath, WriteOperationType op, List<String> transformerClassNames,
                                                 String propsFilename, boolean enableHiveSync) {
      return makeConfig(basePath, op, transformerClassNames, propsFilename, enableHiveSync, true,
          false, null, null);
    }

    static HoodieDeltaStreamer.Config makeConfig(String basePath, WriteOperationType op, List<String> transformerClassNames,
                                                 String propsFilename, boolean enableHiveSync, boolean useSchemaProviderClass, boolean updatePayloadClass,
                                                 String payloadClassName, String tableType) {
      return makeConfig(basePath, op, TestDataSource.class.getName(), transformerClassNames, propsFilename, enableHiveSync,
          useSchemaProviderClass, 1000, updatePayloadClass, payloadClassName, tableType, "timestamp");
    }

    static HoodieDeltaStreamer.Config makeConfig(String basePath, WriteOperationType op, String sourceClassName,
                                                 List<String> transformerClassNames, String propsFilename, boolean enableHiveSync, boolean useSchemaProviderClass,
                                                 int sourceLimit, boolean updatePayloadClass, String payloadClassName, String tableType, String sourceOrderingField) {
      HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
      cfg.targetBasePath = basePath;
      cfg.targetTableName = "hoodie_trips";
      cfg.tableType = tableType == null ? "COPY_ON_WRITE" : tableType;
      cfg.sourceClassName = sourceClassName;
      cfg.transformerClassNames = transformerClassNames;
      cfg.operation = op;
      cfg.enableHiveSync = enableHiveSync;
      cfg.sourceOrderingField = sourceOrderingField;
      cfg.propsFilePath = dfsBasePath + "/" + propsFilename;
      cfg.sourceLimit = sourceLimit;
      if (updatePayloadClass) {
        cfg.payloadClassName = payloadClassName;
      }
      if (useSchemaProviderClass) {
        cfg.schemaProviderClassName = FilebasedSchemaProvider.class.getName();
      }
      return cfg;
    }

    static HoodieDeltaStreamer.Config makeConfigForHudiIncrSrc(String srcBasePath, String basePath, WriteOperationType op,
                                                               boolean addReadLatestOnMissingCkpt, String schemaProviderClassName) {
      HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
      cfg.targetBasePath = basePath;
      cfg.targetTableName = "hoodie_trips_copy";
      cfg.tableType = "COPY_ON_WRITE";
      cfg.sourceClassName = HoodieIncrSource.class.getName();
      cfg.operation = op;
      cfg.sourceOrderingField = "timestamp";
      cfg.propsFilePath = dfsBasePath + "/test-downstream-source.properties";
      cfg.sourceLimit = 1000;
      if (null != schemaProviderClassName) {
        cfg.schemaProviderClassName = schemaProviderClassName;
      }
      List<String> cfgs = new ArrayList<>();
      cfgs.add("hoodie.deltastreamer.source.hoodieincr.read_latest_on_missing_ckpt=" + addReadLatestOnMissingCkpt);
      cfgs.add("hoodie.deltastreamer.source.hoodieincr.path=" + srcBasePath);
      // No partition
      cfgs.add("hoodie.deltastreamer.source.hoodieincr.partition.fields=datestr");
      cfg.configs = cfgs;
      return cfg;
    }

    static void assertRecordCount(long expected, String tablePath, SQLContext sqlContext) {
      long recordCount = sqlContext.read().format("org.apache.hudi").load(tablePath).count();
      assertEquals(expected, recordCount);
    }

    static List<Row> countsPerCommit(String tablePath, SQLContext sqlContext) {
      return sqlContext.read().format("org.apache.hudi").load(tablePath).groupBy("_hoodie_commit_time").count()
          .sort("_hoodie_commit_time").collectAsList();
    }

    static void assertDistanceCount(long expected, String tablePath, SQLContext sqlContext) {
      sqlContext.read().format("org.apache.hudi").load(tablePath).registerTempTable("tmp_trips");
      long recordCount =
          sqlContext.sparkSession().sql("select * from tmp_trips where haversine_distance is not NULL").count();
      assertEquals(expected, recordCount);
    }

    static void assertDistanceCountWithExactValue(long expected, String tablePath, SQLContext sqlContext) {
      sqlContext.read().format("org.apache.hudi").load(tablePath).registerTempTable("tmp_trips");
      long recordCount =
          sqlContext.sparkSession().sql("select * from tmp_trips where haversine_distance = 1.0").count();
      assertEquals(expected, recordCount);
    }

    static void assertAtleastNCompactionCommits(int minExpected, String tablePath, FileSystem fs) {
      HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(tablePath).build();
      HoodieTimeline timeline = meta.getActiveTimeline().getCommitTimeline().filterCompletedInstants();
      LOG.info("Timeline Instants=" + meta.getActiveTimeline().getInstants().collect(Collectors.toList()));
      int numCompactionCommits = (int) timeline.getInstants().count();
      assertTrue(minExpected <= numCompactionCommits, "Got=" + numCompactionCommits + ", exp >=" + minExpected);
    }

    static void assertAtleastNDeltaCommits(int minExpected, String tablePath, FileSystem fs) {
      HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(tablePath).build();
      HoodieTimeline timeline = meta.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants();
      LOG.info("Timeline Instants=" + meta.getActiveTimeline().getInstants().collect(Collectors.toList()));
      int numDeltaCommits = (int) timeline.getInstants().count();
      assertTrue(minExpected <= numDeltaCommits, "Got=" + numDeltaCommits + ", exp >=" + minExpected);
    }

    static void assertAtLeastNCompletedReplaceCommits(int minExpected, String tablePath, DistributedFileSystem fs) {
      HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(fs.getConf()).setLoadActiveTimelineOnLoad(true).setBasePath(tablePath).build();
      HoodieTimeline timeline = meta.getActiveTimeline().getCompletedReplaceTimeline();
      LOG.info("Timeline Instants=" + meta.getActiveTimeline().getInstants().collect(Collectors.toList()));
      int numReplaceCommits = (int) timeline.getInstants().count();
      assertTrue(minExpected <= numReplaceCommits, "Got=" + numReplaceCommits + ", exp >=" + minExpected);
    }

    static void assertAtleastNCompactionCommitsAfterCommit(int minExpected, String lastSuccessfulCommit, String tablePath, FileSystem fs) {
      HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(tablePath).build();
      HoodieTimeline timeline = meta.getActiveTimeline().getCommitTimeline().findInstantsAfter(lastSuccessfulCommit).filterCompletedInstants();
      LOG.info("Timeline Instants=" + meta.getActiveTimeline().getInstants().collect(Collectors.toList()));
      int numCompactionCommits = (int) timeline.getInstants().count();
      assertTrue(minExpected <= numCompactionCommits, "Got=" + numCompactionCommits + ", exp >=" + minExpected);
    }

    static void assertAtleastNDeltaCommitsAfterCommit(int minExpected, String lastSuccessfulCommit, String tablePath, FileSystem fs) {
      HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(tablePath).build();
      HoodieTimeline timeline = meta.getActiveTimeline().getDeltaCommitTimeline().findInstantsAfter(lastSuccessfulCommit).filterCompletedInstants();
      LOG.info("Timeline Instants=" + meta.getActiveTimeline().getInstants().collect(Collectors.toList()));
      int numDeltaCommits = (int) timeline.getInstants().count();
      assertTrue(minExpected <= numDeltaCommits, "Got=" + numDeltaCommits + ", exp >=" + minExpected);
    }

    static String assertCommitMetadata(String expected, String tablePath, FileSystem fs, int totalCommits)
        throws IOException {
      HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(tablePath).build();
      HoodieTimeline timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
      HoodieInstant lastInstant = timeline.lastInstant().get();
      HoodieCommitMetadata commitMetadata =
          HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(lastInstant).get(), HoodieCommitMetadata.class);
      assertEquals(totalCommits, timeline.countInstants());
      assertEquals(expected, commitMetadata.getMetadata(CHECKPOINT_KEY));
      return lastInstant.getTimestamp();
    }

    static void waitTillCondition(Function<Boolean, Boolean> condition, Future dsFuture, long timeoutInSecs) throws Exception {
      Future<Boolean> res = Executors.newSingleThreadExecutor().submit(() -> {
        boolean ret = false;
        while (!ret && !dsFuture.isDone()) {
          try {
            Thread.sleep(3000);
            ret = condition.apply(true);
          } catch (Throwable error) {
            LOG.warn("Got error :", error);
            ret = false;
          }
        }
        return true;
      });
      res.get(timeoutInSecs, TimeUnit.SECONDS);
    }

    static void assertAtLeastNCommits(int minExpected, String tablePath, FileSystem fs) {
      HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(tablePath).build();
      HoodieTimeline timeline = meta.getActiveTimeline().filterCompletedInstants();
      LOG.info("Timeline Instants=" + meta.getActiveTimeline().getInstants().collect(Collectors.toList()));
      int numDeltaCommits = (int) timeline.getInstants().count();
      assertTrue(minExpected <= numDeltaCommits, "Got=" + numDeltaCommits + ", exp >=" + minExpected);
    }

    static void assertAtLeastNReplaceCommits(int minExpected, String tablePath, FileSystem fs) {
      HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(tablePath).setLoadActiveTimelineOnLoad(true).build();
      HoodieTimeline timeline = meta.getActiveTimeline().getCompletedReplaceTimeline();
      LOG.info("Timeline Instants=" + meta.getActiveTimeline().getInstants().collect(Collectors.toList()));
      int numDeltaCommits = (int) timeline.getInstants().count();
      assertTrue(minExpected <= numDeltaCommits, "Got=" + numDeltaCommits + ", exp >=" + minExpected);
    }
  }

  @Test
  public void testProps() {
    TypedProperties props =
        new DFSPropertiesConfiguration(dfs, new Path(dfsBasePath + "/" + PROPS_FILENAME_TEST_SOURCE)).getConfig();
    assertEquals(2, props.getInteger("hoodie.upsert.shuffle.parallelism"));
    assertEquals("_row_key", props.getString("hoodie.datasource.write.recordkey.field"));
    assertEquals("org.apache.hudi.utilities.functional.TestHoodieDeltaStreamer$TestGenerator",
        props.getString("hoodie.datasource.write.keygenerator.class"));
  }

  private static HoodieDeltaStreamer.Config getBaseConfig() {
    // Base config with all required fields
    HoodieDeltaStreamer.Config base = new HoodieDeltaStreamer.Config();
    base.targetBasePath = TGT_BASE_PATH_VALUE;
    base.tableType = TABLE_TYPE_VALUE;
    base.targetTableName = TARGET_TABLE_VALUE;
    return base;
  }

  private static Stream<Arguments> provideValidCliArgs() {

    HoodieDeltaStreamer.Config base = getBaseConfig();
    // String parameter
    HoodieDeltaStreamer.Config conf1 = getBaseConfig();
    conf1.baseFileFormat = BASE_FILE_FORMAT_VALUE;

    // Integer parameter
    HoodieDeltaStreamer.Config conf2 = getBaseConfig();
    conf2.sourceLimit = Long.parseLong(SOURCE_LIMIT_VALUE);

    // Boolean Parameter
    HoodieDeltaStreamer.Config conf3 = getBaseConfig();
    conf3.enableHiveSync = true;

    // ArrayList Parameter with 1 value
    HoodieDeltaStreamer.Config conf4 = getBaseConfig();
    conf4.configs = Arrays.asList(HOODIE_CONF_VALUE1);

    // ArrayList Parameter with comma separated values
    HoodieDeltaStreamer.Config conf5 = getBaseConfig();
    conf5.configs = Arrays.asList(HOODIE_CONF_VALUE2);

    // Multiple ArrayList values
    HoodieDeltaStreamer.Config conf6 = getBaseConfig();
    conf6.configs = Arrays.asList(HOODIE_CONF_VALUE1, HOODIE_CONF_VALUE2);

    // Super set of all cases
    HoodieDeltaStreamer.Config conf = getBaseConfig();
    conf.baseFileFormat = BASE_FILE_FORMAT_VALUE;
    conf.sourceLimit = Long.parseLong(SOURCE_LIMIT_VALUE);
    conf.enableHiveSync = true;
    conf.configs = Arrays.asList(HOODIE_CONF_VALUE1, HOODIE_CONF_VALUE2);

    String[] allConfig = new String[]{TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE, SOURCE_LIMIT_PARAM,
        SOURCE_LIMIT_VALUE, TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
        BASE_FILE_FORMAT_PARAM, BASE_FILE_FORMAT_VALUE, ENABLE_HIVE_SYNC_PARAM, HOODIE_CONF_PARAM, HOODIE_CONF_VALUE1,
        HOODIE_CONF_PARAM, HOODIE_CONF_VALUE2};

    return Stream.of(
            // Base
            Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
                TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE}, base),
            // String
            Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
                TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
                BASE_FILE_FORMAT_PARAM, BASE_FILE_FORMAT_VALUE}, conf1),
            // Integer
            Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
                TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
                SOURCE_LIMIT_PARAM, SOURCE_LIMIT_VALUE}, conf2),
            // Boolean
            Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
                TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
                ENABLE_HIVE_SYNC_PARAM}, conf3),
            // Array List 1
            Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
                TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
                HOODIE_CONF_PARAM, HOODIE_CONF_VALUE1}, conf4),
            // Array List with comma
            Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
                TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
                HOODIE_CONF_PARAM, HOODIE_CONF_VALUE2}, conf5),
            // Array list with multiple values
            Arguments.of(new String[] {TGT_BASE_PATH_PARAM, TGT_BASE_PATH_VALUE,
                TABLE_TYPE_PARAM, TABLE_TYPE_VALUE, TARGET_TABLE_PARAM, TARGET_TABLE_VALUE,
                HOODIE_CONF_PARAM, HOODIE_CONF_VALUE1, HOODIE_CONF_PARAM, HOODIE_CONF_VALUE2}, conf6),
            // All
            Arguments.of(allConfig, conf)
    );
  }
  
  @ParameterizedTest
  @MethodSource("provideValidCliArgs")
  public void testValidCommandLineArgs(String[] args, HoodieDeltaStreamer.Config expected) {
    assertEquals(expected, HoodieDeltaStreamer.getConfig(args));
  }

  @Test
  public void testKafkaConnectCheckpointProvider() throws IOException {
    String tableBasePath = dfsBasePath + "/test_table";
    String bootstrapPath = dfsBasePath + "/kafka_topic1";
    String partitionPath = bootstrapPath + "/year=2016/month=05/day=01";
    String filePath = partitionPath + "/kafka_topic1+0+100+200.parquet";
    String checkpointProviderClass = "org.apache.hudi.utilities.checkpointing.KafkaConnectHdfsProvider";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeDropAllConfig(tableBasePath, WriteOperationType.UPSERT);
    TypedProperties props =
        new DFSPropertiesConfiguration(dfs, new Path(dfsBasePath + "/" + PROPS_FILENAME_TEST_SOURCE)).getConfig();
    props.put("hoodie.deltastreamer.checkpoint.provider.path", bootstrapPath);
    cfg.initialCheckpointProvider = checkpointProviderClass;
    // create regular kafka connect hdfs dirs
    dfs.mkdirs(new Path(bootstrapPath));
    dfs.mkdirs(new Path(partitionPath));
    // generate parquet files using kafka connect naming convention
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    Helpers.saveParquetToDFS(Helpers.toGenericRecords(dataGenerator.generateInserts("000", 100)), new Path(filePath));
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(cfg, jsc, dfs, hdfsTestService.getHadoopConf(), Option.ofNullable(props));
    assertEquals("kafka_topic1,0:200", deltaStreamer.getConfig().checkpoint);
  }

  @Test
  public void testPropsWithInvalidKeyGenerator() throws Exception {
    Exception e = assertThrows(IOException.class, () -> {
      String tableBasePath = dfsBasePath + "/test_table";
      HoodieDeltaStreamer deltaStreamer =
          new HoodieDeltaStreamer(TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT,
              Collections.singletonList(TripsWithDistanceTransformer.class.getName()), PROPS_FILENAME_TEST_INVALID, false), jsc);
      deltaStreamer.sync();
    }, "Should error out when setting the key generator class property to an invalid value");
    // expected
    LOG.debug("Expected error during getting the key generator", e);
    assertTrue(e.getMessage().contains("Could not load key generator class"));
  }

  @Test
  public void testTableCreation() throws Exception {
    Exception e = assertThrows(TableNotFoundException.class, () -> {
      dfs.mkdirs(new Path(dfsBasePath + "/not_a_table"));
      HoodieDeltaStreamer deltaStreamer =
          new HoodieDeltaStreamer(TestHelpers.makeConfig(dfsBasePath + "/not_a_table", WriteOperationType.BULK_INSERT), jsc);
      deltaStreamer.sync();
    }, "Should error out when pointed out at a dir thats not a table");
    // expected
    LOG.debug("Expected error during table creation", e);
  }

  @Test
  public void testBulkInsertsAndUpsertsWithBootstrap() throws Exception {
    String tableBasePath = dfsBasePath + "/test_table";

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    new HoodieDeltaStreamer(cfg, jsc).sync();
    TestHelpers.assertRecordCount(1000, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCount(1000, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, dfs, 1);

    // No new data => no commits.
    cfg.sourceLimit = 0;
    new HoodieDeltaStreamer(cfg, jsc).sync();
    TestHelpers.assertRecordCount(1000, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCount(1000, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, dfs, 1);

    // upsert() #1
    cfg.sourceLimit = 2000;
    cfg.operation = WriteOperationType.UPSERT;
    new HoodieDeltaStreamer(cfg, jsc).sync();
    TestHelpers.assertRecordCount(1950, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCount(1950, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertCommitMetadata("00001", tableBasePath, dfs, 2);
    List<Row> counts = TestHelpers.countsPerCommit(tableBasePath + "/*/*.parquet", sqlContext);
    assertEquals(1950, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());

    // Perform bootstrap with tableBasePath as source
    String bootstrapSourcePath = dfsBasePath + "/src_bootstrapped";
    Dataset<Row> sourceDf = sqlContext.read()
                              .format("org.apache.hudi")
                              .load(tableBasePath + "/*/*.parquet");
    sourceDf.write().format("parquet").save(bootstrapSourcePath);

    String newDatasetBasePath = dfsBasePath + "/test_dataset_bootstrapped";
    cfg.runBootstrap = true;
    cfg.configs.add(String.format("hoodie.bootstrap.base.path=%s", bootstrapSourcePath));
    cfg.configs.add(String.format("hoodie.bootstrap.keygen.class=%s", SimpleKeyGenerator.class.getName()));
    cfg.configs.add("hoodie.bootstrap.parallelism=5");
    cfg.targetBasePath = newDatasetBasePath;
    new HoodieDeltaStreamer(cfg, jsc).sync();
    Dataset<Row> res = sqlContext.read().format("org.apache.hudi").load(newDatasetBasePath + "/*.parquet");
    LOG.info("Schema :");
    res.printSchema();

    TestHelpers.assertRecordCount(1950, newDatasetBasePath + "/*.parquet", sqlContext);
    res.registerTempTable("bootstrapped");
    assertEquals(1950, sqlContext.sql("select distinct _hoodie_record_key from bootstrapped").count());

    StructField[] fields = res.schema().fields();
    List<String> fieldNames = Arrays.asList(res.schema().fieldNames());
    List<String> expectedFieldNames = Arrays.asList(sourceDf.schema().fieldNames());
    assertEquals(expectedFieldNames.size(), fields.length);
    assertTrue(fieldNames.containsAll(HoodieRecord.HOODIE_META_COLUMNS));
    assertTrue(fieldNames.containsAll(expectedFieldNames));
  }

  @Test
  public void testUpsertsCOWContinuousMode() throws Exception {
    testUpsertsContinuousMode(HoodieTableType.COPY_ON_WRITE, "continuous_cow");
  }

  @Test
  public void testUpsertsMORContinuousMode() throws Exception {
    testUpsertsContinuousMode(HoodieTableType.MERGE_ON_READ, "continuous_mor");
  }

  @Test
  public void testUpsertsCOWContinuousModeWithMultipleWriters() throws Exception {
    testUpsertsContinuousModeWithMultipleWriters(HoodieTableType.COPY_ON_WRITE, "continuous_cow_mulitwriter");
  }

  @Test
  public void testUpsertsMORContinuousModeWithMultipleWriters() throws Exception {
    testUpsertsContinuousModeWithMultipleWriters(HoodieTableType.MERGE_ON_READ, "continuous_mor_mulitwriter");
  }

  @Test
  public void testLatestCheckpointCarryOverWithMultipleWriters() throws Exception {
    testLatestCheckpointCarryOverWithMultipleWriters(HoodieTableType.COPY_ON_WRITE, "continuous_cow_checkpoint");
  }

  private void testUpsertsContinuousMode(HoodieTableType tableType, String tempDir) throws Exception {
    String tableBasePath = dfsBasePath + "/" + tempDir;
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;
    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    cfg.continuousMode = true;
    cfg.tableType = tableType.name();
    cfg.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfg.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN_PROP.key()));
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
        TestHelpers.assertAtleastNDeltaCommits(5, tableBasePath, dfs);
        TestHelpers.assertAtleastNCompactionCommits(2, tableBasePath, dfs);
      } else {
        TestHelpers.assertAtleastNCompactionCommits(5, tableBasePath, dfs);
      }
      TestHelpers.assertRecordCount(totalRecords, tableBasePath + "/*/*.parquet", sqlContext);
      TestHelpers.assertDistanceCount(totalRecords, tableBasePath + "/*/*.parquet", sqlContext);
      return true;
    });
  }

  private void testUpsertsContinuousModeWithMultipleWriters(HoodieTableType tableType, String tempDir) throws Exception {
    // NOTE : Overriding the LockProvider to FileSystemBasedLockProviderTestClass since Zookeeper locks work in unit test but fail on Jenkins with connection timeouts
    String tableBasePath = dfsBasePath + "/" + tempDir;
    // enable carrying forward latest checkpoint
    TypedProperties props = prepareMultiWriterProps(PROPS_FILENAME_TEST_MULTI_WRITER);
    props.setProperty("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass");
    props.setProperty("hoodie.write.lock.filesystem.path", tableBasePath);
    UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_MULTI_WRITER);
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;

    HoodieDeltaStreamer.Config cfgIngestionJob = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT,
        Arrays.asList(TripsWithDistanceTransformer.class.getName()), PROPS_FILENAME_TEST_MULTI_WRITER, false);
    cfgIngestionJob.continuousMode = true;
    cfgIngestionJob.tableType = tableType.name();
    cfgIngestionJob.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfgIngestionJob.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN_PROP.key()));
    HoodieDeltaStreamer ingestionJob = new HoodieDeltaStreamer(cfgIngestionJob, jsc);

    // Prepare base dataset with some commits
    deltaStreamerTestRunner(ingestionJob, cfgIngestionJob, (r) -> {
      if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
        TestHelpers.assertAtleastNDeltaCommits(3, tableBasePath, dfs);
        TestHelpers.assertAtleastNCompactionCommits(1, tableBasePath, dfs);
      } else {
        TestHelpers.assertAtleastNCompactionCommits(3, tableBasePath, dfs);
      }
      TestHelpers.assertRecordCount(totalRecords, tableBasePath + "/*/*.parquet", sqlContext);
      TestHelpers.assertDistanceCount(totalRecords, tableBasePath + "/*/*.parquet", sqlContext);
      return true;
    });

    // create a backfill job
    HoodieDeltaStreamer.Config cfgBackfillJob = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT,
        Arrays.asList(TripsWithDistanceTransformer.class.getName()), PROPS_FILENAME_TEST_MULTI_WRITER, false);
    cfgBackfillJob.continuousMode = false;
    cfgBackfillJob.tableType = tableType.name();
    HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(dfs.getConf()).setBasePath(tableBasePath).build();
    HoodieTimeline timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
        .fromBytes(timeline.getInstantDetails(timeline.firstInstant().get()).get(), HoodieCommitMetadata.class);
    cfgBackfillJob.checkpoint = commitMetadata.getMetadata(CHECKPOINT_KEY);
    cfgBackfillJob.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfgBackfillJob.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN_PROP.key()));
    HoodieDeltaStreamer backfillJob = new HoodieDeltaStreamer(cfgBackfillJob, jsc);

    // re-init ingestion job to start sync service
    HoodieDeltaStreamer ingestionJob2 = new HoodieDeltaStreamer(cfgIngestionJob, jsc);

    // run ingestion & backfill in parallel, create conflict and fail one
    runJobsInParallel(tableBasePath, tableType, totalRecords, ingestionJob2,
        cfgIngestionJob, backfillJob, cfgBackfillJob, true);

    // create new ingestion & backfill job config to generate only INSERTS to avoid conflict
    props = prepareMultiWriterProps(PROPS_FILENAME_TEST_MULTI_WRITER);
    props.setProperty("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass");
    props.setProperty("hoodie.write.lock.filesystem.path", tableBasePath);
    props.setProperty("hoodie.test.source.generate.inserts", "true");
    UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_MULTI_WRITER);
    cfgBackfillJob = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT,
        Arrays.asList(TestIdentityTransformer.class.getName()), PROPS_FILENAME_TEST_MULTI_WRITER, false);
    cfgBackfillJob.continuousMode = false;
    cfgBackfillJob.tableType = tableType.name();
    meta = HoodieTableMetaClient.builder().setConf(dfs.getConf()).setBasePath(tableBasePath).build();
    timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    commitMetadata = HoodieCommitMetadata
        .fromBytes(timeline.getInstantDetails(timeline.firstInstant().get()).get(), HoodieCommitMetadata.class);
    cfgBackfillJob.checkpoint = commitMetadata.getMetadata(CHECKPOINT_KEY);
    cfgBackfillJob.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfgBackfillJob.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN_PROP.key()));

    cfgIngestionJob = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT,
        Arrays.asList(TestIdentityTransformer.class.getName()), PROPS_FILENAME_TEST_MULTI_WRITER, false);
    cfgIngestionJob.continuousMode = true;
    cfgIngestionJob.tableType = tableType.name();
    cfgIngestionJob.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfgIngestionJob.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN_PROP.key()));
    // re-init ingestion job
    HoodieDeltaStreamer ingestionJob3 = new HoodieDeltaStreamer(cfgIngestionJob, jsc);
    // re-init backfill job
    HoodieDeltaStreamer backfillJob2 = new HoodieDeltaStreamer(cfgBackfillJob, jsc);

    // run ingestion & backfill in parallel, avoid conflict and succeed both
    runJobsInParallel(tableBasePath, tableType, totalRecords, ingestionJob3,
        cfgIngestionJob, backfillJob2, cfgBackfillJob, false);
  }

  private void testLatestCheckpointCarryOverWithMultipleWriters(HoodieTableType tableType, String tempDir) throws Exception {
    // NOTE : Overriding the LockProvider to FileSystemBasedLockProviderTestClass since Zookeeper locks work in unit test but fail on Jenkins with connection timeouts
    String tableBasePath = dfsBasePath + "/" + tempDir;
    // enable carrying forward latest checkpoint
    TypedProperties props = prepareMultiWriterProps(PROPS_FILENAME_TEST_MULTI_WRITER);
    props.setProperty("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass");
    props.setProperty("hoodie.write.lock.filesystem.path", tableBasePath);
    UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_MULTI_WRITER);
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;

    HoodieDeltaStreamer.Config cfgIngestionJob = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT,
        Arrays.asList(TripsWithDistanceTransformer.class.getName()), PROPS_FILENAME_TEST_MULTI_WRITER, false);
    cfgIngestionJob.continuousMode = true;
    cfgIngestionJob.tableType = tableType.name();
    cfgIngestionJob.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfgIngestionJob.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN_PROP.key()));
    HoodieDeltaStreamer ingestionJob = new HoodieDeltaStreamer(cfgIngestionJob, jsc);

    // Prepare base dataset with some commits
    deltaStreamerTestRunner(ingestionJob, cfgIngestionJob, (r) -> {
      if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
        TestHelpers.assertAtleastNDeltaCommits(3, tableBasePath, dfs);
        TestHelpers.assertAtleastNCompactionCommits(1, tableBasePath, dfs);
      } else {
        TestHelpers.assertAtleastNCompactionCommits(3, tableBasePath, dfs);
      }
      TestHelpers.assertRecordCount(totalRecords, tableBasePath + "/*/*.parquet", sqlContext);
      TestHelpers.assertDistanceCount(totalRecords, tableBasePath + "/*/*.parquet", sqlContext);
      return true;
    });

    // create a backfill job with checkpoint from the first instant
    HoodieDeltaStreamer.Config cfgBackfillJob = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT,
        Arrays.asList(TripsWithDistanceTransformer.class.getName()), PROPS_FILENAME_TEST_MULTI_WRITER, false);
    cfgBackfillJob.continuousMode = false;
    cfgBackfillJob.tableType = tableType.name();
    HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(dfs.getConf()).setBasePath(tableBasePath).build();
    HoodieTimeline timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieCommitMetadata commitMetadataForFirstInstant = HoodieCommitMetadata
        .fromBytes(timeline.getInstantDetails(timeline.firstInstant().get()).get(), HoodieCommitMetadata.class);

    // get current checkpoint after preparing base dataset with some commits
    HoodieCommitMetadata commitMetadataForLastInstant = HoodieCommitMetadata
        .fromBytes(timeline.getInstantDetails(timeline.lastInstant().get()).get(), HoodieCommitMetadata.class);
    String lastCheckpointBeforeParallelBackfill = commitMetadataForLastInstant.getMetadata(CHECKPOINT_KEY);

    // run the backfill job, enable overriding checkpoint from the latest commit
    props = prepareMultiWriterProps(PROPS_FILENAME_TEST_MULTI_WRITER);
    props.setProperty("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.FileSystemBasedLockProviderTestClass");
    props.setProperty("hoodie.write.lock.filesystem.path", tableBasePath);
    props.setProperty("hoodie.write.meta.key.prefixes", CHECKPOINT_KEY);
    UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_MULTI_WRITER);

    // reset checkpoint to first instant to simulate a random checkpoint for backfill job
    // checkpoint will move from 00000 to 00001 for this backfill job
    cfgBackfillJob.checkpoint = commitMetadataForFirstInstant.getMetadata(CHECKPOINT_KEY);
    cfgBackfillJob.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfgBackfillJob.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN_PROP.key()));
    HoodieDeltaStreamer backfillJob = new HoodieDeltaStreamer(cfgBackfillJob, jsc);
    backfillJob.sync();

    // check if the checkpoint is carried over
    timeline = meta.getActiveTimeline().reload().getCommitsTimeline().filterCompletedInstants();
    commitMetadataForLastInstant = HoodieCommitMetadata
        .fromBytes(timeline.getInstantDetails(timeline.lastInstant().get()).get(), HoodieCommitMetadata.class);
    String lastCheckpointAfterParallelBackfill = commitMetadataForLastInstant.getMetadata(CHECKPOINT_KEY);
    Assertions.assertEquals(lastCheckpointBeforeParallelBackfill, lastCheckpointAfterParallelBackfill);
  }

  private void runJobsInParallel(String tableBasePath, HoodieTableType tableType, int totalRecords,
      HoodieDeltaStreamer ingestionJob, HoodieDeltaStreamer.Config cfgIngestionJob, HoodieDeltaStreamer backfillJob,
      HoodieDeltaStreamer.Config cfgBackfillJob, boolean expectConflict) throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(2);
    HoodieTableMetaClient meta = HoodieTableMetaClient.builder().setConf(dfs.getConf()).setBasePath(tableBasePath).build();
    HoodieTimeline timeline = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    String lastSuccessfulCommit = timeline.lastInstant().get().getTimestamp();
    // Condition for parallel ingestion job
    Function<Boolean, Boolean> conditionForRegularIngestion = (r) -> {
      if (tableType.equals(HoodieTableType.MERGE_ON_READ)) {
        TestHelpers.assertAtleastNDeltaCommitsAfterCommit(3, lastSuccessfulCommit, tableBasePath, dfs);
      } else {
        TestHelpers.assertAtleastNCompactionCommitsAfterCommit(3, lastSuccessfulCommit, tableBasePath, dfs);
      }
      TestHelpers.assertRecordCount(totalRecords, tableBasePath + "/*/*.parquet", sqlContext);
      TestHelpers.assertDistanceCount(totalRecords, tableBasePath + "/*/*.parquet", sqlContext);
      return true;
    };

    try {
      Future regularIngestionJobFuture = service.submit(() -> {
        try {
          deltaStreamerTestRunner(ingestionJob, cfgIngestionJob, conditionForRegularIngestion);
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      });
      Future backfillJobFuture = service.submit(() -> {
        try {
          backfillJob.sync();
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      });
      backfillJobFuture.get();
      regularIngestionJobFuture.get();
      if (expectConflict) {
        Assertions.fail("Failed to handle concurrent writes");
      }
    } catch (Exception e) {
      /**
       * Need to perform getMessage().contains since the exception coming
       * from {@link org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer.DeltaSyncService} gets wrapped many times into RuntimeExceptions.
       */
      if (expectConflict && e.getCause().getMessage().contains(ConcurrentModificationException.class.getName())) {
        // expected ConcurrentModificationException since ingestion & backfill will have overlapping writes
      } else {
        throw e;
      }
    }
  }

  private void deltaStreamerTestRunner(HoodieDeltaStreamer ds, HoodieDeltaStreamer.Config cfg, Function<Boolean, Boolean> condition) throws Exception {
    Future dsFuture = Executors.newSingleThreadExecutor().submit(() -> {
      try {
        ds.sync();
      } catch (Exception ex) {
        throw new RuntimeException(ex.getMessage(), ex);
      }
    });

    TestHelpers.waitTillCondition(condition, dsFuture, 360);
    ds.shutdownGracefully();
    dsFuture.get();
  }

  @Test
  public void testInlineClustering() throws Exception {
    String tableBasePath = dfsBasePath + "/inlineClustering";
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    cfg.configs.addAll(getAsyncServicesConfigs(totalRecords, "false", "true", "2", "", ""));
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      TestHelpers.assertAtLeastNCommits(2, tableBasePath, dfs);
      TestHelpers.assertAtLeastNReplaceCommits(1, tableBasePath, dfs);
      return true;
    });
  }

  private List<String> getAsyncServicesConfigs(int totalRecords, String autoClean, String inlineCluster,
                                               String inlineClusterMaxCommit, String asyncCluster, String asyncClusterMaxCommit) {
    List<String> configs = new ArrayList<>();
    configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    if (!StringUtils.isNullOrEmpty(autoClean)) {
      configs.add(String.format("%s=%s", HoodieCompactionConfig.AUTO_CLEAN_PROP.key(), autoClean));
    }
    if (!StringUtils.isNullOrEmpty(inlineCluster)) {
      configs.add(String.format("%s=%s", HoodieClusteringConfig.INLINE_CLUSTERING_PROP.key(), inlineCluster));
    }
    if (!StringUtils.isNullOrEmpty(inlineClusterMaxCommit)) {
      configs.add(String.format("%s=%s", HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMIT_PROP.key(), inlineClusterMaxCommit));
    }
    if (!StringUtils.isNullOrEmpty(asyncCluster)) {
      configs.add(String.format("%s=%s", HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE_OPT_KEY.key(), asyncCluster));
    }
    if (!StringUtils.isNullOrEmpty(asyncClusterMaxCommit)) {
      configs.add(String.format("%s=%s", HoodieClusteringConfig.ASYNC_CLUSTERING_MAX_COMMIT_PROP.key(), asyncClusterMaxCommit));
    }
    return configs;
  }

  private HoodieClusteringJob.Config buildHoodieClusteringUtilConfig(String basePath,
                                                                  String clusteringInstantTime, boolean runSchedule) {
    HoodieClusteringJob.Config config = new HoodieClusteringJob.Config();
    config.basePath = basePath;
    config.clusteringInstantTime = clusteringInstantTime;
    config.runSchedule = runSchedule;
    config.propsFilePath = dfsBasePath + "/clusteringjob.properties";
    return config;
  }

  @Test
  public void testHoodieAsyncClusteringJob() throws Exception {
    String tableBasePath = dfsBasePath + "/asyncClustering";
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT);
    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.configs.addAll(getAsyncServicesConfigs(totalRecords, "false", "", "", "true", ""));
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      TestHelpers.assertAtLeastNCommits(2, tableBasePath, dfs);
      HoodieClusteringJob.Config scheduleClusteringConfig = buildHoodieClusteringUtilConfig(tableBasePath,
          null, true);
      HoodieClusteringJob scheduleClusteringJob = new HoodieClusteringJob(jsc, scheduleClusteringConfig);
      Option<String> scheduleClusteringInstantTime = Option.empty();
      try {
        scheduleClusteringInstantTime = scheduleClusteringJob.doSchedule();
      } catch (Exception e) {
        LOG.warn("Schedule clustering failed", e);
        return false;
      }
      if (scheduleClusteringInstantTime.isPresent()) {
        LOG.info("Schedule clustering success, now cluster with instant time " + scheduleClusteringInstantTime.get());
        HoodieClusteringJob.Config clusterClusteringConfig = buildHoodieClusteringUtilConfig(tableBasePath,
            scheduleClusteringInstantTime.get(), false);
        HoodieClusteringJob clusterClusteringJob = new HoodieClusteringJob(jsc, clusterClusteringConfig);
        clusterClusteringJob.cluster(clusterClusteringConfig.retry);
        LOG.info("Cluster success");
      } else {
        LOG.warn("Schedule clustering failed");
      }
      TestHelpers.assertAtLeastNReplaceCommits(2, tableBasePath, dfs);
      return true;
    });
  }

  @Test
  public void testAsyncClusteringService() throws Exception {
    String tableBasePath = dfsBasePath + "/asyncClustering";
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT);
    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.configs.addAll(getAsyncServicesConfigs(totalRecords, "false", "", "", "true", "2"));
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      TestHelpers.assertAtLeastNCommits(2, tableBasePath, dfs);
      TestHelpers.assertAtLeastNReplaceCommits(2, tableBasePath, dfs);
      return true;
    });
  }

  @Test
  public void testAsyncClusteringServiceWithCompaction() throws Exception {
    String tableBasePath = dfsBasePath + "/asyncClusteringCompaction";
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT);
    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.MERGE_ON_READ.name();
    cfg.configs.addAll(getAsyncServicesConfigs(totalRecords, "false", "", "", "true", "2"));
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      TestHelpers.assertAtLeastNCommits(2, tableBasePath, dfs);
      TestHelpers.assertAtleastNCompactionCommits(2, tableBasePath, dfs);
      TestHelpers.assertAtLeastNReplaceCommits(2, tableBasePath, dfs);
      return true;
    });
  }

  @Test
  public void testHoodieAsyncClusteringJobWithScheduleAndExecute() throws Exception {
    String tableBasePath = dfsBasePath + "/asyncClustering2";
    // Keep it higher than batch-size to test continuous mode
    int totalRecords = 3000;

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT);
    cfg.continuousMode = true;
    cfg.tableType = HoodieTableType.COPY_ON_WRITE.name();
    cfg.configs.add(String.format("%s=%d", SourceConfigs.MAX_UNIQUE_RECORDS_PROP, totalRecords));
    cfg.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN_PROP.key()));
    cfg.configs.add(String.format("%s=true", HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE_OPT_KEY.key()));
    HoodieDeltaStreamer ds = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamerTestRunner(ds, cfg, (r) -> {
      TestHelpers.assertAtLeastNCommits(2, tableBasePath, dfs);
      HoodieClusteringJob.Config scheduleClusteringConfig = buildHoodieClusteringUtilConfig(tableBasePath,
              null, true);
      scheduleClusteringConfig.runningMode = "scheduleAndExecute";
      HoodieClusteringJob scheduleClusteringJob = new HoodieClusteringJob(jsc, scheduleClusteringConfig);

      try {
        int result = scheduleClusteringJob.doScheduleAndCluster(jsc);
        if (result == 0) {
          LOG.info("Cluster success");
        } else {
          LOG.warn("Import failed");
          return false;
        }
      } catch (Exception e) {
        LOG.warn("ScheduleAndExecute clustering failed", e);
        return false;
      }

      TestHelpers.assertAtLeastNCompletedReplaceCommits(0, tableBasePath, dfs);
      return true;
    });
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(this.dfs.getConf()).setBasePath(tableBasePath).setLoadActiveTimelineOnLoad(true).build();
    assertEquals(1, metaClient.getActiveTimeline().getCompletedReplaceTimeline().getInstants().toArray().length);
  }

  /**
   * Test Bulk Insert and upserts with hive syncing. Tests Hudi incremental processing using a 2 step pipeline The first
   * step involves using a SQL template to transform a source TEST-DATA-SOURCE ============================> HUDI TABLE
   * 1 ===============> HUDI TABLE 2 (incr-pull with transform) (incr-pull) Hudi Table 1 is synced with Hive.
   */
  @Test
  public void testBulkInsertsAndUpsertsWithSQLBasedTransformerFor2StepPipeline() throws Exception {
    String tableBasePath = dfsBasePath + "/test_table2";
    String downstreamTableBasePath = dfsBasePath + "/test_downstream_table2";

    HiveSyncConfig hiveSyncConfig = getHiveSyncConfig(tableBasePath, "hive_trips");

    // Initial bulk insert to ingest to first hudi table
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, true);
    new HoodieDeltaStreamer(cfg, jsc, dfs, hiveServer.getHiveConf()).sync();
    TestHelpers.assertRecordCount(1000, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCount(1000, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCountWithExactValue(1000, tableBasePath + "/*/*.parquet", sqlContext);
    String lastInstantForUpstreamTable = TestHelpers.assertCommitMetadata("00000", tableBasePath, dfs, 1);

    // Now incrementally pull from the above hudi table and ingest to second table
    HoodieDeltaStreamer.Config downstreamCfg =
        TestHelpers.makeConfigForHudiIncrSrc(tableBasePath, downstreamTableBasePath, WriteOperationType.BULK_INSERT,
            true, null);
    new HoodieDeltaStreamer(downstreamCfg, jsc, dfs, hiveServer.getHiveConf()).sync();
    TestHelpers.assertRecordCount(1000, downstreamTableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCount(1000, downstreamTableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCountWithExactValue(1000, downstreamTableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertCommitMetadata(lastInstantForUpstreamTable, downstreamTableBasePath, dfs, 1);

    // No new data => no commits for upstream table
    cfg.sourceLimit = 0;
    new HoodieDeltaStreamer(cfg, jsc, dfs, hiveServer.getHiveConf()).sync();
    TestHelpers.assertRecordCount(1000, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCount(1000, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCountWithExactValue(1000, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, dfs, 1);

    // with no change in upstream table, no change in downstream too when pulled.
    HoodieDeltaStreamer.Config downstreamCfg1 =
        TestHelpers.makeConfigForHudiIncrSrc(tableBasePath, downstreamTableBasePath,
            WriteOperationType.BULK_INSERT, true, DummySchemaProvider.class.getName());
    new HoodieDeltaStreamer(downstreamCfg1, jsc).sync();
    TestHelpers.assertRecordCount(1000, downstreamTableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCount(1000, downstreamTableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCountWithExactValue(1000, downstreamTableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertCommitMetadata(lastInstantForUpstreamTable, downstreamTableBasePath, dfs, 1);

    // upsert() #1 on upstream hudi table
    cfg.sourceLimit = 2000;
    cfg.operation = WriteOperationType.UPSERT;
    new HoodieDeltaStreamer(cfg, jsc, dfs, hiveServer.getHiveConf()).sync();
    TestHelpers.assertRecordCount(1950, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCount(1950, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCountWithExactValue(1950, tableBasePath + "/*/*.parquet", sqlContext);
    lastInstantForUpstreamTable = TestHelpers.assertCommitMetadata("00001", tableBasePath, dfs, 2);
    List<Row> counts = TestHelpers.countsPerCommit(tableBasePath + "/*/*.parquet", sqlContext);
    assertEquals(1950, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());

    // Incrementally pull changes in upstream hudi table and apply to downstream table
    downstreamCfg =
        TestHelpers.makeConfigForHudiIncrSrc(tableBasePath, downstreamTableBasePath, WriteOperationType.UPSERT,
            false, null);
    downstreamCfg.sourceLimit = 2000;
    new HoodieDeltaStreamer(downstreamCfg, jsc).sync();
    TestHelpers.assertRecordCount(2000, downstreamTableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCount(2000, downstreamTableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertDistanceCountWithExactValue(2000, downstreamTableBasePath + "/*/*.parquet", sqlContext);
    String finalInstant =
        TestHelpers.assertCommitMetadata(lastInstantForUpstreamTable, downstreamTableBasePath, dfs, 2);
    counts = TestHelpers.countsPerCommit(downstreamTableBasePath + "/*/*.parquet", sqlContext);
    assertEquals(2000, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());

    // Test Hive integration
    HoodieHiveClient hiveClient = new HoodieHiveClient(hiveSyncConfig, hiveServer.getHiveConf(), dfs);
    assertTrue(hiveClient.doesTableExist(hiveSyncConfig.tableName), "Table " + hiveSyncConfig.tableName + " should exist");
    assertEquals(1, hiveClient.scanTablePartitions(hiveSyncConfig.tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(lastInstantForUpstreamTable,
        hiveClient.getLastCommitTimeSynced(hiveSyncConfig.tableName).get(),
        "The last commit that was sycned should be updated in the TBLPROPERTIES");
  }

  @Test
  public void testNullSchemaProvider() throws Exception {
    String tableBasePath = dfsBasePath + "/test_table";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, true,
        false, false, null, null);
    Exception e = assertThrows(HoodieException.class, () -> {
      new HoodieDeltaStreamer(cfg, jsc, dfs, hiveServer.getHiveConf()).sync();
    }, "Should error out when schema provider is not provided");
    LOG.debug("Expected error during reading data from source ", e);
    assertTrue(e.getMessage().contains("Please provide a valid schema provider class!"));
  }

  @Test
  public void testPayloadClassUpdate() throws Exception {
    String dataSetBasePath = dfsBasePath + "/test_dataset_mor";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(dataSetBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, true,
        true, false, null, "MERGE_ON_READ");
    new HoodieDeltaStreamer(cfg, jsc, dfs, hiveServer.getHiveConf()).sync();
    TestHelpers.assertRecordCount(1000, dataSetBasePath + "/*/*.parquet", sqlContext);

    //now create one more deltaStreamer instance and update payload class
    cfg = TestHelpers.makeConfig(dataSetBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, true,
        true, true, DummyAvroPayload.class.getName(), "MERGE_ON_READ");
    new HoodieDeltaStreamer(cfg, jsc, dfs, hiveServer.getHiveConf());

    //now assert that hoodie.properties file now has updated payload class name
    Properties props = new Properties();
    String metaPath = dataSetBasePath + "/.hoodie/hoodie.properties";
    FileSystem fs = FSUtils.getFs(cfg.targetBasePath, jsc.hadoopConfiguration());
    try (FSDataInputStream inputStream = fs.open(new Path(metaPath))) {
      props.load(inputStream);
    }

    assertEquals(new HoodieConfig(props).getString(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP), DummyAvroPayload.class.getName());
  }

  @Test
  public void testPayloadClassUpdateWithCOWTable() throws Exception {
    String dataSetBasePath = dfsBasePath + "/test_dataset_cow";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(dataSetBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, true,
        true, false, null, null);
    new HoodieDeltaStreamer(cfg, jsc, dfs, hiveServer.getHiveConf()).sync();
    TestHelpers.assertRecordCount(1000, dataSetBasePath + "/*/*.parquet", sqlContext);

    //now create one more deltaStreamer instance and update payload class
    cfg = TestHelpers.makeConfig(dataSetBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, true,
        true, true, DummyAvroPayload.class.getName(), null);
    new HoodieDeltaStreamer(cfg, jsc, dfs, hiveServer.getHiveConf());

    //now assert that hoodie.properties file does not have payload class prop since it is a COW table
    Properties props = new Properties();
    String metaPath = dataSetBasePath + "/.hoodie/hoodie.properties";
    FileSystem fs = FSUtils.getFs(cfg.targetBasePath, jsc.hadoopConfiguration());
    try (FSDataInputStream inputStream = fs.open(new Path(metaPath))) {
      props.load(inputStream);
    }

    assertFalse(props.containsKey(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP.key()));
  }

  @Test
  public void testFilterDupes() throws Exception {
    String tableBasePath = dfsBasePath + "/test_dupes_table";

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    new HoodieDeltaStreamer(cfg, jsc).sync();
    TestHelpers.assertRecordCount(1000, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, dfs, 1);

    // Generate the same 1000 records + 1000 new ones for upsert
    cfg.filterDupes = true;
    cfg.sourceLimit = 2000;
    cfg.operation = WriteOperationType.INSERT;
    new HoodieDeltaStreamer(cfg, jsc).sync();
    TestHelpers.assertRecordCount(2000, tableBasePath + "/*/*.parquet", sqlContext);
    TestHelpers.assertCommitMetadata("00001", tableBasePath, dfs, 2);
    // 1000 records for commit 00000 & 1000 for commit 00001
    List<Row> counts = TestHelpers.countsPerCommit(tableBasePath + "/*/*.parquet", sqlContext);
    assertEquals(1000, counts.get(0).getLong(1));
    assertEquals(1000, counts.get(1).getLong(1));

    // Test with empty commits
    HoodieTableMetaClient mClient = HoodieTableMetaClient.builder().setConf(jsc.hadoopConfiguration()).setBasePath(tableBasePath).setLoadActiveTimelineOnLoad(true).build();
    HoodieInstant lastFinished = mClient.getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieDeltaStreamer.Config cfg2 = TestHelpers.makeDropAllConfig(tableBasePath, WriteOperationType.UPSERT);
    cfg2.filterDupes = false;
    cfg2.sourceLimit = 2000;
    cfg2.operation = WriteOperationType.UPSERT;
    cfg2.configs.add(String.format("%s=false", HoodieCompactionConfig.AUTO_CLEAN_PROP.key()));
    HoodieDeltaStreamer ds2 = new HoodieDeltaStreamer(cfg2, jsc);
    ds2.sync();
    mClient = HoodieTableMetaClient.builder().setConf(jsc.hadoopConfiguration()).setBasePath(tableBasePath).setLoadActiveTimelineOnLoad(true).build();
    HoodieInstant newLastFinished = mClient.getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    assertTrue(HoodieTimeline.compareTimestamps(newLastFinished.getTimestamp(), HoodieTimeline.GREATER_THAN, lastFinished.getTimestamp()
    ));

    // Ensure it is empty
    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
        .fromBytes(mClient.getActiveTimeline().getInstantDetails(newLastFinished).get(), HoodieCommitMetadata.class);
    System.out.println("New Commit Metadata=" + commitMetadata);
    assertTrue(commitMetadata.getPartitionToWriteStats().isEmpty());

    // Try UPSERT with filterDupes true. Expect exception
    cfg2.filterDupes = true;
    cfg2.operation = WriteOperationType.UPSERT;
    try {
      new HoodieDeltaStreamer(cfg2, jsc).sync();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("'--filter-dupes' needs to be disabled when '--op' is 'UPSERT' to ensure updates are not missed."));
    }

  }

  @Test
  public void testDistributedTestDataSource() {
    TypedProperties props = new TypedProperties();
    props.setProperty(SourceConfigs.MAX_UNIQUE_RECORDS_PROP, "1000");
    props.setProperty(SourceConfigs.NUM_SOURCE_PARTITIONS_PROP, "1");
    props.setProperty(SourceConfigs.USE_ROCKSDB_FOR_TEST_DATAGEN_KEYS, "true");
    DistributedTestDataSource distributedTestDataSource = new DistributedTestDataSource(props, jsc, sparkSession, null);
    InputBatch<JavaRDD<GenericRecord>> batch = distributedTestDataSource.fetchNext(Option.empty(), 10000000);
    batch.getBatch().get().cache();
    long c = batch.getBatch().get().count();
    assertEquals(1000, c);
  }

  private static void prepareParquetDFSFiles(int numRecords) throws IOException {
    prepareParquetDFSFiles(numRecords, PARQUET_SOURCE_ROOT);
  }

  protected static void prepareParquetDFSFiles(int numRecords, String baseParquetPath) throws IOException {
    prepareParquetDFSFiles(numRecords, baseParquetPath, FIRST_PARQUET_FILE_NAME, false, null, null);
  }

  protected static void prepareParquetDFSFiles(int numRecords, String baseParquetPath, String fileName, boolean useCustomSchema,
      String schemaStr, Schema schema) throws IOException {
    String path = baseParquetPath + "/" + fileName;
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    if (useCustomSchema) {
      Helpers.saveParquetToDFS(Helpers.toGenericRecords(
          dataGenerator.generateInsertsAsPerSchema("000", numRecords, schemaStr),
          schema), new Path(path), HoodieTestDataGenerator.AVRO_TRIP_SCHEMA);
    } else {
      Helpers.saveParquetToDFS(Helpers.toGenericRecords(
          dataGenerator.generateInserts("000", numRecords)), new Path(path));
    }
  }

  private static void prepareJsonKafkaDFSFiles(int numRecords, boolean createTopic, String topicName) throws IOException {
    if (createTopic) {
      try {
        testUtils.createTopic(topicName, 2);
      } catch (TopicExistsException e) {
        // no op
      }
    }
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    testUtils.sendMessages(topicName, Helpers.jsonifyRecords(dataGenerator.generateInsertsAsPerSchema("000", numRecords, HoodieTestDataGenerator.TRIP_SCHEMA)));
  }

  private void prepareParquetDFSSource(boolean useSchemaProvider, boolean hasTransformer) throws IOException {
    prepareParquetDFSSource(useSchemaProvider, hasTransformer, "source.avsc", "target.avsc",
        PROPS_FILENAME_TEST_PARQUET, PARQUET_SOURCE_ROOT, false);
  }

  private void prepareParquetDFSSource(boolean useSchemaProvider, boolean hasTransformer, String sourceSchemaFile, String targetSchemaFile,
      String propsFileName, String parquetSourceRoot, boolean addCommonProps) throws IOException {
    // Properties used for testing delta-streamer with Parquet source
    TypedProperties parquetProps = new TypedProperties();

    if (addCommonProps) {
      populateCommonProps(parquetProps);
    }

    parquetProps.setProperty("include", "base.properties");
    parquetProps.setProperty("hoodie.embed.timeline.server","false");
    parquetProps.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    parquetProps.setProperty("hoodie.datasource.write.partitionpath.field", "not_there");
    if (useSchemaProvider) {
      parquetProps.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/" + sourceSchemaFile);
      if (hasTransformer) {
        parquetProps.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/" + targetSchemaFile);
      }
    }
    parquetProps.setProperty("hoodie.deltastreamer.source.dfs.root", parquetSourceRoot);
    UtilitiesTestBase.Helpers.savePropsToDFS(parquetProps, dfs, dfsBasePath + "/" + propsFileName);
  }

  private void testParquetDFSSource(boolean useSchemaProvider, List<String> transformerClassNames) throws Exception {
    prepareParquetDFSSource(useSchemaProvider, transformerClassNames != null);
    String tableBasePath = dfsBasePath + "/test_parquet_table" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, ParquetDFSSource.class.getName(),
            transformerClassNames, PROPS_FILENAME_TEST_PARQUET, false,
            useSchemaProvider, 100000, false, null, null, "timestamp"), jsc);
    deltaStreamer.sync();
    TestHelpers.assertRecordCount(PARQUET_NUM_RECORDS, tableBasePath + "/*/*.parquet", sqlContext);
    testNum++;
  }

  private void prepareJsonKafkaDFSSource(String propsFileName, String autoResetValue, String topicName) throws IOException {
    // Properties used for testing delta-streamer with JsonKafka source
    TypedProperties props = new TypedProperties();
    populateAllCommonProps(props);
    props.setProperty("include", "base.properties");
    props.setProperty("hoodie.embed.timeline.server","false");
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "not_there");
    props.setProperty("hoodie.deltastreamer.source.dfs.root", JSON_KAFKA_SOURCE_ROOT);
    props.setProperty("hoodie.deltastreamer.source.kafka.topic",topicName);
    props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source_uber.avsc");
    props.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target_uber.avsc");
    props.setProperty(Config.KAFKA_AUTO_OFFSET_RESET, autoResetValue);

    UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/" + propsFileName);
  }

  /**
   * Tests Deltastreamer with parquet dfs source and transitions to JsonKafkaSource.
   * @param autoResetToLatest true if auto reset value to be set to LATEST. false to leave it as default(i.e. EARLIEST)
   * @throws Exception
   */
  private void testDeltaStreamerTransitionFromParquetToKafkaSource(boolean autoResetToLatest) throws Exception {
    // prep parquet source
    PARQUET_SOURCE_ROOT = dfsBasePath + "/parquetFilesDfsToKafka" + testNum;
    int parquetRecords = 10;
    prepareParquetDFSFiles(parquetRecords, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, true, HoodieTestDataGenerator.TRIP_SCHEMA, HoodieTestDataGenerator.AVRO_TRIP_SCHEMA);

    prepareParquetDFSSource(true, false,"source_uber.avsc", "target_uber.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false);
    // delta streamer w/ parquest source
    String tableBasePath = dfsBasePath + "/test_dfs_to_kakfa" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, ParquetDFSSource.class.getName(),
            Collections.EMPTY_LIST, PROPS_FILENAME_TEST_PARQUET, false,
            false, 100000, false, null, null, "timestamp"), jsc);
    deltaStreamer.sync();
    TestHelpers.assertRecordCount(parquetRecords, tableBasePath + "/*/*.parquet", sqlContext);
    deltaStreamer.shutdownGracefully();

    // prep json kafka source
    topicName = "topic" + testNum;
    prepareJsonKafkaDFSFiles(JSON_KAFKA_NUM_RECORDS, true, topicName);
    prepareJsonKafkaDFSSource(PROPS_FILENAME_TEST_JSON_KAFKA, autoResetToLatest ? "latest" : "earliest", topicName);
    // delta streamer w/ json kafka source
    deltaStreamer = new HoodieDeltaStreamer(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, JsonKafkaSource.class.getName(),
            Collections.EMPTY_LIST, PROPS_FILENAME_TEST_JSON_KAFKA, false,
            true, 100000, false, null, null, "timestamp"), jsc);
    deltaStreamer.sync();
    // if auto reset value is set to LATEST, this all kafka records so far may not be synced.
    int totalExpectedRecords = parquetRecords + ((autoResetToLatest) ? 0 : JSON_KAFKA_NUM_RECORDS);
    TestHelpers.assertRecordCount(totalExpectedRecords, tableBasePath + "/*/*.parquet", sqlContext);

    // verify 2nd batch to test LATEST auto reset value.
    prepareJsonKafkaDFSFiles(20, false, topicName);
    totalExpectedRecords += 20;
    deltaStreamer.sync();
    TestHelpers.assertRecordCount(totalExpectedRecords, tableBasePath + "/*/*.parquet", sqlContext);
    testNum++;
  }

  @Test
  public void testJsonKafkaDFSSource() throws Exception {
    topicName = "topic" + testNum;
    prepareJsonKafkaDFSFiles(JSON_KAFKA_NUM_RECORDS, true, topicName);
    prepareJsonKafkaDFSSource(PROPS_FILENAME_TEST_JSON_KAFKA, "earliest",topicName);
    String tableBasePath = dfsBasePath + "/test_json_kafka_table" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, JsonKafkaSource.class.getName(),
            Collections.EMPTY_LIST, PROPS_FILENAME_TEST_JSON_KAFKA, false,
            true, 100000, false, null, null, "timestamp"), jsc);
    deltaStreamer.sync();
    TestHelpers.assertRecordCount(JSON_KAFKA_NUM_RECORDS, tableBasePath + "/*/*.parquet", sqlContext);

    int totalRecords = JSON_KAFKA_NUM_RECORDS;
    int records = 10;
    totalRecords += records;
    prepareJsonKafkaDFSFiles(records, false, topicName);
    deltaStreamer.sync();
    TestHelpers.assertRecordCount(totalRecords, tableBasePath + "/*/*.parquet", sqlContext);
  }

  @Test
  public void testParquetSourceToKafkaSourceEarliestAutoResetValue() throws Exception {
    testDeltaStreamerTransitionFromParquetToKafkaSource(false);
  }

  @Test
  public void testParquetSourceToKafkaSourceLatestAutoResetValue() throws Exception {
    testDeltaStreamerTransitionFromParquetToKafkaSource(true);
  }

  @Test
  public void testParquetDFSSourceWithoutSchemaProviderAndNoTransformer() throws Exception {
    testParquetDFSSource(false, null);
  }

  @Test
  public void testParquetDFSSourceWithoutSchemaProviderAndTransformer() throws Exception {
    testParquetDFSSource(false, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
  }

  @Test
  public void testParquetDFSSourceWithSourceSchemaFileAndNoTransformer() throws Exception {
    testParquetDFSSource(true, null);
  }

  @Test
  public void testParquetDFSSourceWithSchemaFilesAndTransformer() throws Exception {
    testParquetDFSSource(true, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
  }

  private void prepareCsvDFSSource(
      boolean hasHeader, char sep, boolean useSchemaProvider, boolean hasTransformer) throws IOException {
    String sourceRoot = dfsBasePath + "/csvFiles";
    String recordKeyField = (hasHeader || useSchemaProvider) ? "_row_key" : "_c0";

    // Properties used for testing delta-streamer with CSV source
    TypedProperties csvProps = new TypedProperties();
    csvProps.setProperty("include", "base.properties");
    csvProps.setProperty("hoodie.datasource.write.recordkey.field", recordKeyField);
    csvProps.setProperty("hoodie.datasource.write.partitionpath.field", "not_there");
    if (useSchemaProvider) {
      csvProps.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source-flattened.avsc");
      if (hasTransformer) {
        csvProps.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target-flattened.avsc");
      }
    }
    csvProps.setProperty("hoodie.deltastreamer.source.dfs.root", sourceRoot);

    if (sep != ',') {
      if (sep == '\t') {
        csvProps.setProperty("hoodie.deltastreamer.csv.sep", "\\t");
      } else {
        csvProps.setProperty("hoodie.deltastreamer.csv.sep", Character.toString(sep));
      }
    }
    if (hasHeader) {
      csvProps.setProperty("hoodie.deltastreamer.csv.header", Boolean.toString(hasHeader));
    }

    UtilitiesTestBase.Helpers.savePropsToDFS(csvProps, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_CSV);

    String path = sourceRoot + "/1.csv";
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    UtilitiesTestBase.Helpers.saveCsvToDFS(
        hasHeader, sep,
        Helpers.jsonifyRecords(dataGenerator.generateInserts("000", CSV_NUM_RECORDS, true)),
        dfs, path);
  }

  private void testCsvDFSSource(
      boolean hasHeader, char sep, boolean useSchemaProvider, List<String> transformerClassNames) throws Exception {
    prepareCsvDFSSource(hasHeader, sep, useSchemaProvider, transformerClassNames != null);
    String tableBasePath = dfsBasePath + "/test_csv_table" + testNum;
    String sourceOrderingField = (hasHeader || useSchemaProvider) ? "timestamp" : "_c0";
    HoodieDeltaStreamer deltaStreamer =
        new HoodieDeltaStreamer(TestHelpers.makeConfig(
            tableBasePath, WriteOperationType.INSERT, CsvDFSSource.class.getName(),
            transformerClassNames, PROPS_FILENAME_TEST_CSV, false,
            useSchemaProvider, 1000, false, null, null, sourceOrderingField), jsc);
    deltaStreamer.sync();
    TestHelpers.assertRecordCount(CSV_NUM_RECORDS, tableBasePath + "/*/*.parquet", sqlContext);
    testNum++;
  }

  @Test
  public void testCsvDFSSourceWithHeaderWithoutSchemaProviderAndNoTransformer() throws Exception {
    // The CSV files have header, the columns are separated by ',', the default separator
    // No schema provider is specified, no transformer is applied
    // In this case, the source schema comes from the inferred schema of the CSV files
    testCsvDFSSource(true, ',', false, null);
  }

  @Test
  public void testCsvDFSSourceWithHeaderAndSepWithoutSchemaProviderAndNoTransformer() throws Exception {
    // The CSV files have header, the columns are separated by '\t',
    // which is passed in through the Hudi CSV properties
    // No schema provider is specified, no transformer is applied
    // In this case, the source schema comes from the inferred schema of the CSV files
    testCsvDFSSource(true, '\t', false, null);
  }

  @Test
  public void testCsvDFSSourceWithHeaderAndSepWithSchemaProviderAndNoTransformer() throws Exception {
    // The CSV files have header, the columns are separated by '\t'
    // File schema provider is used, no transformer is applied
    // In this case, the source schema comes from the source Avro schema file
    testCsvDFSSource(true, '\t', true, null);
  }

  @Test
  public void testCsvDFSSourceWithHeaderAndSepWithoutSchemaProviderAndWithTransformer() throws Exception {
    // The CSV files have header, the columns are separated by '\t'
    // No schema provider is specified, transformer is applied
    // In this case, the source schema comes from the inferred schema of the CSV files.
    // Target schema is determined based on the Dataframe after transformation
    testCsvDFSSource(true, '\t', false, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
  }

  @Test
  public void testCsvDFSSourceWithHeaderAndSepWithSchemaProviderAndTransformer() throws Exception {
    // The CSV files have header, the columns are separated by '\t'
    // File schema provider is used, transformer is applied
    // In this case, the source and target schema come from the Avro schema files
    testCsvDFSSource(true, '\t', true, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
  }

  @Test
  public void testCsvDFSSourceNoHeaderWithoutSchemaProviderAndNoTransformer() throws Exception {
    // The CSV files do not have header, the columns are separated by '\t',
    // which is passed in through the Hudi CSV properties
    // No schema provider is specified, no transformer is applied
    // In this case, the source schema comes from the inferred schema of the CSV files
    // No CSV header and no schema provider at the same time are not recommended
    // as the column names are not informative
    testCsvDFSSource(false, '\t', false, null);
  }

  @Test
  public void testCsvDFSSourceNoHeaderWithSchemaProviderAndNoTransformer() throws Exception {
    // The CSV files do not have header, the columns are separated by '\t'
    // File schema provider is used, no transformer is applied
    // In this case, the source schema comes from the source Avro schema file
    testCsvDFSSource(false, '\t', true, null);
  }

  @Test
  public void testCsvDFSSourceNoHeaderWithoutSchemaProviderAndWithTransformer() throws Exception {
    // The CSV files do not have header, the columns are separated by '\t'
    // No schema provider is specified, transformer is applied
    // In this case, the source schema comes from the inferred schema of the CSV files.
    // Target schema is determined based on the Dataframe after transformation
    // No CSV header and no schema provider at the same time are not recommended,
    // as the transformer behavior may be unexpected
    Exception e = assertThrows(AnalysisException.class, () -> {
      testCsvDFSSource(false, '\t', false, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
    }, "Should error out when doing the transformation.");
    LOG.debug("Expected error during transformation", e);
    assertTrue(e.getMessage().contains("cannot resolve '`begin_lat`' given input columns:"));
  }

  @Test
  public void testCsvDFSSourceNoHeaderWithSchemaProviderAndTransformer() throws Exception {
    // The CSV files do not have header, the columns are separated by '\t'
    // File schema provider is used, transformer is applied
    // In this case, the source and target schema come from the Avro schema files
    testCsvDFSSource(false, '\t', true, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
  }

  @Test
  public void testJdbcSourceIncrementalFetchInContinuousMode() {
    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test_mem", "test", "jdbc")) {
      TypedProperties props = new TypedProperties();
      props.setProperty("hoodie.deltastreamer.jdbc.url", "jdbc:h2:mem:test_mem");
      props.setProperty("hoodie.deltastreamer.jdbc.driver.class", "org.h2.Driver");
      props.setProperty("hoodie.deltastreamer.jdbc.user", "test");
      props.setProperty("hoodie.deltastreamer.jdbc.password", "jdbc");
      props.setProperty("hoodie.deltastreamer.jdbc.table.name", "triprec");
      props.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "true");
      props.setProperty("hoodie.deltastreamer.jdbc.table.incr.column.name", "id");

      props.setProperty("hoodie.datasource.write.keygenerator.class", SimpleKeyGenerator.class.getName());
      props.setProperty("hoodie.datasource.write.recordkey.field", "ID");
      props.setProperty("hoodie.datasource.write.partitionpath.field", "not_there");

      UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/test-jdbc-source.properties");

      int numRecords = 1000;
      int sourceLimit = 100;
      String tableBasePath = dfsBasePath + "/triprec";
      HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, JdbcSource.class.getName(),
          null, "test-jdbc-source.properties", false,
          false, sourceLimit, false, null, null, "timestamp");
      cfg.continuousMode = true;
      // Add 1000 records
      JdbcTestUtils.clearAndInsert("000", numRecords, connection, new HoodieTestDataGenerator(), props);

      HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(cfg, jsc);
      deltaStreamerTestRunner(deltaStreamer, cfg, (r) -> {
        TestHelpers.assertAtleastNCompactionCommits(numRecords / sourceLimit + ((numRecords % sourceLimit == 0) ? 0 : 1), tableBasePath, dfs);
        TestHelpers.assertRecordCount(numRecords, tableBasePath + "/*/*.parquet", sqlContext);
        return true;
      });
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  /**
   * UDF to calculate Haversine distance.
   */
  public static class DistanceUDF implements UDF4<Double, Double, Double, Double, Double> {

    /**
     * Returns some random number as distance between the points.
     *
     * @param lat1 Latitiude of source
     * @param lat2 Latitude of destination
     * @param lon1 Longitude of source
     * @param lon2 Longitude of destination
     */
    @Override
    public Double call(Double lat1, Double lat2, Double lon1, Double lon2) {
      return RANDOM.nextDouble();
    }
  }

  /**
   * Adds a new field "haversine_distance" to the row.
   */
  public static class TripsWithDistanceTransformer implements Transformer {

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                              TypedProperties properties) {
      rowDataset.sqlContext().udf().register("distance_udf", new DistanceUDF(), DataTypes.DoubleType);
      return rowDataset.withColumn("haversine_distance", functions.callUDF("distance_udf", functions.col("begin_lat"),
          functions.col("end_lat"), functions.col("begin_lon"), functions.col("end_lat")));
    }
  }

  public static class TestGenerator extends SimpleKeyGenerator {

    public TestGenerator(TypedProperties props) {
      super(props);
    }
  }

  public static class TestTableLevelGenerator extends SimpleKeyGenerator {

    public TestTableLevelGenerator(TypedProperties props) {
      super(props);
    }
  }

  public static class DummyAvroPayload extends OverwriteWithLatestAvroPayload {

    public DummyAvroPayload(GenericRecord gr, Comparable orderingVal) {
      super(gr, orderingVal);
    }
  }

  /**
   * Return empty table.
   */
  public static class DropAllTransformer implements Transformer {

    @Override
    public Dataset apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                         TypedProperties properties) {
      System.out.println("DropAllTransformer called !!");
      return sparkSession.createDataFrame(jsc.emptyRDD(), rowDataset.schema());
    }
  }

  public static class TestIdentityTransformer implements Transformer {

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
        TypedProperties properties) {
      return rowDataset;
    }
  }
}
