/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import java.sql.{Date, Timestamp}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.testutils.RawTripTestPayload.deleteRecordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieUpsertException
import org.apache.hudi.keygen._
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator.Config
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, concat, lit, udf}
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue, fail}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource


/**
 * Basic tests on the spark datasource for COW table.
 */

class TestCOWDataSource extends HoodieClientTestBase {
  var spark: SparkSession = null
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY.key -> "timestamp",
    HoodieWriteConfig.TABLE_NAME.key -> "hoodie_test"
  )

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  @BeforeEach override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @Test def testShortNameStorage() {
    // Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
  }

  /**
   * Test for https://issues.apache.org/jira/browse/HUDI-1615. Null Schema in BulkInsert row writer flow.
   * This was reported by customer when archival kicks in as the schema in commit metadata is not set for bulk_insert
   * row writer flow.
   * In this test, we trigger a round of bulk_inserts and set archive related configs to be minimal. So, after 4 rounds,
   * archival should kick in and 2 commits should be archived. If schema is valid, no exception will be thrown. If not,
   * NPE will be thrown.
   */
  @Test
  def testArchivalWithBulkInsert(): Unit = {
    var structType : StructType = null
    for (i <- 1 to 4) {
      val records = recordsToStrings(dataGen.generateInserts("%05d".format(i), 100)).toList
      val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
      structType = inputDF.schema
      inputDF.write.format("hudi")
        .options(commonOpts)
        .option("hoodie.keep.min.commits", "1")
        .option("hoodie.keep.max.commits", "2")
        .option("hoodie.cleaner.commits.retained", "0")
        .option("hoodie.datasource.write.row.writer.enable", "true")
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
        .mode(if (i == 0) SaveMode.Overwrite else SaveMode.Append)
        .save(basePath)
    }

    val tableMetaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath).build()
    val actualSchema = new TableSchemaResolver(tableMetaClient).getTableAvroSchemaWithoutMetadataFields
    val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(commonOpts(HoodieWriteConfig.TABLE_NAME.key))
    spark.sparkContext.getConf.registerKryoClasses(
      Array(classOf[org.apache.avro.generic.GenericData],
        classOf[org.apache.avro.Schema]))
    val schema = AvroConversionUtils.convertStructTypeToAvroSchema(structType, structName, nameSpace)
    assertTrue(actualSchema != null)
    assertEquals(schema, actualSchema)
  }

  @Test
  def testCopyOnWriteDeletes(): Unit = {
    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*")
    assertEquals(100, snapshotDF1.count())

    val records2 = deleteRecordsToStrings(dataGen.generateUniqueDeletes(20)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2 , 2))

    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshotDF2 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*")
    assertEquals(snapshotDF1.count() - inputDF2.count(), snapshotDF2.count())
  }


  @ParameterizedTest
  //TODO(metadata): Needs HUDI-1459 to be fixed
  //@ValueSource(booleans = Array(true, false))
  @ValueSource(booleans = Array(false))
  def testCopyOnWriteStorage(isMetadataEnabled: Boolean) {
    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieMetadataConfig.METADATA_ENABLE_PROP.key, isMetadataEnabled)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    // Snapshot query
    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.METADATA_ENABLE_PROP.key, isMetadataEnabled)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, snapshotDF1.count())

    // Upsert based on the written table with Hudi metadata columns
    val verificationRowKey = snapshotDF1.limit(1).select("_row_key").first.getString(0)
    val updateDf = snapshotDF1.filter(col("_row_key") === verificationRowKey).withColumn(verificationCol, lit(updatedVerificationVal))

    updateDf.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    val snapshotDF2 = spark.read.format("hudi").load(basePath + "/*/*/*/*")
    assertEquals(100, snapshotDF2.count())
    assertEquals(updatedVerificationVal, snapshotDF2.filter(col("_row_key") === verificationRowKey).select(verificationCol).first.getString(0))

    // Upsert Operation without Hudi metadata columns
    val records2 = recordsToStrings(dataGen.generateUpdates("001", 100)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2 , 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(HoodieMetadataConfig.METADATA_ENABLE_PROP.key, isMetadataEnabled)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime3 = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(3, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Snapshot Query
    val snapshotDF3 = spark.read.format("org.apache.hudi")
      .option(HoodieMetadataConfig.METADATA_ENABLE_PROP.key, isMetadataEnabled)
      .load(basePath + "/*/*/*/*")
    assertEquals(100, snapshotDF3.count()) // still 100, since we only updated

    // Read Incremental Query
    // we have 2 commits, try pulling the first commit (which is not the latest)
    val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").get(0)
    val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY.key, firstCommit)
      .load(basePath)
    assertEquals(100, hoodieIncViewDF1.count()) // 100 initial inserts must be pulled
    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(firstCommit, countsPerCommit(0).get(0))

    // Upsert an empty dataFrame
    val emptyRecords = recordsToStrings(dataGen.generateUpdates("002", 0)).toList
    val emptyDF = spark.read.json(spark.sparkContext.parallelize(emptyRecords, 1))
    emptyDF.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(HoodieMetadataConfig.METADATA_ENABLE_PROP.key, isMetadataEnabled)
      .mode(SaveMode.Append)
      .save(basePath)

    // pull the latest commit
    val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY.key, commitInstantTime2)
      .load(basePath)

    assertEquals(uniqueKeyCnt, hoodieIncViewDF2.count()) // 100 records must be pulled
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime3, countsPerCommit(0).get(0))

    // pull the latest commit within certain partitions
    val hoodieIncViewDF3 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY.key, commitInstantTime2)
      .option(DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY.key, "/2016/*/*/*")
      .load(basePath)
    assertEquals(hoodieIncViewDF2.filter(col("_hoodie_partition_path").contains("2016")).count(), hoodieIncViewDF3.count())

    val timeTravelDF = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY.key, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY.key, firstCommit)
      .load(basePath)
    assertEquals(100, timeTravelDF.count()) // 100 initial inserts must be pulled
  }

  @Test def testOverWriteModeUseReplaceAction(): Unit = {
    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val records2 = recordsToStrings(dataGen.generateInserts("002", 5)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
    .setLoadActiveTimelineOnLoad(true).build();
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => (instant.asInstanceOf[HoodieInstant]).getAction)
    assertEquals(2, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("replacecommit", commits(1))
  }

  @Test def testOverWriteTableModeUseReplaceAction(): Unit = {
    val records1 = recordsToStrings(dataGen.generateInserts("001", 5)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val records2 = recordsToStrings(dataGen.generateInserts("002", 5)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
    .setLoadActiveTimelineOnLoad(true).build()
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => (instant.asInstanceOf[HoodieInstant]).getAction)
    assertEquals(2, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("replacecommit", commits(1))
  }

  @Test def testOverWriteModeUseReplaceActionOnDisJointPartitions(): Unit = {
    // step1: Write 5 records to hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("001", 5, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // step2: Write 7 records to hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("002", 7, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // step3: Write 6 records to hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH using INSERT_OVERWRITE_OPERATION_OPT_VAL
    val records3 = recordsToStrings(dataGen.generateInsertsForPartition("001", 6, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val allRecords = spark.read.format("org.apache.hudi").load(basePath + "/*/*/*")
    allRecords.registerTempTable("tmpTable")

    spark.sql(String.format("select count(*) from tmpTable")).show()

    // step4: Query the rows count from hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val recordCountForParititon1 = spark.sql(String.format("select count(*) from tmpTable where partition = '%s'", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).collect()
    assertEquals("6", recordCountForParititon1(0).get(0).toString)

    // step5: Query the rows count from hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH
    val recordCountForParititon2 = spark.sql(String.format("select count(*) from tmpTable where partition = '%s'", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).collect()
    assertEquals("7", recordCountForParititon2(0).get(0).toString)

    // step6: Query the rows count from hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH using spark.collect and then filter mode
    val recordsForPartitionColumn = spark.sql(String.format("select partition from tmpTable")).collect()
    val filterSecondPartitionCount = recordsForPartitionColumn.filter(row => row.get(0).equals(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).size
    assertEquals(7, filterSecondPartitionCount)

    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
    .setLoadActiveTimelineOnLoad(true).build()
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => instant.asInstanceOf[HoodieInstant].getAction)
    assertEquals(3, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("commit", commits(1))
    assertEquals("replacecommit", commits(2))
  }

  @Test def testOverWriteTableModeUseReplaceActionOnDisJointPartitions(): Unit = {
    // step1: Write 5 records to hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("001", 5, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // step2: Write 7 more records using SaveMode.Overwrite for partition2 DEFAULT_SECOND_PARTITION_PATH
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("002", 7, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val allRecords = spark.read.format("org.apache.hudi").load(basePath + "/*/*/*")
    allRecords.registerTempTable("tmpTable")

    spark.sql(String.format("select count(*) from tmpTable")).show()

    // step3: Query the rows count from hoodie table for partition1 DEFAULT_FIRST_PARTITION_PATH
    val recordCountForParititon1 = spark.sql(String.format("select count(*) from tmpTable where partition = '%s'", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).collect()
    assertEquals("0", recordCountForParititon1(0).get(0).toString)

    // step4: Query the rows count from hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH
    val recordCountForParititon2 = spark.sql(String.format("select count(*) from tmpTable where partition = '%s'", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).collect()
    assertEquals("7", recordCountForParititon2(0).get(0).toString)

    // step5: Query the rows count from hoodie table
    val recordCount = spark.sql(String.format("select count(*) from tmpTable")).collect()
    assertEquals("7", recordCount(0).get(0).toString)

    // step6: Query the rows count from hoodie table for partition2 DEFAULT_SECOND_PARTITION_PATH using spark.collect and then filter mode
    val recordsForPartitionColumn = spark.sql(String.format("select partition from tmpTable")).collect()
    val filterSecondPartitionCount = recordsForPartitionColumn.filter(row => row.get(0).equals(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)).size
    assertEquals(7, filterSecondPartitionCount)

    val metaClient = HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(basePath)
    .setLoadActiveTimelineOnLoad(true).build()
    val commits = metaClient.getActiveTimeline.filterCompletedInstants().getInstants.toArray
      .map(instant => instant.asInstanceOf[HoodieInstant].getAction)
    assertEquals(2, commits.size)
    assertEquals("commit", commits(0))
    assertEquals("replacecommit", commits(1))
  }

  @Test def testDropInsertDup(): Unit = {
    val insert1Cnt = 10
    val insert2DupKeyCnt = 9
    val insert2NewKeyCnt = 2

    val totalUniqueKeyToGenerate = insert1Cnt + insert2NewKeyCnt
    val allRecords =  dataGen.generateInserts("001", totalUniqueKeyToGenerate)
    val inserts1 = allRecords.subList(0, insert1Cnt)
    val inserts2New = dataGen.generateSameKeyInserts("002", allRecords.subList(insert1Cnt, insert1Cnt + insert2NewKeyCnt))
    val inserts2Dup = dataGen.generateSameKeyInserts("002", inserts1.subList(0, insert2DupKeyCnt))

    val records1 = recordsToStrings(inserts1).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val hoodieROViewDF1 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*")
    assertEquals(insert1Cnt, hoodieROViewDF1.count())

    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val records2 = recordsToStrings(inserts2Dup ++ inserts2New).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.INSERT_DROP_DUPS_OPT_KEY.key, "true")
      .mode(SaveMode.Append)
      .save(basePath)
    val hoodieROViewDF2 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*")
    assertEquals(hoodieROViewDF2.count(), totalUniqueKeyToGenerate)

    val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY.key, commitInstantTime1)
      .load(basePath)
    assertEquals(hoodieIncViewDF2.count(), insert2NewKeyCnt)
  }

  @Test def testComplexDataTypeWriteAndReadConsistency(): Unit = {
    val schema = StructType(StructField("_row_key", StringType, true) :: StructField("name", StringType, true)
      :: StructField("timeStampValue", TimestampType, true) :: StructField("dateValue", DateType, true)
      :: StructField("decimalValue", DataTypes.createDecimalType(15, 10), true) :: StructField("timestamp", IntegerType, true)
      :: StructField("partition", IntegerType, true) :: Nil)

    val records = Seq(Row("11", "Andy", Timestamp.valueOf("1970-01-01 13:31:24"), Date.valueOf("1991-11-07"), BigDecimal.valueOf(1.0), 11, 1),
      Row("22", "lisi", Timestamp.valueOf("1970-01-02 13:31:24"), Date.valueOf("1991-11-08"), BigDecimal.valueOf(2.0), 11, 1),
      Row("33", "zhangsan", Timestamp.valueOf("1970-01-03 13:31:24"), Date.valueOf("1991-11-09"), BigDecimal.valueOf(3.0), 11, 1))
    val rdd = jsc.parallelize(records)
    val  recordsDF = spark.createDataFrame(rdd, schema)
    recordsDF.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*")
    recordsReadDF.printSchema()
    recordsReadDF.schema.foreach(f => {
      f.name match {
        case "timeStampValue" =>
          assertEquals(f.dataType, org.apache.spark.sql.types.TimestampType)
        case "dateValue" =>
          assertEquals(f.dataType, org.apache.spark.sql.types.DateType)
        case "decimalValue" =>
          assertEquals(f.dataType, org.apache.spark.sql.types.DecimalType(15, 10))
        case _ =>
      }
    })
  }

  @Test def testWithAutoCommitOn(): Unit = {
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.HOODIE_AUTO_COMMIT_PROP.key, "true")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
  }

  private def getDataFrameWriter(keyGenerator: String): DataFrameWriter[Row] = {
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    inputDF.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY.key, keyGenerator)
      .mode(SaveMode.Overwrite)
  }

  @Test def testSparkPartitonByWithCustomKeyGenerator(): Unit = {
    // Without fieldType, the default is SIMPLE
    var writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName)
    writer.partitionBy("current_ts")
      .save(basePath)

    var recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*")

    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= col("current_ts").cast("string")).count() == 0)

    // Specify fieldType as TIMESTAMP
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName)
    writer.partitionBy("current_ts:TIMESTAMP")
      .option(Config.TIMESTAMP_TYPE_FIELD_PROP, "EPOCHMILLISECONDS")
      .option(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, "yyyyMMdd")
      .save(basePath)

    recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*")

    val udf_date_format = udf((data: Long) => new DateTime(data).toString(DateTimeFormat.forPattern("yyyyMMdd")))
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= udf_date_format(col("current_ts"))).count() == 0)

    // Mixed fieldType
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName)
    writer.partitionBy("driver", "rider:SIMPLE", "current_ts:TIMESTAMP")
      .option(Config.TIMESTAMP_TYPE_FIELD_PROP, "EPOCHMILLISECONDS")
      .option(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, "yyyyMMdd")
      .save(basePath)

    recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*")
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!=
      concat(col("driver"), lit("/"), col("rider"), lit("/"), udf_date_format(col("current_ts")))).count() == 0)

    // Test invalid partitionKeyType
    writer = getDataFrameWriter(classOf[CustomKeyGenerator].getName)
    writer = writer.partitionBy("current_ts:DUMMY")
      .option(Config.TIMESTAMP_TYPE_FIELD_PROP, "EPOCHMILLISECONDS")
      .option(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, "yyyyMMdd")
    try {
      writer.save(basePath)
      fail("should fail when invalid PartitionKeyType is provided!")
    } catch {
      case e: Exception =>
        assertTrue(e.getCause.getMessage.contains("No enum constant org.apache.hudi.keygen.CustomAvroKeyGenerator.PartitionKeyType.DUMMY"))
    }
  }

  @Test def testSparkPartitonByWithSimpleKeyGenerator() {
    // Use the `driver` field as the partition key
    var writer = getDataFrameWriter(classOf[SimpleKeyGenerator].getName)
    writer.partitionBy("driver")
      .save(basePath)

    var recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*")

    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= col("driver")).count() == 0)

    // Use the `driver,rider` field as the partition key, If no such field exists, the default value `default` is used
    writer = getDataFrameWriter(classOf[SimpleKeyGenerator].getName)
    writer.partitionBy("driver", "rider")
      .save(basePath)

    recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*")

    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= lit("default")).count() == 0)
  }

  @Test def testSparkPartitonByWithComplexKeyGenerator() {
    // Use the `driver` field as the partition key
    var writer = getDataFrameWriter(classOf[ComplexKeyGenerator].getName)
    writer.partitionBy("driver")
      .save(basePath)

    var recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*")

    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= col("driver")).count() == 0)

    // Use the `driver`,`rider` field as the partition key
    writer = getDataFrameWriter(classOf[ComplexKeyGenerator].getName)
    writer.partitionBy("driver", "rider")
      .save(basePath)

    recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*")

    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= concat(col("driver"), lit("/"), col("rider"))).count() == 0)
  }

  @Test def testSparkPartitonByWithTimestampBasedKeyGenerator() {
    val writer = getDataFrameWriter(classOf[TimestampBasedKeyGenerator].getName)
    writer.partitionBy("current_ts")
      .option(Config.TIMESTAMP_TYPE_FIELD_PROP, "EPOCHMILLISECONDS")
      .option(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, "yyyyMMdd")
      .save(basePath)

    val recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*")
    val udf_date_format = udf((data: Long) => new DateTime(data).toString(DateTimeFormat.forPattern("yyyyMMdd")))
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= udf_date_format(col("current_ts"))).count() == 0)
  }

  @Test def testSparkPartitonByWithGlobalDeleteKeyGenerator() {
    val writer = getDataFrameWriter(classOf[GlobalDeleteKeyGenerator].getName)
    writer.partitionBy("driver")
      .save(basePath)

    val recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*")
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= lit("")).count() == 0)
  }

  @Test def testSparkPartitonByWithNonpartitionedKeyGenerator() {
    // Empty string column
    var writer = getDataFrameWriter(classOf[NonpartitionedKeyGenerator].getName)
    writer.partitionBy("")
      .save(basePath)

    var recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*")
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= lit("")).count() == 0)

    // Non-existent column
    writer = getDataFrameWriter(classOf[NonpartitionedKeyGenerator].getName)
    writer.partitionBy("abc")
      .save(basePath)

    recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*")
    assertTrue(recordsReadDF.filter(col("_hoodie_partition_path") =!= lit("")).count() == 0)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testQueryCOWWithBasePathAndFileIndex(partitionEncode: Boolean): Unit = {
    val N = 20
    // Test query with partition prune if URL_ENCODE_PARTITIONING_OPT_KEY has enable
    val records1 = dataGen.generateInsertsContainsAllPartitions("000", N)
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records1), 2))
    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING_OPT_KEY.key, partitionEncode)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    val countIn20160315 = records1.asScala.count(record => record.getPartitionPath == "2016/03/15")
    // query the partition by filter
    val count1 = spark.read.format("hudi")
      .load(basePath)
      .filter("partition = '2016/03/15'")
      .count()
    assertEquals(countIn20160315, count1)

    // query the partition by path
    val partitionPath = if (partitionEncode) "2016%2F03%2F15" else "2016/03/15"
    val count2 = spark.read.format("hudi")
      .load(basePath + s"/$partitionPath")
      .count()
    assertEquals(countIn20160315, count2)

    // Second write with Append mode
    val records2 = dataGen.generateInsertsContainsAllPartitions("000", N + 1)
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(recordsToStrings(records2), 2))
    inputDF2.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING_OPT_KEY.key, partitionEncode)
      .mode(SaveMode.Append)
      .save(basePath)
    // Incremental query without "*" in path
    val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY.key, commitInstantTime1)
      .load(basePath)
    assertEquals(N + 1, hoodieIncViewDF1.count())
  }

  @Test def testSchemaEvolution(): Unit = {
    // open the schema validate
    val  opts = commonOpts ++ Map("hoodie.avro.schema.validate" -> "true")
    // 1. write records with schema1
    val schema1 = StructType(StructField("_row_key", StringType, true) :: StructField("name", StringType, true)::
      StructField("timestamp", IntegerType, true) :: StructField("partition", IntegerType, true)::Nil)
    val records1 = Seq(Row("1", "Andy", 1, 1),
      Row("2", "lisi", 1, 1),
      Row("3", "zhangsan", 1, 1))
    val rdd = jsc.parallelize(records1)
    val  recordsDF = spark.createDataFrame(rdd, schema1)
    recordsDF.write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Append)
      .save(basePath)

    // 2. write records with schema2 add column age
    val schema2 = StructType(StructField("_row_key", StringType, true) :: StructField("name", StringType, true) ::
      StructField("age", StringType, true) :: StructField("timestamp", IntegerType, true) ::
      StructField("partition", IntegerType, true)::Nil)

    val records2 = Seq(Row("11", "Andy", "10", 1, 1),
      Row("22", "lisi", "11",1, 1),
      Row("33", "zhangsan", "12", 1, 1))
    val rdd2 = jsc.parallelize(records2)
    val  recordsDF2 = spark.createDataFrame(rdd2, schema2)
    recordsDF2.write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Append)
      .save(basePath)

    val recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*")
    val resultSchema = new StructType(recordsReadDF.schema.filter(p=> !p.name.startsWith("_hoodie")).toArray)
    assertEquals(resultSchema, schema2)

    // 3. write records with schema3 delete column name
    try {
      val schema3 = StructType(StructField("_row_key", StringType, true) ::
        StructField("age", StringType, true) :: StructField("timestamp", IntegerType, true) ::
        StructField("partition", IntegerType, true)::Nil)

      val records3 = Seq(Row("11", "10", 1, 1),
        Row("22", "11",1, 1),
        Row("33", "12", 1, 1))
      val rdd3 = jsc.parallelize(records3)
      val  recordsDF3 = spark.createDataFrame(rdd3, schema3)
      recordsDF3.write.format("org.apache.hudi")
        .options(opts)
        .mode(SaveMode.Append)
        .save(basePath)
      fail("Delete column should fail")
    } catch {
      case ex: HoodieUpsertException =>
        assertTrue(ex.getMessage.equals("Failed upsert schema compatibility check."))
    }
  }

  @Test def testSchemaNotEqualData(): Unit = {
    val  opts = commonOpts ++ Map("hoodie.avro.schema.validate" -> "true")
    val schema1 = StructType(StructField("_row_key", StringType, true) :: StructField("name", StringType, true)::
      StructField("timestamp", IntegerType, true):: StructField("age", StringType, true)  :: StructField("partition", IntegerType, true)::Nil)

    val records = Array("{\"_row_key\":\"1\",\"name\":\"lisi\",\"timestamp\":1,\"partition\":1}",
      "{\"_row_key\":\"1\",\"name\":\"lisi\",\"timestamp\":1,\"partition\":1}")

    val inputDF = spark.read.schema(schema1.toDDL).json(spark.sparkContext.parallelize(records, 2))

    inputDF.write.format("org.apache.hudi")
      .options(opts)
      .mode(SaveMode.Append)
      .save(basePath)
    val recordsReadDF = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*")

    val resultSchema = new StructType(recordsReadDF.schema.filter(p=> !p.name.startsWith("_hoodie")).toArray)
    assertEquals(resultSchema, schema1)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testWithEmptyInput(allowEmptyCommit: Boolean): Unit = {
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(Seq.empty[String], 1))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key(), allowEmptyCommit.toString)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertEquals(allowEmptyCommit, HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
  }
}
