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

package org.apache.hudi.examples.spark

import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME_OPT_KEY, END_INSTANTTIME_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL, QUERY_TYPE_OPT_KEY}
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.common.model.HoodieAvroPayload
import org.apache.hudi.examples.datagen.HoodieExampleDataGenerator
import org.apache.hudi.examples.spark.util.HoodieExampleSparkUtils
import org.apache.spark.sql.SparkSession

/**
 * Simple examples of [[org.apache.hudi.DefaultSource]]
 *
 * To run this example, you should
 *   1. For running in IDE, set VM options `-Dspark.master=local[2]`
 *      2. For running in shell, using `spark-submit`
 *
 * Usage: HoodieWriteClientExample <tablePath> <tableName>.
 * <tablePath> and <tableName> describe root path of hudi and table name
 * for example, `HoodieDataSourceExample file:///tmp/hoodie/hudi_cow_table hudi_cow_table`
 */
object HoodieDataSourceExample {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: HoodieDataSourceExample <tablePath> <tableName>")
      System.exit(1)
    }
    val tablePath = args(0)
    val tableName = args(1)

    val spark = HoodieExampleSparkUtils.defaultSparkSession("Hudi Spark basic example")

    val dataGen = new HoodieExampleDataGenerator[HoodieAvroPayload]
    insertData(spark, tablePath, tableName, dataGen)
    updateData(spark, tablePath, tableName, dataGen)
    queryData(spark, tablePath, tableName, dataGen)

    incrementalQuery(spark, tablePath, tableName)
    pointInTimeQuery(spark, tablePath, tableName)

    spark.stop()
  }


  /**
   * Generate some new trips, load them into a DataFrame and write the DataFrame into the Hudi dataset as below.
   */
  def insertData(spark: SparkSession, tablePath: String, tableName: String, dataGen: HoodieExampleDataGenerator[HoodieAvroPayload]): Unit = {

    val commitTime: String = System.currentTimeMillis().toString
    val inserts = dataGen.convertToStringList(dataGen.generateInserts(commitTime, 20))
    spark.sparkContext.parallelize(inserts, 2)
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 1))
    df.write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY.key, "ts").
      option(RECORDKEY_FIELD_OPT_KEY.key, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY.key, "partitionpath").
      option(    <dependency>
        <groupId>org.apache.hudi</groupId>
        <artifactId>hudi-spark_${scala.binary.version}</artifactId>
        <version>${project.version}</version>
        <exclusions>
          <exclusion>
            <groupId>javax.servlet</groupId>
            <artifactId>servlet-api</artifactId>
          </exclusion>
        </exclusions>
      </dependency>.key, tableName).
      mode(Overwrite).
      save(tablePath)
  }

  /**
   * Load the data files into a DataFrame.
   */
  def queryData(spark: SparkSession, tablePath: String, tableName: String, dataGen: HoodieExampleDataGenerator[HoodieAvroPayload]): Unit = {
    val roViewDF = spark.
      read.
      format("org.apache.hudi").
      load(tablePath + "/*/*/*/*")

    roViewDF.createOrReplaceTempView("hudi_ro_table")

    spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_ro_table where fare > 20.0").show()
    //  +-----------------+-------------------+-------------------+---+
    //  |             fare|          begin_lon|          begin_lat| ts|
    //  +-----------------+-------------------+-------------------+---+
    //  |98.88075495133515|0.39556048623031603|0.17851135255091155|0.0|
    //  ...

    spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_ro_table").show()
    //  +-------------------+--------------------+----------------------+-------------------+--------------------+------------------+
    //  |_hoodie_commit_time|  _hoodie_record_key|_hoodie_partition_path|              rider|              driver|              fare|
    //  +-------------------+--------------------+----------------------+-------------------+--------------------+------------------+
    //  |     20191231181501|31cafb9f-0196-4b1...|            2020/01/02|rider-1577787297889|driver-1577787297889| 98.88075495133515|
    //  ...
  }

  /**
   * This is similar to inserting new data. Generate updates to existing trips using the data generator,
   * load into a DataFrame and write DataFrame into the hudi dataset.
   */
  def updateData(spark: SparkSession, tablePath: String, tableName: String, dataGen: HoodieExampleDataGenerator[HoodieAvroPayload]): Unit = {

    val commitTime: String = System.currentTimeMillis().toString
    val updates = dataGen.convertToStringList(dataGen.generateUpdates(commitTime, 10))
    val df = spark.read.json(spark.sparkContext.parallelize(updates, 1))
    df.write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY.key, "ts").
      option(RECORDKEY_FIELD_OPT_KEY.key, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY.key, "partitionpath").
      option(TABLE_NAME.key, tableName).
      mode(Append).
      save(tablePath)
  }

  /**
   * Hudi also provides capability to obtain a stream of records that changed since given commit timestamp.
   * This can be achieved using Hudi’s incremental view and providing a begin time from which changes need to be streamed.
   * We do not need to specify endTime, if we want all changes after the given commit (as is the common case).
   */
  def incrementalQuery(spark: SparkSession, tablePath: String, tableName: String) {
    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from hudi_ro_table order by commitTime").map(k => k.getString(0)).take(50)
    val beginTime = commits(commits.length - 2) // commit time we are interested in

    // incrementally query data
    val incViewDF = spark.
      read.
      format("org.apache.hudi").
      option(QUERY_TYPE_OPT_KEY.key, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME_OPT_KEY.key, beginTime).
      load(tablePath)
    incViewDF.createOrReplaceTempView("hudi_incr_table")
    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from hudi_incr_table where fare > 20.0").show()
  }

  /**
   * Lets look at how to query data as of a specific time.
   * The specific time can be represented by pointing endTime to a specific commit time
   * and beginTime to “000” (denoting earliest possible commit time).
   */
  def pointInTimeQuery(spark: SparkSession, tablePath: String, tableName: String) {
    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_ro_table order by commitTime").map(k => k.getString(0)).take(50)
    val beginTime = "000" // Represents all commits > this time.
    val endTime = commits(commits.length - 2) // commit time we are interested in

    //incrementally query data
    val incViewDF = spark.read.format("org.apache.hudi").
      option(QUERY_TYPE_OPT_KEY.key, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME_OPT_KEY.key, beginTime).
      option(END_INSTANTTIME_OPT_KEY.key, endTime).
      load(tablePath)
    incViewDF.createOrReplaceTempView("hudi_incr_table")
    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_incr_table where fare > 20.0").show()
  }
}
