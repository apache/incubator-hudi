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

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hudi.utils.BucketUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileStatusCache, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

case class HoodieMergeOnReadFileSplit(dataFile: Option[PartitionedFile],
                                      logPaths: Option[List[String]],
                                      latestCommit: String,
                                      tablePath: String,
                                      maxCompactionMemoryInBytes: Long,
                                      mergeType: String)

case class HoodieMergeOnReadTableState(tableStructSchema: StructType,
                                       requiredStructSchema: StructType,
                                       tableAvroSchema: String,
                                       requiredAvroSchema: String,
                                       hoodieRealtimeFileSplits: List[List[HoodieMergeOnReadFileSplit]],
                                       preCombineField: Option[String])

class MergeOnReadSnapshotRelation(val sqlContext: SQLContext,
                                  val optParams: Map[String, String],
                                  val userSchema: StructType,
                                  val globPaths: Option[Seq[Path]],
                                  val metaClient: HoodieTableMetaClient,
                                  val bucketSpec: Option[BucketSpec] = None)
  extends BaseRelation with PrunedFilteredScan with Logging {

  private val conf = sqlContext.sparkContext.hadoopConfiguration
  private val jobConf = new JobConf(conf)
  // use schema from latest metadata, if not present, read schema from the data file
  private val schemaUtil = new TableSchemaResolver(metaClient)
  private lazy val tableAvroSchema = {
    try {
      schemaUtil.getTableAvroSchema
    } catch {
      case _: Throwable => // If there is no commit in the table, we cann't get the schema
        // with schemaUtil, use the userSchema instead.
        SchemaConverters.toAvroType(userSchema)
    }
  }

  private lazy val tableStructSchema = AvroConversionUtils.convertAvroSchemaToStructType(tableAvroSchema)
  private val mergeType = optParams.getOrElse(
    DataSourceReadOptions.REALTIME_MERGE_OPT_KEY.key,
    DataSourceReadOptions.REALTIME_MERGE_OPT_KEY.defaultValue)
  private val maxCompactionMemoryInBytes = getMaxCompactionMemoryInBytes(jobConf)
  private val preCombineField = {
    val preCombineFieldFromTableConfig = metaClient.getTableConfig.getPreCombineField
    if (preCombineFieldFromTableConfig != null) {
      Some(preCombineFieldFromTableConfig)
    } else {
      // get preCombineFiled from the options if this is a old table which have not store
      // the field to hoodie.properties
      optParams.get(DataSourceReadOptions.READ_PRE_COMBINE_FIELD.key)
    }
  }
  override def schema: StructType = tableStructSchema

  override def needConversion: Boolean = false

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    log.debug(s" buildScan requiredColumns = ${requiredColumns.mkString(",")}")
    log.debug(s" buildScan filters = ${filters.mkString(",")}")

    val (requiredAvroSchema, requiredStructSchema) =
      MergeOnReadSnapshotRelation.getRequiredSchema(tableAvroSchema, requiredColumns)
    val fileIndex = buildFileIndex(filters)
    val hoodieTableState = HoodieMergeOnReadTableState(
      tableStructSchema,
      requiredStructSchema,
      tableAvroSchema.toString,
      requiredAvroSchema.toString,
      fileIndex,
      preCombineField
    )
    val fullSchemaParquetReader = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = sqlContext.sparkSession,
      dataSchema = tableStructSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = tableStructSchema,
      filters = filters,
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    )
    val requiredSchemaParquetReader = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = sqlContext.sparkSession,
      dataSchema = tableStructSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = requiredStructSchema,
      filters = filters,
      options = optParams,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    )

    val rdd = new HoodieMergeOnReadRDD(
      sqlContext.sparkContext,
      jobConf,
      fullSchemaParquetReader,
      requiredSchemaParquetReader,
      hoodieTableState
    )
    rdd.asInstanceOf[RDD[Row]]
  }

  def buildFileIndex(filters: Array[Filter]): List[List[HoodieMergeOnReadFileSplit]] = {

    val fileStatuses = if (globPaths.isDefined) {
      // Load files from the global paths if it has defined to be compatible with the original mode
      val inMemoryFileIndex = HoodieSparkUtils.createInMemoryFileIndex(sqlContext.sparkSession, globPaths.get)
      inMemoryFileIndex.allFiles()
    } else { // Load files by the HoodieFileIndex.
      val hoodieFileIndex = HoodieFileIndex(sqlContext.sparkSession, metaClient,
        Some(tableStructSchema), optParams, FileStatusCache.getOrCreate(sqlContext.sparkSession))

      // Get partition filter and convert to catalyst expression
      val partitionColumns = hoodieFileIndex.partitionSchema.fieldNames.toSet
      val partitionFilters = filters.filter(f => f.references.forall(p => partitionColumns.contains(p)))
      val partitionFilterExpression =
        HoodieSparkUtils.convertToCatalystExpressions(partitionFilters, tableStructSchema)

      // if convert success to catalyst expression, use the partition prune
      if (partitionFilterExpression.isDefined) {
        hoodieFileIndex.listFiles(Seq(partitionFilterExpression.get), Seq.empty).flatMap(_.files)
      } else {
        hoodieFileIndex.allFiles
      }
    }

    if (fileStatuses.isEmpty) { // If this an empty table, return an empty split list.
      List.empty
    } else {
      val fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline.getCommitsTimeline
          .filterCompletedInstants, fileStatuses.toArray)
      val latestFiles: List[HoodieBaseFile] = fsView.getLatestBaseFiles.iterator().asScala.toList

      if (!fsView.getLastInstant.isPresent) { // Return empty list if the table has no commit
        List.empty
      } else {
        val latestCommit = fsView.getLastInstant.get().getTimestamp
        val fileGroup = HoodieRealtimeInputFormatUtils.groupLogsByBaseFile(conf, latestFiles.asJava).asScala
        val fileSplits = fileGroup.map(kv => {
          val baseFile = kv._1
          val logPaths = if (kv._2.isEmpty) Option.empty else Option(kv._2.asScala.toList)
          val filePath = MergeOnReadSnapshotRelation.getFilePath(baseFile.getFileStatus.getPath)

          val partitionedFile = PartitionedFile(InternalRow.empty, filePath, 0, baseFile.getFileLen)
          (kv._1.getFileId, HoodieMergeOnReadFileSplit(Option(partitionedFile), logPaths, latestCommit,
            metaClient.getBasePath, maxCompactionMemoryInBytes, mergeType))
        }).toList
        bucketSpec.fold(fileSplits.map(e => List(e._2))) { spec =>
          val bucketIdSplits = fileSplits
            .map { case (fileId, split) => (BucketUtils.bucketIdFromFileId(fileId), split) }
            .groupBy(_._1)
            .map(e => (e._1, e._2.map(_._2)))
          Seq.tabulate(spec.numBuckets) { bucketId =>
            bucketIdSplits.getOrElse(bucketId, Nil)
          }.toList
        }
      }
    }
  }
}

object MergeOnReadSnapshotRelation {

  def getFilePath(path: Path): String = {
    // Here we use the Path#toUri to encode the path string, as there is a decode in
    // ParquetFileFormat#buildReaderWithPartitionValues in the spark project when read the table
    // .So we should encode the file path here. Otherwise, there is a FileNotException throw
    // out.
    // For example, If the "pt" is the partition path field, and "pt" = "2021/02/02", If
    // we enable the URL_ENCODE_PARTITIONING_OPT_KEY and write data to hudi table.The data
    // path in the table will just like "/basePath/2021%2F02%2F02/xxxx.parquet". When we read
    // data from the table, if there are no encode for the file path,
    // ParquetFileFormat#buildReaderWithPartitionValues will decode it to
    // "/basePath/2021/02/02/xxxx.parquet" witch will result to a FileNotException.
    // See FileSourceScanExec#createBucketedReadRDD in spark project which do the same thing
    // when create PartitionedFile.
    path.toUri.toString
  }

  def getRequiredSchema(tableAvroSchema: Schema, requiredColumns: Array[String]): (Schema, StructType) = {
    // First get the required avro-schema, then convert the avro-schema to spark schema.
    val name2Fields = tableAvroSchema.getFields.asScala.map(f => f.name() -> f).toMap
    val requiredFields = requiredColumns.map(c => name2Fields(c))
      .map(f => new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order())).toList
    val requiredAvroSchema = Schema.createRecord(tableAvroSchema.getName, tableAvroSchema.getDoc,
      tableAvroSchema.getNamespace, tableAvroSchema.isError, requiredFields.asJava)
    val requiredStructSchema = AvroConversionUtils.convertAvroSchemaToStructType(requiredAvroSchema)
    (requiredAvroSchema, requiredStructSchema)
  }
}
