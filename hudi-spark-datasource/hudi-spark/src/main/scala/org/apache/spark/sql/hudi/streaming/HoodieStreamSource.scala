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

package org.apache.spark.sql.hudi.streaming

import java.io.{BufferedWriter, InputStream, OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.util.Date

import org.apache.hadoop.fs.Path
import org.apache.hudi.{DataSourceReadOptions, IncrementalRelation, MergeOnReadIncrementalRelation, SparkAdapterSupport}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.{FileIOUtils, TablePathUtils}
import org.apache.spark.sql.hudi.streaming.HoodieStreamSource.VERSION
import org.apache.spark.sql.hudi.streaming.HoodieSourceOffset.INIT_OFFSET
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, Offset, Source}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * The Struct Stream Source for Hudi to consume the data by streaming job.
  * @param sqlContext
  * @param metadataPath
  * @param schemaOption
  * @param parameters
  */
class HoodieStreamSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schemaOption: Option[StructType],
    parameters: Map[String, String])
  extends Source with Logging with Serializable with SparkAdapterSupport {

  @transient private val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
  private lazy val tablePath: Path = {
    val path = new Path(parameters.getOrElse("path", "Missing 'path' option"))
    val fs = path.getFileSystem(hadoopConf)
    TablePathUtils.getTablePath(fs, path).get()
  }
  private lazy val metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(tablePath.toString).build()
  private lazy val tableType = metaClient.getTableType

  @transient private var lastOffset: HoodieSourceOffset = _
  @transient private lazy val initialOffsets = {
    val metadataLog =
      new HDFSMetadataLog[HoodieSourceOffset](sqlContext.sparkSession, metadataPath) {
        override def serialize(metadata: HoodieSourceOffset, out: OutputStream): Unit = {
          val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
          writer.write("v" + VERSION + "\n")
          writer.write(metadata.json)
          writer.flush()
        }

        /**
          * Deserialize the init offset from the metadata file.
          * The format in the metadata file is like this:
          * ----------------------------------------------
          * v1         -- The version info in the first line
          * offsetJson -- The json string of HoodieSourceOffset in the rest of the file
          * -----------------------------------------------
          * @param in
          * @return
          */
        override def deserialize(in: InputStream): HoodieSourceOffset = {
          val content = FileIOUtils.readAsUTFString(in)
          // Get version from the first line
          val firstLineEnd = content.indexOf("\n")
          if (firstLineEnd > 0) {
            val version = getVersion(content.substring(0, firstLineEnd))
            if (version > VERSION) {
              throw new IllegalStateException(s"UnSupportVersion: max support version is: $VERSION" +
                s" current version is: $version")
            }
            // Get offset from the rest line in the file
            HoodieSourceOffset.fromJson(content.substring(firstLineEnd + 1))
          } else {
            throw new IllegalStateException(s"Bad metadata format, failed to find the version line.")
          }
        }
      }
    metadataLog.get(0).getOrElse {
      metadataLog.add(0, INIT_OFFSET)
      INIT_OFFSET
    }
  }

  private def getVersion(versionLine: String): Int = {
    if (versionLine.startsWith("v")) {
      versionLine.substring(1).toInt
    } else {
      throw new IllegalStateException(s"Illegal version line: $versionLine " +
        s"in the streaming metadata path")
    }
  }

  override def schema: StructType = {
    schemaOption.getOrElse {
      val schemaUtil = new TableSchemaResolver(metaClient)
      SchemaConverters.toSqlType(schemaUtil.getTableAvroSchema)
        .dataType.asInstanceOf[StructType]
    }
  }

  /**
    * Get the latest offset from the hoodie table.
    * @return
    */
  override def getOffset: Option[Offset] = {
    metaClient.reloadActiveTimeline()
    val activeInstants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
    if (!activeInstants.empty()) {
      val currentLatestCommitTime = activeInstants.lastInstant().get().getTimestamp
      if (lastOffset == null || currentLatestCommitTime > lastOffset.commitTime) {
        lastOffset = HoodieSourceOffset(currentLatestCommitTime)
      }
    } else { // if there are no active commits, use the init offset
      lastOffset = initialOffsets
    }
    Some(lastOffset)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    initialOffsets

    val startOffset = start.map(HoodieSourceOffset(_))
      .getOrElse(initialOffsets)
    val endOffset = HoodieSourceOffset(end)

    if (startOffset == endOffset) {
      sqlContext.internalCreateDataFrame(
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    } else {
      // Consume the data between (startCommitTime, endCommitTime]
      val incParams = parameters ++ Map(
        DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY.key -> startCommitTime(startOffset),
        DataSourceReadOptions.END_INSTANTTIME_OPT_KEY.key -> endOffset.commitTime
      )

      val rdd = tableType match {
        case HoodieTableType.COPY_ON_WRITE =>
          val serDe = sparkAdapter.createSparkRowSerDe(RowEncoder(schema))
          new IncrementalRelation(sqlContext, incParams, schema, metaClient)
            .buildScan()
            .map(serDe.serializeRow)
        case HoodieTableType.MERGE_ON_READ =>
          val requiredColumns = schema.fields.map(_.name)
          new MergeOnReadIncrementalRelation(sqlContext, incParams, schema, metaClient)
            .buildScan(requiredColumns, Array.empty[Filter])
            .asInstanceOf[RDD[InternalRow]]
        case _ => throw new IllegalArgumentException(s"UnSupport tableType: $tableType")
      }
      sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
    }
  }

  private def startCommitTime(startOffset: HoodieSourceOffset): String = {
    startOffset match {
      case INIT_OFFSET => startOffset.commitTime
      case HoodieSourceOffset(commitTime) =>
        val time = HoodieActiveTimeline.parseInstantTime(commitTime).getTime
        // As we consume the data between (start, end], start is not included,
        // so we +1s to the start commit time here.
        HoodieActiveTimeline.formatInstantTime(new Date(time + 1000))
      case _=> throw new IllegalStateException("UnKnow offset type.")
    }
  }

  override def stop(): Unit = {

  }
}

object HoodieStreamSource {
  val VERSION = 1
}
