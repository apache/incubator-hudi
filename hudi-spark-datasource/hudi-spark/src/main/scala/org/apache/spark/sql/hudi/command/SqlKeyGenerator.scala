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

package org.apache.spark.sql.hudi.command

import org.apache.avro.generic.GenericRecord
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.util.PartitionPathEncodeUtils
import org.apache.hudi.keygen.{ComplexKeyGenerator, KeyGenUtils}
import org.apache.spark.sql.types.{StructType, TimestampType}

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit.{MICROSECONDS, MILLISECONDS}

/**
 * A complex key generator for sql command which do some process for the
 * timestamp data type partition field.
 */
class SqlKeyGenerator(props: TypedProperties) extends ComplexKeyGenerator(props) {

  private lazy val partitionSchema = {
    val partitionSchema = props.getString(SqlKeyGenerator.PARTITION_SCHEMA, "")
    if (partitionSchema != null && partitionSchema.nonEmpty) {
      Some(StructType.fromDDL(partitionSchema))
    } else {
      None
    }
  }

  override def getPartitionPath(record: GenericRecord): String = {
    val partitionPath = super.getPartitionPath(record)
    if (partitionSchema.isDefined) {
      // we can split the partitionPath here because we enable the URL_ENCODE_PARTITIONING_OPT_KEY
      // by default for sql.
      val partitionFragments = partitionPath.split(KeyGenUtils.DEFAULT_PARTITION_PATH_SEPARATOR)
      assert(partitionFragments.size == partitionSchema.get.size)

      partitionFragments.zip(partitionSchema.get.fields).map {
        case (partitionValue, partitionField) =>
          val hiveStylePrefix = s"${partitionField.name}="
          val isHiveStyle = partitionValue.startsWith(hiveStylePrefix)
          val _partitionValue = if (isHiveStyle) {
            partitionValue.substring(hiveStylePrefix.length)
          } else {
            partitionValue
          }

          partitionField.dataType match {
            case TimestampType =>
              val timeMs = MILLISECONDS.convert(_partitionValue.toLong, MICROSECONDS)
              val timestampFormat = PartitionPathEncodeUtils.escapePathName(
                SqlKeyGenerator.timestampTimeFormat.format(Instant.ofEpochMilli(timeMs)))
              if (isHiveStyle) {
                s"$hiveStylePrefix$timestampFormat"
              } else {
                timestampFormat
              }
            case _=> partitionValue
          }
      }.mkString(KeyGenUtils.DEFAULT_PARTITION_PATH_SEPARATOR)
    } else {
      partitionPath
    }
  }
}

object SqlKeyGenerator {
  val PARTITION_SCHEMA = "hoodie.sql.partition.schema"
  private val timestampTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
}
