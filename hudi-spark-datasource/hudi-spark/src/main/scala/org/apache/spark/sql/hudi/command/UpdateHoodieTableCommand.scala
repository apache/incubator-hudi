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

import org.apache.hudi.{DataSourceWriteOptions, SparkAdapterSupport}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, SubqueryAlias, UpdateTable}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.hudi.HoodieSqlUtils._
import org.apache.spark.sql.types.StructField

import scala.collection.JavaConverters._

case class UpdateHoodieTableCommand(updateTable: UpdateTable) extends RunnableCommand
  with SparkAdapterSupport {

  private val table = updateTable.table
  private val tableId = getTableIdentify(table)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logInfo(s"start execute update command for $tableId")
    def cast(exp:Expression, field: StructField): Expression = {
      castIfNeeded(exp, field.dataType, sparkSession.sqlContext.conf)
    }
    val name2UpdateValue = updateTable.assignments.map {
      case Assignment(attr: AttributeReference, value) =>
        attr.name -> value
    }.toMap

    val updateExpressions = table.output
      .map(attr => name2UpdateValue.getOrElse(attr.name, attr))
      .filter { // filter the meta columns
        case attr: AttributeReference =>
          !HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet.contains(attr.name)
        case _=> true
      }

    val projects = updateExpressions.zip(removeMetaFields(table.schema).fields).map {
      case (attr: AttributeReference, field) =>
        Column(cast(attr, field))
      case (exp, field) =>
        Column(Alias(cast(exp, field), field.name)())
    }

    var df = Dataset.ofRows(sparkSession, table)
    if (updateTable.condition.isDefined) {
      df = df.filter(Column(updateTable.condition.get))
    }
    df = df.select(projects: _*)
    val config = buildHoodieConfig(sparkSession)
    df.write
      .format("hudi")
      .mode(SaveMode.Append)
      .options(config)
      .save()
    sparkSession.catalog.refreshTable(tableId.unquotedString)
    logInfo(s"Finish execute update command for $tableId")
    Seq.empty[Row]
  }

  private def buildHoodieConfig(sparkSession: SparkSession): Map[String, String] = {
    val targetTable = sparkSession.sessionState.catalog
      .getTableMetadata(tableId)
    val path = getTableLocation(targetTable, sparkSession)
      .getOrElse(s"missing location for $tableId")

    val primaryColumns = HoodieOptionConfig.getPrimaryColumns(targetTable.storage.properties)

    assert(primaryColumns.nonEmpty,
      s"There are no primary key in table $tableId, cannot execute update operator")
    val enableHive = isEnableHive(sparkSession)
    withSparkConf(sparkSession, targetTable.storage.properties) {
      Map(
        "path" -> path,
        RECORDKEY_FIELD_OPT_KEY.key -> primaryColumns.mkString(","),
        KEYGENERATOR_CLASS_OPT_KEY.key -> classOf[SqlKeyGenerator].getCanonicalName,
        PRECOMBINE_FIELD_OPT_KEY.key -> primaryColumns.head, //set the default preCombine field.
        TABLE_NAME.key -> tableId.table,
        OPERATION_OPT_KEY.key -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
        PARTITIONPATH_FIELD_OPT_KEY.key -> targetTable.partitionColumnNames.mkString(","),
        META_SYNC_ENABLED_OPT_KEY.key -> enableHive.toString,
        HIVE_USE_JDBC_OPT_KEY.key -> "false",
        HIVE_DATABASE_OPT_KEY.key -> tableId.database.getOrElse("default"),
        HIVE_TABLE_OPT_KEY.key -> tableId.table,
        HIVE_PARTITION_FIELDS_OPT_KEY.key -> targetTable.partitionColumnNames.mkString(","),
        HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY.key -> classOf[MultiPartKeysValueExtractor].getCanonicalName,
        URL_ENCODE_PARTITIONING_OPT_KEY.key -> "true",
        HIVE_SUPPORT_TIMESTAMP.key -> "true",
        HIVE_STYLE_PARTITIONING_OPT_KEY.key -> "true",
        HoodieWriteConfig.UPSERT_PARALLELISM.key -> "200",
        SqlKeyGenerator.PARTITION_SCHEMA -> targetTable.partitionSchema.toDDL
      )
    }
  }
}
