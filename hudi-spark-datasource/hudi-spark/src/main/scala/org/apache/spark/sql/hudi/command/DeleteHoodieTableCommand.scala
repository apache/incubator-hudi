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
import org.apache.hudi.DataSourceWriteOptions.{HIVE_STYLE_PARTITIONING_OPT_KEY, HIVE_SUPPORT_TIMESTAMP, KEYGENERATOR_CLASS_OPT_KEY, OPERATION_OPT_KEY, PARTITIONPATH_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, SubqueryAlias}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.hudi.HoodieSqlUtils._

case class DeleteHoodieTableCommand(deleteTable: DeleteFromTable) extends RunnableCommand
  with SparkAdapterSupport {

  private val table = deleteTable.table

  private val tableId = getTableIdentify(table)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logInfo(s"start execute delete command for $tableId")

    // Remove meta fields from the data frame
    var df = removeMetaFields(Dataset.ofRows(sparkSession, table))
    if (deleteTable.condition.isDefined) {
      df = df.filter(Column(deleteTable.condition.get))
    }
    val config = buildHoodieConfig(sparkSession)
    df.write
      .format("hudi")
      .mode(SaveMode.Append)
      .options(config)
      .save()
    sparkSession.catalog.refreshTable(tableId.unquotedString)
    logInfo(s"Finish execute delete command for $tableId")
    Seq.empty[Row]
  }

  private def buildHoodieConfig(sparkSession: SparkSession): Map[String, String] = {
    val targetTable = sparkSession.sessionState.catalog
      .getTableMetadata(tableId)
    val path = getTableLocation(targetTable, sparkSession)
      .getOrElse(s"missing location for $tableId")

    val primaryColumns = HoodieOptionConfig.getPrimaryColumns(targetTable.storage.properties)

    assert(primaryColumns.nonEmpty,
      s"There are no primary key defined in table $tableId, cannot execute delete operator")
    withSparkConf(sparkSession, targetTable.storage.properties) {
      Map(
        "path" -> path,
        KEYGENERATOR_CLASS_OPT_KEY.key -> classOf[SqlKeyGenerator].getCanonicalName,
        TABLE_NAME.key -> tableId.table,
        OPERATION_OPT_KEY.key -> DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL,
        PARTITIONPATH_FIELD_OPT_KEY.key -> targetTable.partitionColumnNames.mkString(","),
        HIVE_SUPPORT_TIMESTAMP.key -> "true",
        HIVE_STYLE_PARTITIONING_OPT_KEY.key -> "true",
        HoodieWriteConfig.DELETE_PARALLELISM.key -> "200",
        SqlKeyGenerator.PARTITION_SCHEMA -> targetTable.partitionSchema.toDDL
      )
    }
  }
}
