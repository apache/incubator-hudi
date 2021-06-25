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

package org.apache.spark.sql.hudi

import scala.collection.JavaConverters._
import java.net.URI
import java.util.Locale
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MergeIntoTable, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.{DataType, NullType, StringType, StructField, StructType}

import scala.collection.immutable.Map

object HoodieSqlUtils extends SparkAdapterSupport {

  def isHoodieTable(table: CatalogTable): Boolean = {
    table.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "hudi"
  }

  def isHoodieTable(tableId: TableIdentifier, spark: SparkSession): Boolean = {
    val table = spark.sessionState.catalog.getTableMetadata(tableId)
    isHoodieTable(table)
  }

  def isHoodieTable(table: LogicalPlan, spark: SparkSession): Boolean = {
    tripAlias(table) match {
      case LogicalRelation(_, _, Some(tbl), _) => isHoodieTable(tbl)
      case relation: UnresolvedRelation =>
        isHoodieTable(sparkAdapter.toTableIdentify(relation), spark)
      case _=> false
    }
  }

  private def tripAlias(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case SubqueryAlias(_, relation: LogicalPlan) =>
        tripAlias(relation)
      case other =>
        other
    }
  }

  /**
   * Add the hoodie meta fields to the schema.
   * @param schema
   * @return
   */
  def addMetaFields(schema: StructType): StructType = {
    val metaFields = HoodieRecord.HOODIE_META_COLUMNS.asScala
    // filter the meta field to avoid duplicate field.
    val dataFields = schema.fields.filterNot(f => metaFields.contains(f.name))
    val fields = metaFields.map(StructField(_, StringType)) ++ dataFields
    StructType(fields)
  }

  private lazy val metaFields = HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet

  /**
   * Remove the meta fields from the schema.
   * @param schema
   * @return
   */
  def removeMetaFields(schema: StructType): StructType = {
    StructType(schema.fields.filterNot(f => isMetaField(f.name)))
  }

  def isMetaField(name: String): Boolean = {
    metaFields.contains(name)
  }

  def removeMetaFields(df: DataFrame): DataFrame = {
    val withoutMetaColumns = df.logicalPlan.output
      .filterNot(attr => isMetaField(attr.name))
      .map(new Column(_))
    if (withoutMetaColumns.length != df.logicalPlan.output.size) {
      df.select(withoutMetaColumns: _*)
    } else {
      df
    }
  }

  def removeMetaFields(attrs: Seq[Attribute]): Seq[Attribute] = {
    attrs.filterNot(attr => isMetaField(attr.name))
  }

  /**
   * Get the table location.
   * @param tableId
   * @param spark
   * @return
   */
  def getTableLocation(tableId: TableIdentifier, spark: SparkSession): Option[String] = {
    val table = spark.sessionState.catalog.getTableMetadata(tableId)
    getTableLocation(table, spark)
  }

  def getTableLocation(table: CatalogTable, sparkSession: SparkSession): Option[String] = {
    val uri = if (table.tableType == CatalogTableType.MANAGED && isHoodieTable(table)) {
      Some(sparkSession.sessionState.catalog.defaultTablePath(table.identifier))
    } else {
      table.storage.locationUri
    }
    val conf = sparkSession.sessionState.newHadoopConf()
    uri.map(makePathQualified(_, conf))
      .map(removePlaceHolder)
  }

  private def removePlaceHolder(path: String): String = {
    if (path == null || path.length == 0) {
      path
    } else if (path.endsWith("-__PLACEHOLDER__")) {
      path.substring(0, path.length() - 16)
    } else {
      path
    }
  }

  def makePathQualified(path: URI, hadoopConf: Configuration): String = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConf)
    fs.makeQualified(hadoopPath).toUri.toString
  }

  def castIfNeeded(child: Expression, dataType: DataType, conf: SQLConf): Expression = {
    child match {
      case Literal(nul, NullType) => Literal(nul, dataType)
      case _ => if (child.dataType != dataType)
        Cast(child, dataType, Option(conf.sessionLocalTimeZone)) else child
    }
  }

  /**
   * Get the TableIdentifier of the target table in MergeInto.
   */
  def getMergeIntoTargetTableId(mergeInto: MergeIntoTable): TableIdentifier = {
    val aliaId = mergeInto.targetTable match {
      case SubqueryAlias(_, SubqueryAlias(tableId, _)) => tableId
      case SubqueryAlias(tableId, _) => tableId
      case plan => throw new IllegalArgumentException(s"Illegal plan $plan in target")
    }
    sparkAdapter.toTableIdentify(aliaId)
  }

  /**
   * Split the expression to a sub expression seq by the AND operation.
   * @param expression
   * @return
   */
  def splitByAnd(expression: Expression): Seq[Expression] = {
    expression match {
      case And(left, right) =>
        splitByAnd(left) ++ splitByAnd(right)
      case exp => Seq(exp)
    }
  }

  /**
   * Append the SparkSession config and table options to the baseConfig.
   * We add the "spark" prefix to hoodie's config key.
   */
  def withSparkConf(spark: SparkSession, options: Map[String, String])
                   (baseConfig: Map[String, String]): Map[String, String] = {
    baseConfig ++ // Table options has the highest priority
      (spark.sessionState.conf.getAllConfs ++ HoodieOptionConfig.mappingSqlOptionToHoodieParam(options))
        .filterKeys(_.startsWith("hoodie."))
  }

  def isSpark3: Boolean = SPARK_VERSION.startsWith("3.")

  def isEnableHive(sparkSession: SparkSession): Boolean =
    "hive" == sparkSession.sessionState.conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION)
}
