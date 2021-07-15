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

package org.apache.spark.sql.adapter

import org.apache.hudi.Spark2RowSerDe
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, Like}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.execution.datasources.{Spark2ParsePartitionUtil, SparkParsePartitionUtil}
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.hudi.parser.HoodieSqlParser
import org.apache.spark.sql.internal.SQLConf

/**
 * The adapter for spark2.
 */
class Spark2Adapter extends SparkAdapter {

  override def createSparkRowSerDe(encoder: ExpressionEncoder[Row]): SparkRowSerDe = {
    new Spark2RowSerDe(encoder)
  }

  override def toTableIdentify(aliasId: AliasIdentifier): TableIdentifier = {
    TableIdentifier(aliasId.identifier, aliasId.database)
  }

  override def toTableIdentify(relation: UnresolvedRelation): TableIdentifier = {
    relation.tableIdentifier
  }

  override def createJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Join = {
    Join(left, right, joinType, None)
  }

  override def isInsertInto(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[InsertIntoTable]
  }

  override def getInsertIntoChildren(plan: LogicalPlan):
  Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)] = {
    plan match {
      case InsertIntoTable(table, partition, query, overwrite, ifPartitionNotExists) =>
        Some((table, partition, query, overwrite, ifPartitionNotExists))
      case _=> None
    }
  }

  override def createInsertInto(table: LogicalPlan, partition: Map[String, Option[String]],
      query: LogicalPlan, overwrite: Boolean, ifPartitionNotExists: Boolean): LogicalPlan = {
    InsertIntoTable(table, partition, query, overwrite, ifPartitionNotExists)
  }

  override def createExtendedSparkParser: Option[(SparkSession, ParserInterface) => ParserInterface] = {
    Some(
      (spark: SparkSession, delegate: ParserInterface) => new HoodieSqlParser(spark, delegate)
    )
  }

  override def createSparkParsePartitionUtil(conf: SQLConf): SparkParsePartitionUtil = new Spark2ParsePartitionUtil

  override def createLike(left: Expression, right: Expression): Expression = {
    Like(left, right)
  }
}
