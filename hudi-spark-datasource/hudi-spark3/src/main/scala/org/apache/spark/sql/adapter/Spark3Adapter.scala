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

import org.apache.hudi.Spark3RowSerDe
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, Like}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.execution.datasources.{Spark3ParsePartitionUtil, SparkParsePartitionUtil}
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.internal.SQLConf

/**
 * The adapter for spark3.
 */
class Spark3Adapter extends SparkAdapter {

  override def createSparkRowSerDe(encoder: ExpressionEncoder[Row]): SparkRowSerDe = {
    new Spark3RowSerDe(encoder)
  }

  override def toTableIdentify(aliasId: AliasIdentifier): TableIdentifier = {
    aliasId match {
      case AliasIdentifier(name, Seq(database)) =>
         TableIdentifier(name, Some(database))
      case AliasIdentifier(name, Seq(_, database)) =>
        TableIdentifier(name, Some(database))
      case AliasIdentifier(name, Seq()) =>
        TableIdentifier(name, None)
      case _=> throw new IllegalArgumentException(s"Cannot cast $aliasId to TableIdentifier")
    }
  }

  override def toTableIdentify(relation: UnresolvedRelation): TableIdentifier = {
    relation.multipartIdentifier.asTableIdentifier
  }

  override def createJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Join = {
    Join(left, right, joinType, None, JoinHint.NONE)
  }

  override def isInsertInto(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[InsertIntoStatement]
  }

  override def getInsertIntoChildren(plan: LogicalPlan):
    Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)] = {
    plan match {
      case InsertIntoStatement(table, partitionSpec, query, overwrite, ifPartitionNotExists) =>
        Some((table, partitionSpec, query, overwrite, ifPartitionNotExists))
      case _=> None
    }
  }

  override def createInsertInto(table: LogicalPlan, partition: Map[String, Option[String]],
     query: LogicalPlan, overwrite: Boolean, ifPartitionNotExists: Boolean): LogicalPlan = {
    InsertIntoStatement(table, partition, query, overwrite, ifPartitionNotExists)
  }

  override def createSparkParsePartitionUtil(conf: SQLConf): SparkParsePartitionUtil = {
    new Spark3ParsePartitionUtil(conf)
  }

  override def createLike(left: Expression, right: Expression): Expression = {
    new Like(left, right)
  }
}
