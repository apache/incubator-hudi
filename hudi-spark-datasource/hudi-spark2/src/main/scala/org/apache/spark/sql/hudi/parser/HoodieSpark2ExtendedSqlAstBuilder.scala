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
package org.apache.spark.sql.hudi.parser

import org.antlr.v4.runtime.tree.ParseTree
import org.apache.hudi.spark.sql.parser.HoodieSqlBaseBaseVisitor
import org.apache.hudi.spark.sql.parser.HoodieSqlBaseParser._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConverters._

/**
 * The AstBuilder for HoodieSqlParser to parser the AST tree to Logical Plan.
 * Here we only do the parser for the extended sql syntax. e.g MergeInto. For
 * other sql syntax we use the delegate sql parser which is the SparkSqlParser.
 */
class HoodieSpark2ExtendedSqlAstBuilder(conf: SQLConf, delegate: ParserInterface) extends HoodieSqlBaseBaseVisitor[AnyRef] with Logging {

  import ParserUtils._

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    ctx.statement().accept(this).asInstanceOf[LogicalPlan]
  }

  override def visitMergeIntoTable (ctx: MergeIntoTableContext): LogicalPlan = withOrigin(ctx) {
    visitMergeInto(ctx.mergeInto())
  }

  override def visitMergeInto(ctx: MergeIntoContext): LogicalPlan = withOrigin(ctx) {
    val target = UnresolvedRelation(visitTableIdentifier(ctx.target))
    val source = if (ctx.source != null) {
      UnresolvedRelation(visitTableIdentifier(ctx.source))
    } else {
      val queryText = treeToString(ctx.subquery)
      delegate.parsePlan(queryText)
    }
    val aliasedTarget =
      if (ctx.tableAlias(0) != null) mayApplyAliasPlan(ctx.tableAlias(0), target) else target
    val aliasedSource =
      if (ctx.tableAlias(1) != null) mayApplyAliasPlan(ctx.tableAlias(1), source) else source

    val mergeCondition = expression(ctx.mergeCondition().condition)

    if (ctx.matchedClauses().size() > 2) {
      throw new ParseException("There should be at most 2 'WHEN MATCHED' clauses.",
        ctx.matchedClauses.get(2))
    }

    val matchedClauses: Seq[MergeAction] = ctx.matchedClauses().asScala.flatMap {
      c =>
        val deleteCtx = c.deleteClause()
        val deleteClause = if (deleteCtx != null) {
          val deleteCond = if (deleteCtx.deleteCond != null) {
            Some(expression(deleteCtx.deleteCond))
          } else {
            None
          }
          Some(DeleteAction(deleteCond))
        } else {
          None
        }
        val updateCtx = c.updateClause()
        val updateClause = if (updateCtx != null) {
          val updateAction = updateCtx.updateAction()
          val updateCond = if (updateCtx.updateCond != null) {
            Some(expression(updateCtx.updateCond))
          } else {
            None
          }
          if (updateAction.ASTERISK() != null) {
            Some(UpdateAction(updateCond, Seq.empty))
          } else {
            val assignments = withAssignments(updateAction.assignmentList())
            Some(UpdateAction(updateCond, assignments))
          }
        } else {
          None
        }
        (deleteClause ++ updateClause).toSeq
    }
    val notMatchedClauses: Seq[InsertAction] = ctx.notMatchedClause().asScala.map {
      notMatchedClause =>
        val insertCtx = notMatchedClause.insertClause()
        val insertAction = insertCtx.insertAction()
        val insertCond = if (insertCtx.insertCond != null) {
          Some(expression(insertCtx.insertCond))
        } else {
          None
        }
        if (insertAction.ASTERISK() != null) {
          InsertAction(insertCond, Seq.empty)
        } else {
          val attrList = insertAction.columns.qualifiedName().asScala
            .map(attr => UnresolvedAttribute(visitQualifiedName(attr)))
          val attrSet = scala.collection.mutable.Set[UnresolvedAttribute]()
          attrList.foreach(attr => {
            if (attrSet.contains(attr)) {
              throw new ParseException(s"find duplicate field :'${attr.name}'",
                insertAction.columns)
            }
            attrSet += attr
          })
          val valueList = insertAction.expression().asScala.map(expression)
          if (attrList.size != valueList.size) {
            throw new ParseException("The columns of source and target tables are not equal: " +
              s"target: $attrList, source: $valueList", insertAction)
          }
          val assignments = attrList.zip(valueList).map(kv => Assignment(kv._1, kv._2))
          InsertAction(insertCond, assignments)
        }
    }
    MergeIntoTable(aliasedTarget, aliasedSource, mergeCondition,
      matchedClauses, notMatchedClauses)
  }

  private def withAssignments(assignCtx: AssignmentListContext): Seq[Assignment] =
    withOrigin(assignCtx) {
      assignCtx.assignment().asScala.map { assign =>
        Assignment(UnresolvedAttribute(visitQualifiedName(assign.key)),
          expression(assign.value))
      }
    }

  override def visitUpdateTable(ctx: UpdateTableContext): LogicalPlan = withOrigin(ctx) {
    val updateStmt = ctx.updateTableStmt()
    val table = UnresolvedRelation(visitTableIdentifier(updateStmt.tableIdentifier()))
    val condition = if (updateStmt.where != null) Some(expression(updateStmt.where)) else None
    val assignments = withAssignments(updateStmt.assignmentList())
    UpdateTable(table, assignments, condition)
  }

  override def visitDeleteTable (ctx: DeleteTableContext): LogicalPlan = withOrigin(ctx) {
    val deleteStmt = ctx.deleteTableStmt()
    val table = UnresolvedRelation(visitTableIdentifier(deleteStmt.tableIdentifier()))
    val condition = if (deleteStmt.where != null) Some(expression(deleteStmt.where)) else None
    DeleteFromTable(table, condition)
  }

  /**
    * Convert the tree to string.
    */
  private def treeToString(tree: ParseTree): String = {
    if (tree.getChildCount == 0) {
      tree.getText
    } else {
      (for (i <- 0 until tree.getChildCount) yield {
        treeToString(tree.getChild(i))
      }).mkString(" ")
    }
  }

  /**
    * Parse the expression tree to spark sql Expression.
    * Here we use the SparkSqlParser to do the parse.
    */
  private def expression(tree: ParseTree): Expression = {
    val expressionText = treeToString(tree)
    delegate.parseExpression(expressionText)
  }

  // ============== The following code is fork from org.apache.spark.sql.catalyst.parser.AstBuilder
  /**
    * If aliases specified in a FROM clause, create a subquery alias ([[SubqueryAlias]]) and
    * column aliases for a [[LogicalPlan]].
    */
  protected def mayApplyAliasPlan(tableAlias: TableAliasContext, plan: LogicalPlan): LogicalPlan = {
    if (tableAlias.strictIdentifier != null) {
      val subquery = SubqueryAlias(tableAlias.strictIdentifier.getText, plan)
      if (tableAlias.identifierList != null) {
        val columnNames = visitIdentifierList(tableAlias.identifierList)
        UnresolvedSubqueryColumnAliases(columnNames, subquery)
      } else {
        subquery
      }
    } else {
      plan
    }
  }
  /**
    * Parse a qualified name to a multipart name.
    */
  override def visitQualifiedName(ctx: QualifiedNameContext): Seq[String] = withOrigin(ctx) {
    ctx.identifier.asScala.map(_.getText)
  }

  /**
    * Create a Sequence of Strings for a parenthesis enclosed alias list.
    */
  override def visitIdentifierList(ctx: IdentifierListContext): Seq[String] = withOrigin(ctx) {
    visitIdentifierSeq(ctx.identifierSeq)
  }

  /**
    * Create a Sequence of Strings for an identifier list.
    */
  override def visitIdentifierSeq(ctx: IdentifierSeqContext): Seq[String] = withOrigin(ctx) {
    ctx.identifier.asScala.map(_.getText)
  }

  /* ********************************************************************************************
   * Table Identifier parsing
   * ******************************************************************************************** */
  /**
    * Create a [[TableIdentifier]] from a 'tableName' or 'databaseName'.'tableName' pattern.
    */
  override def visitTableIdentifier(ctx: TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText))
  }
}

