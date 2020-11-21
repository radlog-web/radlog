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

package edu.ucla.cs.wis.bigdatalog.spark.ramsql

import edu.ucla.cs.wis.bigdatalog.spark.ramsql.structs._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.mutable.ListBuffer

class PlanInfoExtractor extends Logging {

  private val goals = ListBuffer[GoalLiteral]()
  private val projectAliasExprs = ListBuffer[Expression]()
  private val filterConditions = ListBuffer[Expression]()
  private val joinConditions = ListBuffer[Expression]()

  private def baseRelation(tableName: String, output: Seq[Attribute]): GoalLiteral =
    GoalLiteral(tableName, output.map(_.asInstanceOf[AttributeReference]).map(Variable).toList)

  private def handleSubquery(q: SubqueryAlias): Unit = {
    def extractChild(q: SubqueryAlias): GoalLiteral = {
      logDebug("handleSubquery.extractChild: " + q)
      q.child match {
        case s: SubqueryAlias => extractChild(s) // if a relation has an alias, then we will have nested subquery
        case r: LogicalRDD => baseRelation(q.alias, r.output)
        case r: LocalRelation => baseRelation(q.alias, r.output)
        case r: LogicalRelation => baseRelation(q.alias, r.output)
        case r: RecursiveRelation => baseRelation(q.alias, r.output)
        case e => throw new RuntimeException("Not a Subquery Child: " + e)
      }
    }
    goals += extractChild(q)
  }

  private def handleProject(project: Project): Unit = {
    logDebug("Project - projectList: " + project.projectList)
    project.projectList foreach { p =>
      logDebug("projectList item: " + p.getClass.getSimpleName)
      p match {
        // change alias in project to equalTo expression
        case a: Alias => projectAliasExprs += AliasUtil.equalToExpr(a)
        case _ =>
      }
    }

    project.child match {
      case f: Filter => handleFilter(f)
      case j: Join => handleJoin(j)
      /**
        * This case will not appear if using [[LocalRelation]], as it will be eliminated by
        * [[org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation]] rule
        */
      case q: SubqueryAlias => handleSubquery(q)
      case OneRowRelation => // skip
      case e => throw new RuntimeException("Not a Project Child: " + e)
    }
  }

  private def handleFilter(filter: Filter): Unit = {
    logDebug("Filter - cond: " + filter.condition)
    filterConditions += filter.condition
    filter.child match {
      case q: SubqueryAlias => handleSubquery(q)
      case e => throw new RuntimeException("Not a Filter Child: " + e)
    }
  }

  private def handleJoin(join: Join): Unit = {
    logDebug("JoinType: " + join.joinType + ", Cond: " + join.condition)
    join.condition.map(joinConditions += _)

    handleJoinChild(join.left)
    handleJoinChild(join.right)

    def handleJoinChild(child: LogicalPlan): Unit = {
      child match {
        case j: Join => handleJoin(j)
        case f: Filter => handleFilter(f)
        case q: SubqueryAlias => handleSubquery(q)
        case e => throw new RuntimeException("Not a Join Child: " + e)
      }
    }
  }

  // handleAggregate is only possible to be called when generating outermost query
  private def handleAggregate(aggr: Aggregate): Unit = {
    logDebug("Aggregate: " + aggr)
    aggr.aggregateExpressions foreach { p =>
      logDebug("aggregate expr: " + p)
      p match {
        // change alias in project to equalTo expression
        case a: Alias if !a.child.isInstanceOf[AggregateExpression] =>
          projectAliasExprs += AliasUtil.equalToExpr(a)
        case _ =>
      }
    }

    aggr.child match {
      case f: Filter => handleFilter(f)
      case j: Join => handleJoin(j)
      /**
        * This case will not appear if using [[LocalRelation]], as it will be eliminated by
        * [[org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation]] rule
        */
      case q: SubqueryAlias => handleSubquery(q)
      case e => throw new RuntimeException("Not an Aggregate Child: " + e)
    }
  }

  def topDownVisits(rootOp: Aggregate): PlanInfo = {
    handleAggregate(rootOp)
    PlanInfo(rootOp, goals.toList, projectAliasExprs.toList, filterConditions.toList, joinConditions.toList)
  }

  def topDownVisits(rootOp: Project): PlanInfo = {
    handleProject(rootOp)
    PlanInfo(rootOp, goals.toList, projectAliasExprs.toList, filterConditions.toList, joinConditions.toList)
  }

}
