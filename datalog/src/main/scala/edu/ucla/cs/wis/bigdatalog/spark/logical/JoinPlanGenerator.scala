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

package edu.ucla.cs.wis.bigdatalog.spark.logical

import java.util.concurrent.atomic.AtomicInteger

import edu.ucla.cs.wis.bigdatalog.database.`type`.DbTypeBase
import edu.ucla.cs.wis.bigdatalog.interpreter.Hints
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.{Argument, Variable}
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.{AliasedArgument, AliasedVariable}
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator._
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftOuter}

import scala.collection.JavaConversions._
import scala.collection.mutable

case class JoinCondition(
  leftRelationIndex: Int,
  rightRelationIndex: Int,
  cond: EqualTo)

case class JoinConditions(conds: Seq[JoinCondition])  {
  // In Operator Plan, multi-way join is allowed, but we need to convert it into a chain of two-way joins in Spark.
  // The join condition can only be added to the current join operator
  // whose descendants contain the attribute that the condition refers to.
  def partition(currentRelationIndex: Int): (JoinConditions, JoinConditions) = {
    val (curr, remain) = conds.partition(cond => {
      cond.leftRelationIndex <= currentRelationIndex && cond.rightRelationIndex <= currentRelationIndex
    })
    (JoinConditions(curr), JoinConditions(remain))
  }

  def toAndCondition: Option[Expression] = {
    var joinCondition: Expression = null
    conds.foreach(jc => {
      joinCondition = joinCondition match {
        case null => jc.cond
        case _ => And(joinCondition, jc.cond)
      }
    })
    Option(joinCondition)
  }

  def isEmpty: Boolean = conds.isEmpty
}

object JoinConditions {
  var constantIdx = new AtomicInteger(1)

  private def unresolvedAlias(alias: Alias): org.apache.spark.sql.catalyst.analysis.UnresolvedAlias = {
    org.apache.spark.sql.catalyst.analysis.UnresolvedAlias(alias)
  }

  private def unresolvedAliasVariable(aliasMap: mutable.HashMap[Variable, Alias], alias: Alias, variable: Variable): org.apache.spark.sql.catalyst.analysis.UnresolvedAlias = {
    val res = org.apache.spark.sql.catalyst.analysis.UnresolvedAlias(alias)
    aliasMap.put(variable, alias)
    res
  }

  private def toNamedExpr(arg: Argument, variableFunc: UnresolvedAttributeQualifier): NamedExpression = {
    val expr = arg match {
      case v: Variable =>
        variableFunc.toUnresolvedAttribute(v)
      case d: DbTypeBase =>
        Alias(Literal.create(TypeUtils.getDbTypeBaseValue(d), TypeUtils.getSparkDataType(d.getDataType)), s"c_${constantIdx.getAndIncrement()}")()
      case aa: AliasedArgument =>
        val v = aa.getAlias.asInstanceOf[Variable]
        unresolvedAliasVariable(variableFunc.aliasMap, Alias(variableFunc.toExpression(aa.getArgument), UniqueName.gen(v.getName))(), v)
      case av: AliasedVariable =>
        val v = av.getVariable
        unresolvedAliasVariable(variableFunc.aliasMap, Alias(variableFunc.toExpression(av.getVariable), UniqueName.gen(av.getAlias))(), v)
    }
    expr
  }

  /**
   * The main issue here is to find the correct qualifier, i.e. relation name prefix, of each join attribute.
   * The old method `getRelationName` adds prefix if the particular join child is of certain types.
   * It is buggy because the join attribute will not be prefixed if the child type does not meet,
   * which will cause resolve failure if the attribute names are ambiguous.
   *
   * Now we use the same way as the attribute resolve in Project operator, using `qualifiedUnresolvedAttribute`
   * which recursively finds any base/recursive relation that contains the variable in its argument
   * and assign the operator's name as the prefix.
   *
   * Note for the fix, we cannot simply add SubqueryAlias to each join child and refer attributes using this alias
   * because in LogicalPlanGenerator, the SubqueryAlias are only used for renaming base/recursive relation.
   * LogicalPlanGenerator uses names of base/recursive relation as prefixes (if renamed, using the SubqueryAlias)
   * to distinguish attributes. Check `LogicalPlanGenerator.qualifiedUnresolvedAttribute` for more details.
   */
  def apply(operator: JoinOperator, childPlans: Seq[(LogicalPlan, UnresolvedAttributeQualifier)]): JoinConditions = {
    JoinConditions(operator.getConditions.map(jc => {
      val (_, lvf) = childPlans.get(jc.leftRelationIndex)
      val leftExpr = toNamedExpr(jc.getLeft, lvf)

      val (_, rvf) = childPlans.get(jc.rightRelationIndex)
      val rightExpr = toNamedExpr(jc.getRight, rvf)

      JoinCondition(jc.leftRelationIndex, jc.rightRelationIndex, EqualTo(leftExpr, rightExpr))
    }))
  }
}

object JoinPlanGenerator {
  def generate(operator: JoinOperator,
               childPlans: Seq[(LogicalPlan, UnresolvedAttributeQualifier)],
               joinType: Hints.JoinHint,
               recursivePlanDetails: RecursivePlanDetails): LogicalPlan = {

    def isRecursive(operator: Operator): Boolean = {
      // TODO - evaluate using SparkPlan operators instead of Operator operators
      /* Examples
      * PROJECT -> RECURSIVE_RELATION = is recursive YES
      * PROJECT -> AGGREGATE = is recursive NO
      * PROJECT -> RECURSIVE_CLIQUE = is recursive NO
      * FILTER -> RECURSIVE_RELATION = is recursive YES
      * RECURSIVE_CLIQUE -> FILTER -> RECURSIVE_RELATION = is recursive NO
      * */
      val operatorType = operator.getOperatorType

      // the following are producer relations within a recursion
      if (operatorType == OperatorType.RECURSIVE_RELATION || operatorType == OperatorType.MUTUAL_RECURSIVE_CLIQUE) {
        val ret = recursivePlanDetails.isInRecursiveClique(operator)
        return ret
      }

      // the following are producer relations that are outside a recursion
      if (operatorType == OperatorType.AGGREGATE ||
        operatorType == OperatorType.BASE_RELATION ||
        operatorType == OperatorType.RECURSIVE_CLIQUE ||
        operatorType == OperatorType.TUPLE) {
        return false
      }

      // if any child node of this subtree is a producer inside a recursion, this operator is part of a recursion
      return operator.getChildren.exists(isRecursive(_))
    }

    var joinConditions = JoinConditions(operator, childPlans)

    var plan: LogicalPlan = childPlans.get(0)._1
    var key: String = getRelationAlias(plan)

    var rightPlan: LogicalPlan = null
    var operatorKey: String = null

    for (i <- 1 until childPlans.size) {
      // we implement negation as a left-outer join.
      if (operator.getChild(i).getOperatorType == OperatorType.NEGATION) {
        val negationOperator = operator.getChild(i).asInstanceOf[NegationOperator]
        rightPlan = childPlans.get(i)._1
        key = getRelationAlias(rightPlan)
        operatorKey = key

        var expressions: Expression = null
        negationOperator.getConditions()
          .map(x => {
            if (key == null)
              IsNull(UnresolvedAttribute(x.getRight.asInstanceOf[Variable].toStringVariableName))
            else
              IsNull(UnresolvedAttribute(key + "." + x.getRight.asInstanceOf[Variable].toStringVariableName))
          })
          .foreach(expr => {
            expressions = expressions match {
              case null => expr
              case _ => And(expressions, expr)
            }
          })

        val (currConds, remainConds) = joinConditions.partition(i)
        joinConditions = remainConds
        plan = Filter(expressions, Join(plan, rightPlan, LeftOuter, currConds.toAndCondition))
      } else {
        rightPlan = childPlans.get(i)._1

        // since we're building top-down, we need to identify operators that are recursion
        // so we can optimize joins -- we do not want to cache or broadcast a recursive relation.
        if (joinType == Hints.JoinHint.ShuffleHashJoin) {
          // cache hints will (hopefully) result in a ShuffleHashJoin
          if (isRecursive(operator.getChild(i - 1)) && !isRecursive(operator.getChild(i)))
            rightPlan = CacheHint(rightPlan)
          else if (!isRecursive(operator.getChild(i - 1)) && isRecursive(operator.getChild(i)))
            plan = CacheHint(plan)
        } else if (joinType == Hints.JoinHint.BroadcastHashJoin) {
          // broadcast non-recursive relations as the default
          if (isRecursive(operator.getChild(i - 1)) && !isRecursive(operator.getChild(i)))
            rightPlan = BroadcastHint(rightPlan)
          else if (!isRecursive(operator.getChild(i - 1)) && isRecursive(operator.getChild(i)))
            plan = BroadcastHint(plan)
          else
            rightPlan = BroadcastHint(rightPlan)
        }
        // with no hints, we get SortMergeJoin

        val (currConds, remainConds) = joinConditions.partition(i)
        joinConditions = remainConds
        var fixJoinType: JoinType = Inner
        // TODO: commented for now, this is a hack.
        if (plan.isInstanceOf[SubqueryAlias] && rightPlan.isInstanceOf[Filter]
          && rightPlan.asInstanceOf[Filter].child.isInstanceOf[SubqueryAlias]) {
          if (plan.asInstanceOf[SubqueryAlias].alias == "cntdecy"
            && rightPlan.asInstanceOf[Filter].child.asInstanceOf[SubqueryAlias].alias == "cntdecyn") {
            fixJoinType = FullOuter
          }
        }
        plan = Join(plan, rightPlan, fixJoinType, currConds.toAndCondition)
      }
    }

    if (!joinConditions.isEmpty) {
      throw new RuntimeException(s"JoinConditions: $joinConditions not attached to any Join Operator")
    }

    plan
  }

  private def getRelationAlias(plan: LogicalPlan): String = {
    plan match {
      case a: Filter => getRelationAlias(a.child)
      case b: Project => getRelationAlias(b.child)
      case c: UnresolvedRelation => c.tableName
      case d: Distinct => getRelationAlias(d.child)
      case e: SubqueryAlias => e.alias
      case r: Recursion => r.name
      case r: AggregateRecursion => r.name
      case lr: LinearRecursiveRelation => lr.name
      case nl: NonLinearRecursiveRelation => nl.name
      case other => null
    }
  }

}
