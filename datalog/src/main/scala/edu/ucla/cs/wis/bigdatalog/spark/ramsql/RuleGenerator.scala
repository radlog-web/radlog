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
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

import scala.collection.mutable.ListBuffer

object AliasUtil {
  /**
    * Hack: as sometimes Alias.toAttribute converts result to unresolved, we use this function to replace it
    */
  def toAttributeRef(a: Alias): AttributeReference = {
    AttributeReference(a.name, a.child.dataType, a.child.nullable, a.metadata)(a.exprId, a.qualifier, a.isGenerated)
  }

  def equalToExpr(a: Alias): Expression = {
    val ref = toAttributeRef(a)
    val expr = a.child match {
      case CheckOverflow(child, _) => child
      case other => other
    }
    EqualTo(ref, expr)
  }
}

trait RuleGenerator {
  protected def generateHead(planInfo: PlanInfo): HeadLiteral

  def toDatalogRule(planInfo: PlanInfo): DatalogRule = {
    val head = generateHead(planInfo)
    DatalogRule(head, planInfo.goals, planInfo.conds)
      .removeAttributeEqualToConds()
      .replaceAttributeByLiteralInHead()
      .replaceAttributeByUnderscoreInGoals()
  }
}

class DefaultRuleGenerator(predicateName: String) extends RuleGenerator with Logging {
  override def generateHead(planInfo: PlanInfo): HeadLiteral = {
    HeadLiteral(predicateName, genTermsInHeadGoal(planInfo.rootOp))
  }

  private def genTermsInHeadGoal(rootOp: LogicalPlan): Seq[Term] = {
    val res = rootOp match {
      case p: Project =>
        p.projectList.map {
          case a: Alias => Variable(AliasUtil.toAttributeRef(a))
          case a: AttributeReference => Variable(a)
          case e => throw new RuntimeException(s"Project Column: $e")
        }
      case a: Aggregate =>
        val groupingTerms = a.groupingExpressions.map {
          case a: AttributeReference => Variable(a)
          case e => throw new UnsupportedOperationException(s"Grouping Expression: $e")
        }

        val aggrTerms = a.aggregateExpressions collect {
          case aggrExpr @ Alias(_: AggregateExpression, _) => aggrExpr
        } map { // collect only aggregate expressions
          case Alias(_ @ AggregateExpression(aggFunc, _, isDistinct, _), alias) =>
            val functionArgs = aggFunc.children.map {
              case a: AttributeReference => Variable(a)
              case e => throw new UnsupportedOperationException(s"AggFunc Arg: $e")
            }
            Function(functionName(aggFunc, isDistinct), functionArgs, alias)
          case e => throw new UnsupportedOperationException(s"Aggregate Expression: $e")
        }

        groupingTerms ++ aggrTerms
      case other =>
        throw new RuntimeException("startOp is not Project or Aggregate:\n" + other)
    }
    logDebug("TermsInHeadGoal: " + res)
    res
  }

  private def functionName(aggFunc: AggregateFunction, isDistinct: Boolean): String = {
    // [Hack] seems the function name is lost in AggregateExpression
    // use this way to get function name
    var funcName = aggFunc.aggBufferAttributes.head.name
    funcName.toLowerCase match {
      case "max" => "max"
      case "min" => "min"
      case "count" => if (isDistinct) "countd" else "count"
      case "sum" => "sum"
      case e => throw new RuntimeException("Unsupported aggregate function: " + e)
    }
  }
}

/**
  * RecursiveCase is a specific case/condition in one recursive view definition.
  * Syntactically a sub select statement in with-union construct in RamSQL, and will map to one single Datalog rule.
  * This class will take a select column in RecursiveCase and treat it as the corresponding aggregate's parameter.
  * The aggregate function is instead defined in recSchema, thus we need both info.
  */
class RecursiveCaseRuleGenerator(recRelation: RecursiveRelation) extends RuleGenerator with Logging {
  override def generateHead(planInfo: PlanInfo): HeadLiteral = {
    HeadLiteral(recRelation.name, genTermsInHeadGoal(planInfo))
  }

  private def genTermsInHeadGoal(planInfo: PlanInfo): Seq[Term] = {
    def projectColumnToVar(ne: NamedExpression): Variable = {
      ne match {
        // case Alias(l: Literal, _) if useLiteralInAlias => Constant(l.value)
        case a: Alias => Variable(AliasUtil.toAttributeRef(a))
        case a: AttributeReference => Variable(a)
        case e => throw new UnsupportedOperationException(s"Project Column: $e")
      }
    }

    val rootOp = planInfo.rootOp.asInstanceOf[Project]
    val groupingVars = ListBuffer[Variable]()

    // The main purpose of zip with recRelation.columnExprs is to identify any aggregates used in it.
    // As inside the view definition, the selected column does not define any aggregate, instead,
    // aggregate is defined in recRelation.columnExprs (but use the corresponding selected column as its arg).
    (rootOp.projectList zip recRelation.columnExprs.unresolved).map(pair => {
      pair._2 match {
        case Alias(func: UnresolvedFunction, alias) =>
          // If an aggregate function is found in recRelation columnExpr,
          // we use the corresponding output column from projectOp as its parameter.
          // For example: WITH recursive cc (X, min() AS CmpId) AS (SELECT X, X FROM arc)
          // will be transformed to function `mmin(X)`.
          func.name.funcName.toLowerCase match {
            case name @ ("max" | "min") =>
              Function(s"m$name", Seq(projectColumnToVar(pair._1)), alias)
            case name @ ("count" | "sum") =>
              // we need to insert the occurrence variable into the function args thus mcount/msum will have 2 args
              // figure out the occurrence variable automatically
              val occurVar = planInfo.joinConditions match {
                case AttributeEqualTo(left, right) :: Nil =>
                  // either side is ok as we will use AttributeReplacer later
                  Variable(left)
                case Nil =>
                  if (groupingVars.isEmpty) throw new RuntimeException("Cannot decide occurrence variable given empty groupingVars")
                  if (groupingVars.length > 1) logWarning(s"Use first groupingVar as occurrence variable because more than one groupingVars: $groupingVars")
                  // this is the case when we generate exit rule like: reach(B, mcount<(B,C)>) <- B = 1, C = 1.
                  groupingVars.head
                case _ => throw new UnsupportedOperationException(
                  s"Cannot decide occurrence variable given joinConditions: ${planInfo.joinConditions}")
              }
              Function(s"m$name", Seq(occurVar, projectColumnToVar(pair._1)), alias)
            case e =>
              throw new RuntimeException("Invalid aggregate function: " + e)
          }
        case _: UnresolvedAttribute =>
          val v = projectColumnToVar(pair._1) // groupingVar should be a Variable
          groupingVars += v
          v
        case e =>
          throw new RuntimeException("Invalid attrExprs in view def: " + e)
      }
    })
  }
}
