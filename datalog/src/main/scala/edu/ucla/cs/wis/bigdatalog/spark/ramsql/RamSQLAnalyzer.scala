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

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, ExtractValue, Generator, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toPrettySQL

/**
  * Union Plan is constructed from "with recursive" construct in RamSQL.
  * To generate a corresponding datalog program, this class needs to analyze the "Optimized Logical Plan".
  *
  * [[Analyzer.ResolveRelations]] where [[UnresolvedRelation]] in parsed plan is replaced
  *
  * [[UnresolvedAttribute]] (one case) used for function args, e.g. sum(i)
  * [[UnresolvedAlias]] (one case) used for projection, e.g. select i from ...
  *
  * [[org.apache.spark.sql.types.StructType.toAttributes]]:
  * create new attribute references for base relations, assign [[NamedExpression.newExprId]]
  *
  * [[Analyzer.ResolveReferences.apply#case q: LogicalPlan]]: resolve expression in unresolvedalias,
  * e.g. resolve unresolvedalias('Basic.Part) to AttributeReference(Part#0)
  */
class RamSQLAnalyzer(catalog: SessionCatalog, conf: CatalystConf)
  extends Analyzer(catalog, conf) {

  override lazy val batches: Seq[Batch] = Seq(
//    Batch("WithRec Transformation", Once,
//      WithRecTransformation),
    Batch("Substitution", fixedPoint,
      CTESubstitution,
      WindowsSubstitution,
      EliminateUnions),
    Batch("Resolution", fixedPoint,
      ResolveTableValuedFunctions ::
      ResolveRelations ::
      ResolveReferences ::
      ResolveDeserializer ::
      ResolveNewInstance ::
      ResolveUpCast ::
      ResolveGroupingAnalytics ::
      ResolvePivot ::
      ResolveOrdinalInOrderByAndGroupBy ::
      ResolveMissingReferences ::
      ExtractGenerator ::
      ResolveGenerate ::
      ResolveFunctions ::
      RamSQLResolveAliases :: // victor
      ResolveSubquery ::
      ResolveWindowOrder ::
      ResolveWindowFrame ::
      ResolveNaturalAndUsingJoin ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      TimeWindowing ::
      ResolveInlineTables ::
      TypeCoercion.typeCoercionRules ++
      extendedResolutionRules : _*),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("UDF", Once,
      HandleNullInputsForUDF),
    Batch("FixNullability", Once,
      FixNullability),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )

//  object WithRecTransformation extends Rule[LogicalPlan] {
//    // TODO allow subquery to define CTE
//    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
//      case With(child, relations) => substituteCTE(child, relations)
//      case other => other
//    }
//  }

  /**
   * Replaces [[UnresolvedAlias]]s with concrete aliases.
   */
  object RamSQLResolveAliases extends Rule[LogicalPlan] {
    private def assignAliases(exprs: Seq[NamedExpression]) = {
      exprs.zipWithIndex.map {
        case (expr, i) =>
          expr.transformUp { case u @ UnresolvedAlias(child, optGenAliasFunc) =>
            child match {
              case ne: NamedExpression => ne
              case e if !e.resolved => u
              case g: Generator => MultiAlias(g, Nil)
              case c @ Cast(ne: NamedExpression, _) => Alias(c, ne.name)()
              case e: ExtractValue => Alias(e, toPrettySQL(e))()
              case e if optGenAliasFunc.isDefined =>
                Alias(child, optGenAliasFunc.get.apply(e))()
              case e => Alias(e, s"C$i")() // victor, ensure alias to start with uppercase
            }
          }
      }.asInstanceOf[Seq[NamedExpression]]
    }

    private def hasUnresolvedAlias(exprs: Seq[NamedExpression]) =
      exprs.exists(_.find(_.isInstanceOf[UnresolvedAlias]).isDefined)

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case Aggregate(groups, aggs, child) if child.resolved && hasUnresolvedAlias(aggs) =>
        Aggregate(groups, assignAliases(aggs), child)

      case g: GroupingSets if g.child.resolved && hasUnresolvedAlias(g.aggregations) =>
        g.copy(aggregations = assignAliases(g.aggregations))

      case Pivot(groupByExprs, pivotColumn, pivotValues, aggregates, child)
        if child.resolved && hasUnresolvedAlias(groupByExprs) =>
        Pivot(assignAliases(groupByExprs), pivotColumn, pivotValues, aggregates, child)

      case Project(projectList, child) if child.resolved && hasUnresolvedAlias(projectList) =>
        Project(assignAliases(projectList), child)
    }
  }

}
