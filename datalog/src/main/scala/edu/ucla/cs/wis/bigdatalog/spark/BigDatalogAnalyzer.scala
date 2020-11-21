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

package edu.ucla.cs.wis.bigdatalog.spark

import edu.ucla.cs.wis.bigdatalog.spark.logical.MonotonicAggregate

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, CreateStruct, CreateStructUnsafe, Expression, Generator, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Pivot, _}
import org.apache.spark.sql.catalyst.rules.Rule

class BigDatalogAnalyzer(catalog: SessionCatalog, conf: CatalystConf, maxIterations: Int)
  extends org.apache.spark.sql.catalyst.analysis.Analyzer(catalog, conf, maxIterations) {

  override lazy val batches: Seq[Batch] = Seq(
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
        ResolveAliases2 :: /* APS */
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
      CleanupAliases2) /* APS */
  )

  object ResolveAliases2 extends Rule[LogicalPlan] {
    private def assignAliases(exprs: Seq[NamedExpression]) = {
      exprs.zipWithIndex.map {
        case (expr, i) =>
          expr transformUp {
            case u @ UnresolvedAlias(child, None) => child match {
              case ne: NamedExpression => ne
              case e if !e.resolved => u
              case g: Generator => MultiAlias(g, Nil)
              case c @ Cast(ne: NamedExpression, _) => Alias(c, ne.name)()
              case other => Alias(other, s"_c$i")()
            }
          }
      }.asInstanceOf[Seq[NamedExpression]]
    }

    private def hasUnresolvedAlias(exprs: Seq[NamedExpression]) =
      exprs.exists(_.find(_.isInstanceOf[UnresolvedAlias]).isDefined)

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      /** victor: Only this case is added from [[ResolveAliases.apply]]  */
      case MonotonicAggregate(groups, aggs, child, partitioning) if child.resolved && hasUnresolvedAlias(aggs.asInstanceOf[Seq[NamedExpression]]) =>
        MonotonicAggregate(groups, assignAliases(aggs.asInstanceOf[Seq[NamedExpression]]), child, partitioning)

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

object CleanupAliases2 extends Rule[LogicalPlan] {
  private def trimAliases(e: Expression): Expression = {
    var stop = false
    e.transformDown {
      // CreateStruct is a special case, we need to retain its top level Aliases as they decide the
      // name of StructField. We also need to stop transform down this expression, or the Aliases
      // under CreateStruct will be mistakenly trimmed.
      case c: CreateStruct if !stop =>
        stop = true
        c.copy(children = c.children.map(trimNonTopLevelAliases))
      case c: CreateStructUnsafe if !stop =>
        stop = true
        c.copy(children = c.children.map(trimNonTopLevelAliases))
      case Alias(child, _) if !stop => child
    }
  }

  def trimNonTopLevelAliases(e: Expression): Expression = e match {
    case a: Alias =>
      Alias(trimAliases(a.child), a.name)(a.exprId, a.qualifier, a.explicitMetadata)
    case other => trimAliases(other)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case Project(projectList, child) =>
      val cleanedProjectList =
        projectList.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Project(cleanedProjectList, child)

    case Aggregate(grouping, aggs, child) =>
      val cleanedAggs = aggs.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Aggregate(grouping.map(trimAliases), cleanedAggs, child)

    case MonotonicAggregate(grouping, aggs, child, partitioning) =>
      val cleanedAggs = aggs.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      MonotonicAggregate(grouping.map(trimAliases), cleanedAggs, child, partitioning)

    case w @ Window(windowExprs, partitionSpec, orderSpec, child) =>
      val cleanedWindowExprs =
        windowExprs.map(e => trimNonTopLevelAliases(e).asInstanceOf[NamedExpression])
      Window(cleanedWindowExprs, partitionSpec.map(trimAliases),
        orderSpec.map(trimAliases(_).asInstanceOf[SortOrder]), child)

    case other =>
      var stop = false
      other transformExpressionsDown {
        case c: CreateStruct if !stop =>
          stop = true
          c.copy(children = c.children.map(trimNonTopLevelAliases))
        case c: CreateStructUnsafe if !stop =>
          stop = true
          c.copy(children = c.children.map(trimNonTopLevelAliases))
        case Alias(child, _) if !stop => child
      }
  }
}