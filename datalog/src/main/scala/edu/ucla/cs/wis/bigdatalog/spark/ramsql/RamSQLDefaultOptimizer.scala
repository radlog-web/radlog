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

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
  * Copied from DefaultOptimizer with ignorance of several optimization rules
  */
class RamSQLDefaultOptimizer(sessionCatalog: SessionCatalog, conf: SQLConf) extends Optimizer(sessionCatalog, conf) {

  override def batches: Seq[Batch] = {
    // Technically some of the rules in Finish Analysis are not optimizer rules and belong more
    // in the analyzer, because they are needed for correctness (e.g. ComputeCurrentTime).
    // However, because we also use the analyzer to canonicalized queries (for view definition),
    // we do not eliminate subqueries or compute current time in the analyzer.
    Batch("Finish Analysis", Once,
      // victor: keep subquery for extracting relation names
      // EliminateSubqueryAliases,
      ReplaceExpressions,
      ComputeCurrentTime,
      GetCurrentDatabase(sessionCatalog),
      RewriteDistinctAggregates) ::
    //////////////////////////////////////////////////////////////////////////////////////////
    // Optimizer rules start here
    //////////////////////////////////////////////////////////////////////////////////////////
    // - Do the first call of CombineUnions before starting the major Optimizer rules,
    //   since it can reduce the number of iteration and the other rules could add/move
    //   extra operators between two adjacent Union operators.
    // - Call CombineUnions again in Batch("Operator Optimizations"),
    //   since the other rules might make two separate Unions operators adjacent.
    Batch("Union", Once,
      CombineUnions) ::
    Batch("Subquery", Once,
      OptimizeSubqueries) ::
    Batch("Replace Operators", fixedPoint,
      ReplaceIntersectWithSemiJoin,
      ReplaceExceptWithAntiJoin,
      ReplaceDistinctWithAggregate) ::
    Batch("Aggregate", fixedPoint,
      RemoveLiteralFromGroupExpressions,
      RemoveRepetitionFromGroupExpressions) ::
    Batch("Operator Optimizations", fixedPoint,
      // Operator push down
      PushThroughSetOperations,
      ReorderJoin,
      EliminateOuterJoin,
      PushPredicateThroughJoin,
      PushDownPredicate,
      LimitPushDown,
      // victor: disable following for bigdatalog
      // ColumnPruning,
      // InferFiltersFromConstraints,
      // Operator combine
      // CollapseRepartition,
      CollapseProject,
      CombineFilters,
      CombineLimits,
      CombineUnions,
      // Constant folding and strength reduction
      NullPropagation,
      FoldablePropagation,
      OptimizeIn(conf),
      ConstantFolding,
      LikeSimplification,
      BooleanSimplification,
      SimplifyConditionals,
      RemoveDispensableExpressions,
      SimplifyBinaryComparison,
      PruneFilters,
      EliminateSorts,
      SimplifyCasts,
      SimplifyCaseConversionExpressions,
      RewriteCorrelatedScalarSubquery,
      EliminateSerialization,
      RemoveAliasOnlyProject) ::
    Batch("Decimal Optimizations", fixedPoint,
      DecimalAggregates) ::
    Batch("Typed Filter Optimization", fixedPoint,
      EmbedSerializerInFilter,
      RemoveAliasOnlyProject) ::
    Batch("LocalRelation", fixedPoint,
      ConvertToLocalRelation) ::
    Batch("OptimizeCodegen", Once,
      OptimizeCodegen(conf)) ::
    Batch("RewriteSubquery", Once,
      RewritePredicateSubquery,
      CollapseProject) ::
    Batch("RamSQL Post-Processing", fixedPoint,
      RemoveCast) :: Nil
  }
}

object RemoveCast extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case u @ Cast(child, _) => child
    }
  }
}
