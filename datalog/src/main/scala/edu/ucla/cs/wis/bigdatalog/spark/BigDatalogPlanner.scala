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

import edu.ucla.cs.wis.bigdatalog.spark.execution.{BroadcastHashJoinPackedExec, CacheBuildSideShuffleHashJoin}
import edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates.{MCount, MSum, MonotonicAggregate}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{BroadcastHint, CacheHint, LogicalPlan}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight}
import org.apache.spark.sql.internal.SQLConf

class BigDatalogPlanner(val sparkSession: SparkSession,
                        override val conf: SQLConf,
                        override val extraStrategies: Seq[Strategy])
  extends SparkPlanner(sparkSession.sparkContext, conf, extraStrategies) {

  override def strategies: Seq[Strategy] =
    (Recursion :: MonotonicAggregation :: JoinSelectionByHints :: Nil) ++ super.strategies

  object Recursion extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.LinearRecursiveRelation(name, output, partitioning) =>
        execution.LinearRecursiveRelation(name, output, partitioning) :: Nil
      case logical.NonLinearRecursiveRelation(name, output, partitioning) =>
        execution.NonLinearRecursiveRelation(name, output, partitioning) :: Nil
      case logical.AggregateRelation(name, output, partitioning) =>
        execution.AggregateRelation(name, output, partitioning) :: Nil
      // recursion
      case logical.Recursion(name, recOptions, left, right, partitioning) =>
        val planLeft = if (left == null) null else planLater(left)
        execution.recursion.Recursion(name, recOptions, planLeft, planLater(right), partitioning) :: Nil
      case logical.AggregateRecursion(name, recOptions, left, right, partitioning) =>
        val planLeft = if (left == null) null else planLater(left)
        execution.recursion.AggregateRecursion(name, recOptions, planLeft, planLater(right), partitioning) :: Nil
      case _ => Nil
    }
  }

  /*Copied and adapted from Aggregation strategy in SparkStrategies*/
  object MonotonicAggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.MonotonicAggregate(groupingExpressions, resultExpressions, child, partitioning) => {
        // A single aggregate expression might appear multiple times in resultExpressions.
        // In order to avoid evaluating an individual aggregate function multiple times, we'll
        // build a set of the distinct aggregate expressions and build a function which can
        // be used to re-write expressions so that they reference the single copy of the
        // aggregate function which actually gets computed.
        val aggregateExpressions = resultExpressions.flatMap { expr =>
          expr.collect {
            case agg: AggregateExpression => agg
          }
        }.distinct
        // For those distinct aggregate expressions, we create a map from the
        // aggregate function to the corresponding attribute of the function.
        val aggregateFunctionToAttribute = aggregateExpressions.map { agg =>
          val aggregateFunction = agg.aggregateFunction
          val attribute = Alias(aggregateFunction, aggregateFunction.toString)().toAttribute
          (aggregateFunction, agg.isDistinct) -> attribute
        }.toMap

        val (functionsWithDistinct, functionsWithoutDistinct) =
          aggregateExpressions.partition(_.isDistinct)
        if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
          // This is a sanity check. We should not reach here when we have multiple distinct
          // column sets. Our MultipleDistinctRewriter should take care this case.
          sys.error("You hit a query analyzer bug. Please report your query to " +
            "Spark user mailing list.")
        }

        val namedGroupingExpressions = groupingExpressions.map {
          case ne: NamedExpression => ne -> ne
          // If the expression is not a NamedExpressions, we add an alias.
          // So, when we generate the result of the operator, the Aggregate Operator
          // can directly get the Seq of attributes representing the grouping expressions.
          case other =>
            val withAlias = Alias(other, other.toString)()
            other -> withAlias
        }
        val groupExpressionMap = namedGroupingExpressions.toMap

        // The original `resultExpressions` are a set of expressions which may reference
        // aggregate expressions, grouping column values, and constants. When aggregate operator
        // emits output rows, we will use `resultExpressions` to generate an output projection
        // which takes the grouping columns and final aggregate result buffer as input.
        // Thus, we must re-write the result expressions so that their attributes match up with
        // the attributes of the final result projection's input row:
        val rewrittenResultExpressions = resultExpressions.map { expr =>
          expr.transformDown {
            case AggregateExpression(aggregateFunction, _, isDistinct, _) =>
              // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
              // so replace each aggregate expression by its corresponding attribute in the set:
              aggregateFunctionToAttribute(aggregateFunction, isDistinct)
            case expression =>
              // Since we're using `namedGroupingAttributes` to extract the grouping key
              // columns, we need to replace grouping key expressions with their corresponding
              // attributes. We do not rely on the equality check at here since attributes may
              // differ cosmetically. Instead, we use semanticEquals.
              groupExpressionMap.collectFirst {
                case (expr, ne) if expr semanticEquals expression => ne.toAttribute
              }.getOrElse(expression)
          }.asInstanceOf[NamedExpression]
        }

        // == bigDatalog specific ==
        if (sparkContext.getConf.getBoolean("spark.datalog.monotonicaggregate.usepartial", true)) {
          planMonotonicAggregate(
            namedGroupingExpressions.map(_._2),
            aggregateExpressions,
            aggregateFunctionToAttribute,
            rewrittenResultExpressions,
            partitioning,
            planLater(child))
        } else {
          planMonotonicAggregateWithoutPartial(
            namedGroupingExpressions.map(_._2),
            aggregateExpressions,
            aggregateFunctionToAttribute,
            rewrittenResultExpressions,
            partitioning,
            planLater(child))
        }
        // == end bigDatalog specific ==
      }
      case _ => Nil
    }
  }

  def planMonotonicAggregateWithoutPartial(groupingExpressions: Seq[NamedExpression],
                                           aggregateExpressions: Seq[AggregateExpression],
                                           aggregateFunctionToAttribute: Map[(AggregateFunction, Boolean), Attribute],
                                           resultExpressions: Seq[NamedExpression],
                                           partitioning: Seq[Int],
                                           child: SparkPlan): Seq[SparkPlan] = {
    val completeAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
    val completeAggregateAttributes = completeAggregateExpressions.map {
      expr => aggregateFunctionToAttribute(expr.aggregateFunction, expr.isDistinct)
    }

    MonotonicAggregate(
      groupingExpressions = groupingExpressions,
      aggregateExpressions = completeAggregateExpressions,
      aggregateAttributes = completeAggregateAttributes,
      initialInputBufferOffset = 0,
      resultExpressions = resultExpressions,
      partitioning = partitioning,
      child = child
    ) :: Nil
  }

  def planMonotonicAggregate(groupingExpressions: Seq[NamedExpression],
                             aggregateExpressions: Seq[AggregateExpression],
                             aggregateFunctionToAttribute: Map[(AggregateFunction, Boolean), Attribute],
                             resultExpressions: Seq[NamedExpression],
                             partitioning: Seq[Int],
                             child: SparkPlan): Seq[SparkPlan] = {

    val nonMonotonic = sparkContext.getConf.getBoolean("spark.datalog.recursion.nonMonotonic", false)
    val subGroupingExpressions = if (nonMonotonic) Seq() else {
      aggregateExpressions.map(_.aggregateFunction).collect {
        case mcount: MCount => mcount.subGroupingKey.asInstanceOf[NamedExpression]
        case msum: MSum => msum.subGroupingKey.asInstanceOf[NamedExpression]
      }
    }

    // 1. Create an Aggregate Operator for partial aggregations.
    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    val partialAggregateExpressions =
      // TODO: victor - check correctness if we do not wrap with Max
      // if (isMCount) {
      //   aggregateExpressions.map(ae => {
      //     val aggregateFunction = Max(ae.aggregateFunction.children(0))
      //     AggregateExpression(aggregateFunction, Partial, ae.isDistinct)
      //   })
      // } else
      aggregateExpressions.map(_.copy(mode = Partial))

    // victor - if using MCount, the partialGroupingExpressions will include subGroupingExpressions (occurrence variable)
    val partialGroupingExpressions = groupingExpressions ++ subGroupingExpressions
    val partialAggregateAttributes = partialAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

    val partialResultExpressionsAggregateComponent = partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)
    val partialResultExpressions = partialGroupingExpressions ++ partialResultExpressionsAggregateComponent

    val partialAggregate = new HashAggregateExec(
      requiredChildDistributionExpressions = None,
      groupingExpressions = partialGroupingExpressions,
      aggregateExpressions = partialAggregateExpressions,
      aggregateAttributes = partialAggregateAttributes,
      initialInputBufferOffset = 0,
      resultExpressions = partialResultExpressions,
      child = child)

    // 2. Create an Aggregate Operator for final aggregations.
    val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))

    // TODO: victor - check correctness if we do not connect with max
    // initialize aggregate buffers and connect mcount with max
    // finalAggregateExpressions.map(_.aggregateFunction)
    //   .zip(partialResultExpressionsAggregateComponent)
    //   .collect { case (mcount: MCount, ar: AttributeReference) => {
    //     mcount.inputAggBufferAttributes
    //     mcount.max = ar
    //   }}

    // The attributes of the final aggregation buffer, which is presented as input to the result projection:
    val finalAggregateAttributes = finalAggregateExpressions.map {
      expr => aggregateFunctionToAttribute(expr.aggregateFunction, expr.isDistinct)
    }

    val finalAggregate = MonotonicAggregate(
      groupingExpressions = groupingAttributes,
      aggregateExpressions = finalAggregateExpressions,
      aggregateAttributes = finalAggregateAttributes,
      // TODO: victor - it should be `groupingExpressions.length` (a bug in large chance), but luckily as we do not use ImperativeAggregate
      initialInputBufferOffset = 0,
      resultExpressions = resultExpressions,
      partitioning = partitioning,
      child = partialAggregate
    )

    finalAggregate :: Nil
  }

  /**
    * Matches a plan whose partitions can be cached and re-used
    */
  object CanCache {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
      case CacheHint(p) => Some(p)
      case _ => None
    }
  }

  object CanBroadcast {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
      case BroadcastHint(p) => Some(p)
      case _ => None
    }
  }

  private lazy val packedBroadcast = sparkContext.getConf.getBoolean("spark.datalog.packedBroadcast", false)

  object JoinSelectionByHints extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, CanCache(right)) =>
        Seq(CacheBuildSideShuffleHashJoin(leftKeys, rightKeys, Inner, BuildRight, condition, planLater(left), planLater(right)))

      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, CanCache(left), right) =>
        Seq(CacheBuildSideShuffleHashJoin(leftKeys, rightKeys, Inner, BuildLeft, condition, planLater(left), planLater(right)))

      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, CanBroadcast(right)) if packedBroadcast =>
        Seq(BroadcastHashJoinPackedExec(leftKeys, rightKeys, Inner, BuildRight, condition, planLater(left), planLater(right)))

      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, CanBroadcast(left), right) if packedBroadcast =>
        Seq(BroadcastHashJoinPackedExec(leftKeys, rightKeys, Inner, BuildLeft, condition, planLater(left), planLater(right)))

      case _ => JoinSelection.apply(plan)
    }
  }
}
