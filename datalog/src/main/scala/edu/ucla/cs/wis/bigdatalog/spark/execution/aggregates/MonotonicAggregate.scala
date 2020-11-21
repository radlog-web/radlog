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

package edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates

import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogSessionState
import edu.ucla.cs.wis.bigdatalog.spark.execution.exchange.{ExitAggrExchange, RecAggrExchange}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, _}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

/**
 * Operator to perform monotonic aggregates on the shuffled relation.
 * The child is often an [[org.apache.spark.sql.execution.exchange.ShuffleExchange]] operator.
 *
 * Based on its position in the plan, it is handled in two cases:
 * 1. In exit rule plan, it is replaced by [[ExitAggrExchange]]
 * 2. In recursive rule plan, it is replaced by [[RecAggrExchange]]
 */
case class MonotonicAggregate(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    partitioning: Seq[Int],
    child: SparkPlan)
  extends UnaryExecNode /* TODO: victor add CodegenSupport */ {

  val aggrType = sparkContext.getConf.get("spark.datalog.kddlog.aggrType", "old")

  @transient
  final val sessionState = SparkSession.getActiveSession.orNull
    .sessionState.asInstanceOf[BigDatalogSessionState]

  override def requiredChildDistribution: List[Distribution] = {
    // victor: bigdatalog specific
    if (groupingExpressions == Nil) {
      AllTuples :: Nil
    } else {
      if (partitioning == Nil) {
        ClusteredDistribution(groupingExpressions) :: Nil
      } else {
        ClusteredDistribution(partitioning.zip(resultExpressions)
          .filter(_._1 == 1).map(_._2)) :: Nil
      }
    }
  }

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = {
    val numPartitions = sessionState.conf.numShufflePartitions
    if (partitioning == Nil) {
      UnknownPartitioning(numPartitions)
    } else {
      HashPartitioning(partitioning.zip(output).filter(_._1 == 1).map(_._2), numPartitions)
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "aggregate time"))

  val testFallbackStartsAt: Option[(Int, Int)] = {
    sqlContext.getConf("spark.sql.TungstenAggregate.testFallbackStartsAt", null) match {
      case null | "" => None
      case fallbackStartsAt =>
        val splits = fallbackStartsAt.split(",").map(_.trim)
        Some((splits.head.toInt, splits.last.toInt))
    }
  }

  def isMin: Boolean = aggregateExpressions.forall(_.aggregateFunction.isInstanceOf[MMin])

  def isMax: Boolean = aggregateExpressions.forall(_.aggregateFunction.isInstanceOf[MMax])

  final val aggrIterType: AggrIterType = {
    val nonMonotonic = sparkContext.getConf
      .getBoolean("spark.datalog.recursion.nonMonotonic", false)

    if (aggregateExpressions.forall(_.aggregateFunction.isInstanceOf[MSum])) {
      // currently only support 2 columns,
      // check PrimitiveMSum/NonmonoSumAggregationIterator for details
      if ((output.length == 2) ||
        (output.length == 3) ||
        (output.length == 4) ||
        output.length == 5 || output.length == 6 || output.length == 7) {
        // if (output.length == 2 || output.length == 3) {
        if (nonMonotonic) SumAggrIterType else PrimitiveMSumAggrIterType
      } else {
        throw new UnsupportedOperationException(
          s"SumAggrIter only support 2 columns (int, double) or 3 columns (int, int, double)," +
            s" but get: (${output.map(_.dataType)})")
      }
    } else if (aggregateExpressions.forall(_.aggregateFunction.isInstanceOf[MCount])) {
      // currently only support 2 or 3 int columns,
      // check PrimitiveMCountAggregationIterator for details
      if ((output.length == 2) ||
        (output.length == 3) ||
        (output.length == 4) ||
        output.length == 5 || output.length == 6 || output.length == 7) {
        PrimitiveMCountAggrIterType
      } else {
        throw new UnsupportedOperationException(
          s"MCountAggrIter only support 2 or 3 int columns, but get: (${output.map(_.dataType)})")
      }
    } else if (sparkContext.getConf.get("spark.datalog.aggrIterType", "t").startsWith("p") ) {
      // currently only support 2 int columns, check PrimitiveAggregationIterator for details
      if ((output.length == 2) ||
        (output.length == 3) ||
        (output.length == 4) ||
        output.length == 5 || output.length == 6 || output.length == 7) {
        PrimitiveAggrIterType
      } else {
        throw new UnsupportedOperationException(
          s"PrimitiveAggrIter only support 2 int columns, but get: (${output.map(_.dataType)})")
      }
    } else {
      TungstenMAggrIterType
    }
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    // This check is not necessary as it is ensured by [[EnsureRequirements]]
    // if (!child.outputPartitioning.satisfies(this.requiredChildDistribution.head)) {
    //   throw new SparkException("There is a missing exchange operator" +
    //     "which should have repartitioned the input rdd!")
    // }
    throw new UnsupportedOperationException("MonotonicAggregate does not support execute code path")
  }

  override def simpleString: String = {
    val allAggregateExpressions = aggregateExpressions
    val keyString = groupingExpressions.mkString("[", ",", "]")
    val functionString = allAggregateExpressions.mkString("[", ",", "]")
    val outputString = output.mkString("[", ",", "]")
    s"MonotonicAggregate(key=$keyString, functions=$functionString, output=$outputString)"
  }

}
