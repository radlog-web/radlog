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

package edu.ucla.cs.wis.bigdatalog.spark.execution.exchange

import edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates._
import edu.ucla.cs.wis.bigdatalog.spark.execution.recursion.AggregateRecursion

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchange

case class RewriteAggregateRecursion() extends Rule[SparkPlan] {
  private def isMAggr(fun: AggregateFunction): Boolean = {
    fun match {
      case _: MMax => true
      case _: MMin => true
      case _: MCount => true
      case _: MSum => true
      case _: CMin => true
      case _: CMax => true
      case _: MAvg => true
      case _ => false
    }
  }

  def checkChild(aggr: MonotonicAggregate): Unit = {
    if (!aggr.child.isInstanceOf[ShuffleExchange]) {
      throw new RuntimeException("MonotonicAggregate child is not ShuffleExchange:\n"
        + aggr.toString)
    }

    // Youfu commented the following 5 lines to support chain aggregation
    /*
    val aggrExprs = aggr.aggregateExpressions
    val numOfMAggrs = aggrExprs.count(f => isMAggr(f.aggregateFunction))
    if (numOfMAggrs > 1) {
      throw new UnsupportedOperationException(s"NumOfMAggrs: $numOfMAggrs > 1")
    } */
  }

  def apply(operator: SparkPlan): SparkPlan = {
    val apply = SparkEnv.get.conf.getBoolean("spark.datalog.deltaAggrExchange", true)
    if (!apply) operator

    operator.transform {
      case recursion: AggregateRecursion =>
        var initAggrMap = true
        val newLeft = recursion.left match {
          case aggr: MonotonicAggregate =>
            checkChild(aggr)
            val exitMaggr = recursion.left.asInstanceOf[MonotonicAggregate]
            val exitExchange = exitMaggr.child.asInstanceOf[ShuffleExchange]
            initAggrMap = false // init inside ExitAggrExchange
            ExitAggrExchange(
              recursion.name,
              exitMaggr,
              exitExchange.newPartitioning,
              exitExchange.child
            )
          case null => null
          case other =>
            throw new UnsupportedOperationException(s"recursion.left: $other")
        }

        val recMaggr = recursion.right.asInstanceOf[MonotonicAggregate]
        checkChild(recMaggr)
        val recExchange = recMaggr.child.asInstanceOf[ShuffleExchange]

        // We directly replace Exchange operator in plan,
        // thus need Exchange.child to be the child of this operator
        val newRight = RecAggrExchange(
          recursion.name,
          recMaggr,
          initAggrMap,
          recExchange.newPartitioning,
          recExchange.child
        )

        AggregateRecursion(recursion.name, recursion.recOptions,
          newLeft, newRight, recursion.partitioning)
    }
  }
}
