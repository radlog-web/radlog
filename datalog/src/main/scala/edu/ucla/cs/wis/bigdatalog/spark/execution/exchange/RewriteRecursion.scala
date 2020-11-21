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

import edu.ucla.cs.wis.bigdatalog.spark.execution.recursion.Recursion

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.execution.SparkPlan

case class RewriteRecursion() extends Rule[SparkPlan] {
  def apply(operator: SparkPlan): SparkPlan = {
    val apply = SparkEnv.get.conf.getBoolean("spark.datalog.deltaExchange", true)
    if (!apply) operator

    operator.transform {
      case recursion: Recursion =>
        var initSets = true
        val newLeft = recursion.left match {
          case exchange: ShuffleExchange =>
            initSets = false // init inside ExitExchange
            ExitExchange(recursion.name, exchange.newPartitioning, exchange.child)
          case null => null
          case other =>
            throw new UnsupportedOperationException(s"recursion.left: $other")
        }

        val newRight = recursion.right match {
          case exchange: ShuffleExchange =>
            RecExchange(recursion.name, initSets, exchange.newPartitioning, exchange.child)
          case _ =>
            val fixpointTask = SparkEnv.get.conf.getBoolean("spark.datalog.recursion.fixpointTask", true)
            if (fixpointTask) {
              if (newLeft == null) {
                throw new UnsupportedOperationException(
                  "FixedPointResultTask does not implement delta/all sets initialization when exit rule plan is null.")
              }
              recursion.right
            } else {
              // [Hack] we insert the RecExchange to simulate the behavior of not using fixpointTask
              // Actually no need to do so as the plan is already correct
              // Check [[execution.recursion.Recursion]] for details
              val child = recursion.right
              RecExchange(recursion.name, initSets, child.outputPartitioning, child)
            }
        }

        Recursion(recursion.name, recursion.recOptions, newLeft, newRight, recursion.partitioning)
    }
  }
}
