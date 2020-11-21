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

package edu.ucla.cs.wis.bigdatalog.spark.execution

import edu.ucla.cs.wis.bigdatalog.spark.execution.exchange.{RewriteAggregateRecursion, RewriteRecursion}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.datalog.CreateRamSQLViewCommand
import org.apache.spark.sql.execution.joins.CartesianProductExec
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}

class BigDatalogQueryExecution(sparkSession: SparkSession, logicalPlan: LogicalPlan)
  extends QueryExecution(sparkSession, logicalPlan) {

  // victor: ugly hack
  var isCreateView: Boolean = logicalPlan.isInstanceOf[CreateRamSQLViewCommand]

  // victor: we need to execute super.preparations first
  // because Exchange is introduced by [[EnsureRequirements]] rule
  override def preparations: Seq[Rule[SparkPlan]] = super.preparations ++ Seq(
    ShuffleDistinct(sparkSession.sessionState.conf),
    RewriteRecursion(),
    RewriteAggregateRecursion(),
    CheckPlan()
  )
}

case class CheckPlan() extends Rule[SparkPlan] {
  def apply(operator: SparkPlan): SparkPlan = {
    operator.foreach {
      case cart: CartesianProductExec =>
        val recRelationInCartesianProduct = cart.left.find(_.isInstanceOf[RecursiveRelation]).isDefined ||
          cart.right.find(_.isInstanceOf[RecursiveRelation]).isDefined
        if (recRelationInCartesianProduct) {
          throw new UnsupportedOperationException(
            "RecRelation is not supported with CartesianProduct:\n" + cart.toString +
              "As RecRelation's iterator does not support multiple passes' access to its content, which is required for CartesianProduct.\n" +
              "The first-pass of the iterator will compute the delta and only allow delta tuples to produce.\n" +
              "The second-pass will not find any delta thus return an empty iterator, which is incorrect.")
        }
      case _ => // Check successful!
    }
    operator
  }
}
