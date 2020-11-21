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

import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.{CliqueOperator, Operator}

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable.HashMap

class RecursivePlanDetails(rootOp: Operator) extends Serializable {
  private val baseRelationsByName = new HashMap[String, LogicalPlan]

  def addBaseRelation(name: String, obj: LogicalPlan) = {
    baseRelationsByName.put(name, obj)
  }

  def containsBaseRelation(name: String): Boolean = {
    baseRelationsByName.contains(name)
  }

  def isInRecursiveClique(op: Operator): Boolean = {
    // Hack: we cannot access parent of the operator thus we can only search from the rootOp
    def topDownSearch(destOp: Operator, currOp: Operator, _m: Boolean): Boolean = {
      if (currOp == null) {
        // victor: this case will hit when we are inside of a mutual recursion CliqueOperator
        // and its ExitRulesOperator is null
        return false
      }
      var marked = _m
      var children = currOp.getChildren
      currOp match {
        case c: CliqueOperator =>
          marked = true
          children = Array(c.getExitRulesOperator, c.getRecursiveRulesOperator)
        case _ => // do nothing
      }
      if (destOp == currOp) {
        marked
      } else {
        children.exists(c => topDownSearch(destOp, c, marked))
      }
    }
    topDownSearch(op, this.rootOp, false)
  }
}
