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

package edu.ucla.cs.wis.bigdatalog.spark.logical.patch

import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator._

object FixMutualRecursiveClique {
  private def existOtherRecRelationMC(root: CliqueOperator): Boolean = {
    val children = Array(root.getExitRulesOperator, root.getRecursiveRulesOperator)
    children.exists(c => existOtherRecRelation(c, root.getName))
  }

  private def existOtherRecRelation(currOp: Operator, name: String): Boolean = {
    if (currOp == null) {
      return false
    }
    if (currOp.isInstanceOf[CliqueOperator]) {
      return true
    }
    if (currOp.getOperatorType == OperatorType.RECURSIVE_RELATION) {
      currOp.getName != name
    } else {
      currOp.getChildren.exists(c => existOtherRecRelation(c, name))
    }
  }

  def fix(root: Operator): Unit = {
    if (root == null) {
      return
    }
    // victor: [Hack] we need to fix the incorrectly identified mutual recursive clique operator
    // as it is generally difficult to do so in ProgramGenerator
    var children = root.getChildren
    val opType = root.getOperatorType
    opType match {
      case OperatorType.MUTUAL_RECURSIVE_CLIQUE =>
        val c = root.asInstanceOf[CliqueOperator]
        if (!existOtherRecRelationMC(c)) {
          // fix if not mutual recursion
          root.setOperatorType(OperatorType.RECURSIVE_CLIQUE)
        }
        children = Array(c.getExitRulesOperator, c.getRecursiveRulesOperator)
      case OperatorType.RECURSIVE_CLIQUE =>
        val c = root.asInstanceOf[CliqueOperator]
        children = Array(c.getExitRulesOperator, c.getRecursiveRulesOperator)
      case _ => // pass
    }
    children.foreach(fix)
  }
}
