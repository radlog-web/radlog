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

import edu.ucla.cs.wis.bigdatalog.interpreter.argument.{Argument, Variable}
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.{AliasedArgument, ComparisonExpression}
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.{FilterOperator, Operator, ProjectionOperator}
import edu.ucla.cs.wis.bigdatalog.spark.logical.SourceOperatorResolver
import org.apache.spark.internal.Logging

import scala.collection.JavaConversions._

object FixUnresolvableFilter extends Logging {

  private def canResolve(arg: Argument, child: Operator): Boolean = {
    arg match {
      case v: Variable => SourceOperatorResolver.getSourceOperator(v, child).isDefined
      case _ => true
    }
  }

  // Hack: we really don't know which position it matches to, thus use the hints in variable name
  private def fixUnresolvable(s: String, child: Operator): Argument = {
//    val posInChildOutput = s.replaceAll("[^\\d]", "").toInt - 1
    // check how operator.getArguments works in getSourceOperator
//    val posInChildOutput = if (s.equals("DistCnt") && child.getName.equals("distc")) {
//      child.getArguments.size() - 1
//    } else {
//      s.replaceAll("[^\\d]", "").toInt - 1
//    }

    if (child.isInstanceOf[ProjectionOperator]) {
      var index = -1
      for (i <- 0 until child.getArguments.size()) {
        val arg = child.getArgument(i).asInstanceOf[AliasedArgument]
        if (arg.getAlias.asInstanceOf[Variable].getName.equals(s)) {
          index = i
        }
      }
      child.getArguments.get(index).copy()
    } else {
      val posInChildOutput = s.replaceAll("[^\\d]", "").toInt - 1
      child.getArguments.get(posInChildOutput).copy()
    }


//    child.getArguments.get(posInChildOutput).copy()
  }

  // Fix the OperatorProgram by replace nodes/expressions in-place
  def fix(rootOp: Operator): Unit = {
    rootOp match {
      case f: FilterOperator =>
        // fix: filter children bug
        f.getExpressions.foreach((e: ComparisonExpression) => {
          val child = f.getChildren()(0)
          val l = canResolve(e.getLeft, child)
          if (!l) {
            val u = e.getLeft.toString
            e.setLeft(fixUnresolvable(u, child))
            logWarning(s"left cannot resolve: $u whole expr change to: $e")
          }
          val r = canResolve(e.getRight, child)
          if (!r) {
            val u = e.getRight.toString
            e.setRight(fixUnresolvable(u, child))
            logWarning(s"right cannot resolve: $u whole expr change to: $e")
          }
        })
      case _ =>
      // skip
    }
    rootOp.getChildren.foreach(fix)
  }
}
