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

import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.AliasedArgument
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.{Operator, OperatorType}

import org.apache.spark.internal.Logging

import scala.collection.JavaConversions._
import scala.util.control.Breaks._

object SourceOperatorResolver extends Logging {
  /**
   * victor: get the closest operator which contains the variable in the output
   * thus we can assign this operator's name as prefix to the variable
   */
  def getSourceOperator(variable: Variable, operator: Operator): Option[Operator] = {
    val ret = operator.getOperatorType match {
      case OperatorType.PROJECT =>
        operator.getArguments.filter(_.isInstanceOf[AliasedArgument]).foreach(arg => {
          if (arg.asInstanceOf[AliasedArgument].getAlias == variable)
            return Some(operator)
        })
        getSourceOperator(variable, operator.getChild(0))
      case OperatorType.JOIN =>
        // Join Operator may have more than two children
        var option: Option[Operator] = None
        breakable {
          for (c <- operator.getChildren) {
            val op = getSourceOperator(variable, c)
            if (op.isDefined) {
              option = op
              break
            }
          }
        }
        option
      case OperatorType.RECURSIVE_CLIQUE |
           OperatorType.MUTUAL_RECURSIVE_CLIQUE |
           OperatorType.UNION |
           OperatorType.AGGREGATE |
           OperatorType.AGGREGATE_FS |
           OperatorType.BASE_RELATION |
           OperatorType.RECURSIVE_RELATION |
           OperatorType.TUPLE =>
        // if any of these cases are encountered, we have found a producer of the values assigned to the variable - i.e., go no deeper
        val variables = operator.getArguments.filter(_.isInstanceOf[Variable]).map(_.asInstanceOf[Variable]) ++
          operator.getArguments.filter(_.isInstanceOf[AliasedArgument]).map(_.asInstanceOf[AliasedArgument].getAlias.asInstanceOf[Variable])
        if (variables.contains(variable)) Some(operator) else None
      case _ =>
        if (operator.getNumberOfChildren > 0) getSourceOperator(variable, operator.getChild(0)) else None
    }
    // getSourceOperator is a recursion call, the end case will be printed first
    // In end case: Start Op will likely be BASE_RELATION, End Op will be the same BASE_RELATION (found) or None (not found)
    logDebug(s"getSourceOperator - Variable: $variable | Start Op: $operator | End Op: ${if (ret.isDefined) ret.get.toString else "None"}")
    ret
  }
}
