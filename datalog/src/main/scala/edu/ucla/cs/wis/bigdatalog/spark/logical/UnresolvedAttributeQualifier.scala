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

import edu.ucla.cs.wis.bigdatalog.database.`type`.DbTypeBase
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.{Cast, _}
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.{Operator, OperatorType}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, _}

import scala.collection.mutable

class UnresolvedAttributeQualifier(operator: Operator,
                                   renamed: mutable.HashMap[Operator, String],
                                   val aliasMap: mutable.HashMap[Variable, Alias])
  extends Logging {

  def toExpression(arg: Argument): Expression = {
    arg match {
      case c: Cast => toExpression(c.getValue)
      case v: Variable => toUnresolvedAttribute(v)
      case d: DbTypeBase =>
        Literal.create(TypeUtils.getDbTypeBaseValue(d), TypeUtils.getSparkDataType(d.getDataType))
      case ce: edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.ComparisonExpression => {
        toExpression(ce.getOperation.getSymbol,
          toExpression(ce.getLeft),
          toExpression(ce.getRight))
      }
      case be: edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.BinaryExpression => {
        toExpression(be.getOperation.getSymbol,
          toExpression(be.getLeft),
          toExpression(be.getRight))
      }
      case ue: edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.UnaryExpression => {
        toExpression(ue.getOperation.getSymbol,
          toExpression(ue.getArgument))
      }
      case _ => throw new UnsupportedOperationException(s"Argument: $arg ${arg.getClass()}" )
    }
  }

  private def toExpression(name: String, left: Expression, right: Expression): Expression = {
    name match {
      case "=" => EqualTo(left, right)
      case "~=" => Not(EqualTo(left, right))
      case ">" => GreaterThan(left, right)
      case "<" => LessThan(left, right)
      case ">=" => GreaterThanOrEqual(left, right)
      case "<=" => LessThanOrEqual(left, right)
      case "+" => Add(left, right)
      case "-" => Subtract(left, right)
      case "*" => Multiply(left, right)
      case "/" => Divide(left, right)
      case "mod" => Remainder(left, right)
      case "opc" => Bopc(left, right)
      case _ => throw new UnsupportedOperationException(s"Operation: $name")
    }
  }



  private def toExpression(name: String, arg: Expression): Expression = {
    name match {
      case "abs" => Abs(arg)
      case "log" => Log(arg)
      case "exp" => Exp(arg)
      case "step" => Step(arg)
      case _ => throw new UnsupportedOperationException(s"Operation: $name")
    }
  }

  def toUnresolvedAttribute(v: Variable): UnresolvedAttribute = {
    if (aliasMap.contains(v)) {
      val alias = aliasMap(v)
      UnresolvedAttribute(alias.name)
    } else {
      // find alias if alias name generated in child op
      logDebug(s"qualifiedUnresolvedAttribute - Variable: $v | Operator's child: $operator")
      val option = SourceOperatorResolver.getSourceOperator(v, operator)
      option match {
        case Some(source) =>
          val sourceOpName = renamed.get(source) match {
            case Some(name) => Some(name)
            case _ if source.getOperatorType == OperatorType.PROJECT => None
            case _ => Some(source.getName)
          }

          sourceOpName match {
            case Some(name) => UnresolvedAttribute(name + "." + v.toStringVariableName)
            case _ => UnresolvedAttribute(v.toStringVariableName)
          }
        case _ => UnresolvedAttribute(v.toStringVariableName)
      }
    }
  }

}
