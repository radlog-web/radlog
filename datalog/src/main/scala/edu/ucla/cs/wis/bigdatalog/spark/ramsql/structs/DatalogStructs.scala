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

package edu.ucla.cs.wis.bigdatalog.spark.ramsql.structs

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, NamedExpression}
import org.apache.spark.sql.types.Decimal

abstract class Literal {
  def predicate: String
  def terms: Seq[Term]
  override def toString: String = s"$predicate(${terms.mkString(", ")})"
}

case class HeadLiteral(predicate: String, terms: Seq[Term]) extends Literal {
  def toAliasString: String = s"$predicate(${terms.map(_.toAliasString).mkString(", ")})"

  def referencedAttrIds: Seq[ExprId] = terms.flatMap(_.referencedAttrIds)
}

case class GoalLiteral(predicate: String, terms: Seq[Term]) extends Literal {
  def referencedAttrIds: Seq[ExprId] = terms.flatMap(_.referencedAttrIds)
}

abstract class Term {
  def toString: String
  def toAliasString: String = toString
  def referencedAttrIds: Seq[ExprId]
}

case class Constant(value: Any) extends Term {
  override def toString: String = {
    value match {
      case i: Int => String.valueOf(i)
      case i: Long => String.valueOf(i)
      case d: Decimal => d.toString()
      case s: String if s == "_" => s
      case s: String => "\"" + s + "\""
      case _ => throw new UnsupportedOperationException(value.getClass.getSimpleName + " Type is not supported.")
    }
  }

  override def referencedAttrIds: Seq[ExprId] = Seq.empty
}

case class Variable(attr: AttributeReference) extends Term {
  val name: String = attr.name
  val exprId: Long = attr.exprId.id

  if (name == "") throw new IllegalArgumentException("Variable name cannot be empty.")
  if (!name.charAt(0).isUpper) throw new IllegalArgumentException(s"Variable name: $name should start in uppercase.")

  override def toString: String = s"$name$exprId" // attr.toString without `#`

  def withNewExprId(): Variable = {
    Variable(attr.withExprId(NamedExpression.newExprId))
  }

  override def referencedAttrIds: Seq[ExprId] = Seq(attr.exprId)

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case v: Variable => exprId == v.exprId
      case _ => false
    }
  }
}

case class Function(funcName: String, args: Seq[Term], alias: String) extends Term {
  override def toString: String = {
    args.length match {
      case 0 => throw new UnsupportedOperationException(s"args length: ${args.length}")
      case 1 => s"$funcName<${args.head}>"
      case _ => s"$funcName<(${args.mkString(",")})>"
    }
  }

  override def referencedAttrIds: Seq[ExprId] = args.flatMap(_.referencedAttrIds)

  override def toAliasString: String = alias
}
