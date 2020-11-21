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

package edu.ucla.cs.wis.bigdatalog.spark.ramsql

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.{InternalRow, analysis}
import org.apache.spark.sql.types.ByteType

// we need to keep unresolvedColumnExprs in this wrapper
// because Spark uses productIterator to check plan/expression resolution
class ColumnExprs(val unresolved: Seq[Expression]) {
  override def toString: String = unresolved.toString()
}

object RecursiveRelation {
  def apply(name: String, unresolvedExprs: Seq[Expression]): RecursiveRelation = {
    // we require all columns as named expression, i.e. attribute or aliased
    // thus even an aggregate function is used, the body query can refer to it by name
    unresolvedExprs map {
      case ne: NamedExpression => ne
      case _ => throw new IllegalArgumentException("Each RecSchema column should be a named expression.")
    }
    // [Hack] we do not know the exact dataType of attribute in RecursiveRelation thus we use ByteType as dummy type.
    // It is fine here because the analyzed plan is used to transform to datalog instead of execution.
    val output = unresolvedExprs map {
      case a: Alias => AttributeReference(a.name, ByteType)()
      case u: UnresolvedAttribute => AttributeReference(u.name, ByteType)()
      case _ => throw new RuntimeException("Invalid attrExprs in view def: " + unresolvedExprs)
    }
    RecursiveRelation(name, output, new ColumnExprs(unresolvedExprs))
  }
}

case class RecursiveRelation(
    name: String,
    output: Seq[Attribute],
    columnExprs: ColumnExprs,
    data: Seq[InternalRow] = Nil)
  extends LeafNode with analysis.MultiInstanceRelation {

  /**
    * Returns an identical copy of this relation with new exprIds for all attributes.  Different
    * attributes are required when a relation is going to be included multiple times in the same
    * query.
    */
  override final def newInstance(): this.type = {
    RecursiveRelation(name, output.map(_.newInstance()), columnExprs, data).asInstanceOf[this.type]
  }

  override protected def stringArgs = Iterator(output)

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case RecursiveRelation(otherName, otherOutput, otherColumnExprs, otherData) =>
      otherName == name &&
      otherOutput.map(_.dataType) == output.map(_.dataType) &&
      otherColumnExprs == columnExprs &&
      otherData == data
    case _ => false
  }

  override lazy val statistics =
    Statistics(sizeInBytes = output.map(_.dataType.defaultSize).sum * data.length)
}
