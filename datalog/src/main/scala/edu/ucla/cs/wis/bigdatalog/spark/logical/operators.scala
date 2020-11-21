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

import javax.annotation.Nullable

import edu.ucla.cs.wis.bigdatalog.spark.logical.RecOption.RecOption

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan, Statistics, UnaryNode}

import scala.collection.mutable.ArrayBuffer

object RecOption extends Enumeration {
    type RecOption = Value
    val Mutual, Driver, NonLinear = Value
}

case class Recursion(name: String,
                     recOptions: Seq[RecOption],
                     @Nullable left: LogicalPlan,
                     right: LogicalPlan,
                     partitioning: Seq[Int]) extends BinaryNode {
  // left is exitRules plan, right is recursive rules plan
  override def children: Seq[LogicalPlan] = if (left == null) Seq(right) else Seq(left, right)

  override def output: Seq[Attribute] = right.output

  override def statistics: Statistics = Statistics(Long.MaxValue)
}

case class AggregateRecursion(name: String,
                              recOptions: Seq[RecOption],
                              @Nullable left: LogicalPlan,
                              right: LogicalPlan,
                              partitioning: Seq[Int]) extends BinaryNode {
  // left is exitRules plan, right is recursive rules plan
  override def children: Seq[LogicalPlan] = if (left == null) Seq(right) else Seq(left, right)

  override def output: Seq[Attribute] = right.output

  override def statistics: Statistics = Statistics(Long.MaxValue)
}

case class LinearRecursiveRelation(name: String, output: Seq[Attribute], partitioning: Seq[Int])
  extends LeafNode {
  override def statistics: Statistics = Statistics(Long.MaxValue)
}

case class NonLinearRecursiveRelation(name: String, output: Seq[Attribute], partitioning: Seq[Int])
  extends LeafNode {
  override def statistics: Statistics = Statistics(Long.MaxValue)
}

case class MonotonicAggregate(groupingExpressions: Seq[Expression],
                              aggregateExpressions: Seq[Expression],
                              child: LogicalPlan,
                              partitioning: Seq[Int]) extends UnaryNode {
  override lazy val resolved: Boolean = !expressions.exists(!_.resolved) && childrenResolved

  override def output: Seq[Attribute] = {
    val buf = ArrayBuffer[Attribute]()
    for (aggrExpr <- aggregateExpressions) {
      val attr = aggrExpr match {
        case ne: NamedExpression => ne.toAttribute
        case aggExpr: AggregateExpression => aggExpr.resultAttribute
        case other => throw new UnsupportedOperationException(other.toString)
      }
      buf += attr
    }
    buf.toSeq
  }
}

case class AggregateRelation(name: String, output: Seq[Attribute], partitioning: Seq[Int])
  extends LeafNode {
  override def statistics: Statistics = Statistics(Long.MaxValue)
}
