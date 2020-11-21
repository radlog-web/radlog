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

import edu.ucla.cs.wis.bigdatalog.spark.ramsql.structs.GoalLiteral
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class PlanInfo(
    val rootOp: LogicalPlan, // Aggregate or Project
    val goals: List[GoalLiteral],
    val projectAliasExprs: List[Expression],
    val filterConditions: List[Expression],
    val joinConditions: List[Expression]) {
  val conds: Seq[Expression] = joinConditions ++ filterConditions ++ projectAliasExprs
}

object PlanInfo {
  def apply(
    rootOp: LogicalPlan, // Aggregate or Project
    goals: List[GoalLiteral],
    projectAliasExprs: List[Expression],
    filterConditions: List[Expression],
    joinConditions: List[Expression]
  ): PlanInfo = {
    new PlanInfo(rootOp, goals, flatten(projectAliasExprs), flatten(filterConditions), flatten(joinConditions))
  }

  private def flatten(conds: List[Expression]): List[Expression] = {
    // we only allow `And` relationships between conditions
    conds match {
      case h @ And(l, r) +: t =>
        flatten(l +: (r +: t))
      case (h @ Not(EqualTo(l, r))) +: t =>
        NotEqualTo(l, r) +: flatten(t)
      case (h @ EqualTo(l: AttributeReference, r: AttributeReference)) +: t =>
        AttributeEqualTo(l, r) +: flatten(t)
      case (h @ EqualTo(l: AttributeReference, r: Literal)) +: t =>
        AttributeEqualToLiteral(l, r) +: flatten(t)
      case (h @ EqualTo(l: Literal, r: AttributeReference)) +: t =>
        AttributeEqualToLiteral(r, l) +: flatten(t)
      case (h : EqualTo) +: t =>
        h +: flatten(t)
      case (h : GreaterThan) +: t =>
        h +: flatten(t)
      case (h : GreaterThanOrEqual) +: t =>
        h +: flatten(t)
      case (h : LessThan) +: t =>
        h +: flatten(t)
      case (h : LessThanOrEqual) +: t =>
        h +: flatten(t)
      case h +: t =>
        throw new RuntimeException(s"$h is not allowed to use in RamSQL conditions")
      case Nil => Nil
    }
  }
}
