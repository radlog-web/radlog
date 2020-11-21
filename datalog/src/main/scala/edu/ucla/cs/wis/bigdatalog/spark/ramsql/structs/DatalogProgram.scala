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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeEqualTo, AttributeEqualToLiteral, AttributeReference, Expression}

case class DatalogProgram(rules: Seq[DatalogRule], query: DatalogQuery) {
  override def toString: String = {
    val builder = StringBuilder.newBuilder
    builder.append("Rules:\n")
    rules.map(_ + "\n").foreach(builder.append)
    builder.append("\nQuery:\n")
    builder.append(query + "\n")
    builder.toString
  }
}

case class DatalogRule(
  head: HeadLiteral,
  goals: Seq[GoalLiteral],
  conds: Seq[Expression]) extends Logging {

  def removeAttributeEqualToConds(): DatalogRule = {
    val (mergeableConds, otherConds) = conds.partition(_.isInstanceOf[AttributeEqualTo])
    logDebug(s"Mergeable Conds: $mergeableConds")
    logDebug(s"Other Conds: $otherConds")
    val replacer = new AttributeToAttributeReplacer(mergeableConds.map(_.asInstanceOf[AttributeEqualTo]))

    val h = replacer.replaceAttrsInHead(head)
    val g = goals.map(replacer.replaceAttrsInGoal)
    val c = otherConds.map(replacer.replaceAttrsInCondition)

    val res = DatalogRule(h, g, c)
    logDebug(s"Before Conds Merge: $toString")
    logDebug(s"After Conds Merge: ${res.toString}")
    res
  }

  def replaceAttributeByLiteralInHead(): DatalogRule = {
    if (goals.isEmpty) {
      // no need to replace if we have empty goals, e.g. SSSP program
      return this
    }
    val goalReferencedAttrIds = goals.flatMap(_.referencedAttrIds).toSet
    val (mergeableConds, otherConds) = conds.partition {
      case e: AttributeEqualToLiteral if !goalReferencedAttrIds.contains(e.left.exprId) => true
      case _ => false
    }
    logDebug(s"Mergeable Literal Conds: $mergeableConds")
    logDebug(s"Other Conds: $otherConds")
    val replacer = new AttributeToLiteralReplacer(mergeableConds.map(_.asInstanceOf[AttributeEqualToLiteral]))

    val h = replacer.replaceAttrsInHead(head)
    val g = goals // no need to replace goals as the attribute in condition is not referenced in goals
    val c = otherConds

    val res = DatalogRule(h, g, c)
    logDebug(s"Before Literal Conds Merge: $toString")
    logDebug(s"After Literal Conds Merge: ${res.toString}")
    res
  }

  def replaceAttributeByUnderscoreInGoals(): DatalogRule = {
    // we replace variable name by underscore iff
    // 1) not referenced in head literal or conditions
    // 2) appear only once in all goal literals
    val referencedAttrIdsInHeadAndConds = (head.referencedAttrIds ++ conds.flatMap(_.collect {
      case a: AttributeReference => a.exprId // tree node collect
    })).toSet
    val referencedOnceAttrIdsInGoals = goals.flatMap(_.referencedAttrIds).groupBy(identity).filter(_._2.length == 1).keySet
    val attrIdsToReplace = referencedOnceAttrIdsInGoals.diff(referencedAttrIdsInHeadAndConds)
    logDebug(s"Attrs to replace by underscore: $attrIdsToReplace")
    val replacer = new AttributeToUnderscoreReplacer(attrIdsToReplace)

    val h = head
    val g = goals.map(replacer.replaceAttrsInGoal)
    val c = conds

    val res = DatalogRule(h, g, c)
    logDebug(s"Before Underscore Replace: $toString")
    logDebug(s"After Underscore Replace: ${res.toString}")
    res
  }

  override def toString: String = {
    val conditions: Seq[String] = conds.map(_.toString.replace("#", "")).map {
      case s if s.startsWith("(") && s.endsWith(")") => s.substring(1, s.length-1)
      case other => other
    }

    s"$head <- ${(goals ++ conditions).mkString(", ")}."
  }
}

case class DatalogQuery(queryLiteral: HeadLiteral, removeSharpSymbol: Boolean = true) {
  override def toString: String = {
    val ret = s"${queryLiteral.toAliasString}."
    if (removeSharpSymbol) ret.replace("#", "") else ret
  }
}
