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
import org.apache.spark.sql.catalyst.expressions.{Literal => ExprLiteral, _}
import org.apache.spark.sql.types.ByteType

import scala.collection.mutable

trait AttributeReplacer {
  def replaceAttr(v: Variable): Term

  private final def replaceAttrInTerms(terms: Seq[Term]): Seq[Term] = {
    terms.map {
      case v: Variable => replaceAttr(v)
      case Function(name, args, alias) => Function(name, replaceAttrInTerms(args), alias)
      case other => other
    }
  }

  final def replaceAttrsInHead(literal: HeadLiteral): HeadLiteral = {
    HeadLiteral(literal.predicate, replaceAttrInTerms(literal.terms))
  }

  final def replaceAttrsInGoal(literal: GoalLiteral): GoalLiteral = {
    GoalLiteral(literal.predicate, replaceAttrInTerms(literal.terms))
  }
}

/**
 * Remove AttributeEqualTo conditions by replacing equaled attributes with shared variables
 * E.g. `tc(A, B), arc(C, D), B = C` will be turned into `tc(A, G), arc(G, D)`
 */
class AttributeToAttributeReplacer(conds: Seq[AttributeEqualTo]) extends AttributeReplacer with Logging {
  private def newGroupAttr = AttributeReference("G", ByteType)()

  // we use ExprId as key because same AttributeReference may have different underlying objects
  val groupMap: Map[ExprId, AttributeReference] = {
    var nextGroupAttr = newGroupAttr
    val groupMap = mutable.HashMap[ExprId, AttributeReference]()

    conds.foreach(c => {
      val l = c.left.exprId
      val r = c.right.exprId
      if (!groupMap.contains(l) && !groupMap.contains(r)) {
        groupMap += l -> nextGroupAttr
        groupMap += r -> nextGroupAttr
        nextGroupAttr = newGroupAttr
      } else if (!groupMap.contains(l)) {
        groupMap += l -> groupMap(r)
      } else if (!groupMap.contains(r)) {
        groupMap += r -> groupMap(l)
      } else {
        // merge group
        val lg = groupMap(l)
        val rg = groupMap(r)
        for ((k, v) <- groupMap if v == lg) {
          groupMap(k) = rg
        }
      }
    })

    groupMap.toMap
  }

  override def replaceAttr(v: Variable): Variable = {
    if (groupMap.contains(v.attr.exprId)) {
      val newAttr = groupMap(v.attr.exprId)
      logDebug(s"Replace '${v.attr}' by '$newAttr'")
      Variable(newAttr)
    } else {
      v
    }
  }

  def replaceAttrsInCondition(cond: Expression): Expression = {
    cond transform {
      case attr: AttributeReference if groupMap.contains(attr.exprId) => groupMap(attr.exprId)
    }
  }
}

/**
 * Remove AttributeEqualToLiteral conditions by replacing attribute with literal
 * Currently only AttributeEqualToLiteral condition having unreferenced attribute w.r.t. other goals
 * needs to be eliminated in this way
 * E.g. `paths(X, Y, count(X, B)) <- rc(X, Y), B = 1` will be turned into `paths(X, Y, count(X, 1)) <- rc(X, Y)`
 */
class AttributeToLiteralReplacer(conds: Seq[AttributeEqualToLiteral]) extends AttributeReplacer with Logging {
  // we use ExprId as key because same AttributeReference may have different underlying objects
  val groupMap: Map[ExprId, ExprLiteral] = conds.map(c => c.left.exprId -> c.right).toMap

  override def replaceAttr(v: Variable): Term = {
    if (groupMap.contains(v.attr.exprId)) {
      val literal = groupMap(v.attr.exprId)
      logDebug(s"Replace '${v.attr}' by literal '$literal'")
      Constant(literal.value)
    } else {
      v
    }
  }
}

class AttributeToUnderscoreReplacer(attrIdsToReplace: Set[ExprId]) extends AttributeReplacer with Logging {
  override def replaceAttr(v: Variable): Term = {
    if (attrIdsToReplace.contains(v.attr.exprId)) {
      Constant("_")
    } else {
      v
    }
  }
}
