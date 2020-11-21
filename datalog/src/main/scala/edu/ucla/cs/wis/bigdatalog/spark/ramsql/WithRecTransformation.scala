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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, WithRec}

case class WithRecQueryPlans(schemaPlans: Seq[SchemaPlan], outermostPlan: LogicalPlan)

object WithRecTransformer extends Logging {

  def transform(withRec: WithRec): WithRecQueryPlans = {
    val relationMap = buildRecRelations(withRec)
    resolveRecRelationInViewDefinitions(withRec, relationMap)
  }

  private def buildRecRelations(withRec: WithRec): Map[String, RecursiveRelation] = {
    withRec.viewDefs.map {
      case (name, (unresolvedColumnExprs, _)) =>
        val recRelation = RecursiveRelation(name, unresolvedColumnExprs)
        name -> recRelation
    }
  }

  // TODO: verify correct handling of cross-refs, i.e. mutual recursion in different rec view defs
  private def resolveRecRelationInViewDefinitions(withRec: WithRec, relationMap: Map[String, RecursiveRelation]) = {
    val schemaPlans = withRec.viewDefs.map {
      case (name, (_, plan)) =>
        val logical = tryResolveRecRelation(plan, relationMap)
        SchemaPlan(relationMap(name), logical)
    }.toSeq

    WithRecQueryPlans(schemaPlans, tryResolveRecRelation(withRec.child, relationMap))
  }

  private def tryResolveRecRelation(plan: LogicalPlan, relationMap: Map[String, RecursiveRelation]): LogicalPlan = {
    plan transform {
      case u: UnresolvedRelation =>
        val tableName = u.tableIdentifier.table // TODO: case-insensitive version, check Catalog.lookupRelation
        if (relationMap.contains(tableName)) {
          val table = relationMap(tableName)
          val tableWithQualifiers = SubqueryAlias(tableName, table)
          // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
          // properly qualified with this alias.
          u.alias.map(a => SubqueryAlias(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
        } else {
          log.debug("UnresolvedRelation not found in relationMap: " + u.toString.trim)
          u
        }
    }
  }

}
