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

import edu.ucla.cs.wis.bigdatalog.spark.ramsql.structs._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical._


abstract class RamSQLQueryExecution(compiler: RamSQLCompiler, logical: LogicalPlan) extends Logging {

  private lazy val unresolved: LogicalPlan = {
    logInfo("** Logical Plan w/ RecRelation resolved **\n" + logical)
    logical
  }

  private lazy val analyzed: LogicalPlan = {
    val analyzed = compiler.ramSQLAnalyzer.execute(unresolved)
    logDebug("RamSQLAnalyzer processed logical plan:\n" + analyzed)
    compiler.ramSQLAnalyzer.checkAnalysis(analyzed)
    logInfo("** Analyzed Logical Plan **\n" + analyzed)
    analyzed
  }

  private lazy val optimized: LogicalPlan = {
    val optimized = compiler.ramSQLOptimizer.execute(analyzed)
    logInfo("** Optimized Logical Plan **\n" + optimized)
    optimized
  }

  def getOptimized: LogicalPlan = {
    compiler.sessionState.out.logTime("\nRamSQL Analyze", analyzed)
    compiler.sessionState.out.logTime("RamSQL Optimize", optimized)
  }

}

case class SchemaPlan(recRelation: RecursiveRelation, logical: LogicalPlan)

class SchemaPlanQueryExecution(compiler: RamSQLCompiler, schemaPlan: SchemaPlan)
  extends RamSQLQueryExecution(compiler, schemaPlan.logical) {

  lazy val rules: Seq[DatalogRule] = {
    def visit(plan: LogicalPlan): Seq[PlanInfo] = {
      plan match {
        // the top-level operator is Aggregate because the default behavior of union is `union distinct`
        case a @ Aggregate(_, _, u @ Union(uChildren)) =>
          if (!uChildren.forall(_.isInstanceOf[Project])) {
            throw new RuntimeException("Union children should all be Project Operators")
          }
          uChildren.flatMap(visit)
        case p: Project =>
          Seq(new PlanInfoExtractor().topDownVisits(p))
        case e =>
          throw new RuntimeException("Plan Root is not Union or Project: " + e)
      }
    }

    // SchemaPlan may generate more than one rule, e.g. Exit Rule, Rec Rule 1, Rec Rule 2, ...
    val ruleGenerator = new RecursiveCaseRuleGenerator(schemaPlan.recRelation)
    visit(getOptimized).map(ruleGenerator.toDatalogRule)
  }

}

class OutermostPlanQueryExecution(compiler: RamSQLCompiler, logical: LogicalPlan)
  extends RamSQLQueryExecution(compiler, logical) {
  val predicateName = "result"

  lazy val rule: DatalogRule = {
    val planInfo = getOptimized match {
      case p: Project => new PlanInfoExtractor().topDownVisits(p)
      case a: Aggregate => new PlanInfoExtractor().topDownVisits(a)
      case e => throw new RuntimeException("OutermostPlan root is not Project or Aggregate:\n" + e)
    }
    val ruleGenerator = new DefaultRuleGenerator(predicateName)
    ruleGenerator.toDatalogRule(planInfo)
  }
}
