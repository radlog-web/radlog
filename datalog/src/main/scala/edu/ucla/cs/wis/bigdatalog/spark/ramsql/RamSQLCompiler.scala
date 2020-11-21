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

import edu.ucla.cs.wis.bigdatalog.interpreter.Hints
import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogSessionState
import edu.ucla.cs.wis.bigdatalog.spark.ramsql.structs._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.datalog.CreateRamSQLViewCommand

/**
  * This class invokes the RamSQL analyzer and optimizer without starting the standalone Spark
  */
class RamSQLCompiler(val sessionState: BigDatalogSessionState) extends Logging {

  lazy val ramSQLAnalyzer: RamSQLAnalyzer = new RamSQLAnalyzer(sessionState.catalog, sessionState.conf)

  lazy val ramSQLOptimizer: Optimizer = new RamSQLDefaultOptimizer(sessionState.catalog, sessionState.conf)

  def compile(sql: String, databaseString: String, hints: Hints): LogicalPlan = {
    val parsed = sessionState.out.logTime("\nRamSQL Parse", sessionState.sqlParser.parsePlan(sql))
    sessionState.out.logInputSQL(sql.trim)
    parsed match {
      case cv @ CreateRamSQLViewCommand(t, c, a, r, i) =>
        c match {
          case w: WithRec => CreateRamSQLViewCommand(t, toDatalogThenToSparkPlan(w, databaseString, hints), a, r, i)
          case _ => cv
        }
      case w: WithRec => toDatalogThenToSparkPlan(w, databaseString, hints)
      case other => other
    }
  }

  private def toDatalogThenToSparkPlan(parsed: WithRec, databaseString: String, hints: Hints): LogicalPlan = {
    if (databaseString == null) {
      throw new IllegalArgumentException("WithRec query cannot be compiled without databaseString")
    }
    val datalog = compileToDatalog(parsed)
    sessionState.compileDatalogToSparkPlan(
      databaseString,
      datalog.rules.map(_.toString).mkString("\n"),
      datalog.query.toString,
      hints
    )
  }

  private def compileToDatalog(withRec: WithRec): DatalogProgram = {
    val withRecPlans = WithRecTransformer.transform(withRec)

    val schemaPlanRules = withRecPlans.schemaPlans.flatMap(plan => {
      val qe = new SchemaPlanQueryExecution(this, plan)
      sessionState.out.logTime(s"RamSQL Generate rules of `${plan.recRelation.name}`", qe.rules)
    })

    val outermostPlanRule = {
      val qe = new OutermostPlanQueryExecution(this, withRecPlans.outermostPlan)
      sessionState.out.logTime(s"RamSQL Generate rules of outermost query", qe.rule)
    }

    val rules = schemaPlanRules ++ Seq(outermostPlanRule)
    val query = DatalogQuery(outermostPlanRule.head)
    DatalogProgram(rules, query)
  }

}
