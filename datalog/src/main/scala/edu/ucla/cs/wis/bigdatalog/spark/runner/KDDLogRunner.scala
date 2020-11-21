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

package edu.ucla.cs.wis.bigdatalog.spark.runner

import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogSessionState

import org.apache.spark.sql.Row

class KDDLogRunner {
  private val dir = "query/datalog/"
  private val ext = ".txt"

  def run(argsMap: Map[String, String]): Array[Row] = {
    val program = DatalogProgram(dir + argsMap("program") + ext).initArgs(argsMap)
    val options = SessionOptions.create(program, argsMap)
    val appName = AppName.build(program.name, program.relations, options)
    val session = BigDatalogSessionBuilder.build(appName, options)
    val sessionState = session.sessionState.asInstanceOf[BigDatalogSessionState]
    val out = sessionState.out

    val start = System.currentTimeMillis()

    out.printTime("\nLoad", {
      program.relations.foreach(r => {
        val dataFilePath = options(r.name)
        sessionState.loadRelation(r.name, r.getSchema, dataFilePath)
      })
    })

    val qe = out.printTime("Compile Datalog", {
      val logicalPlan = sessionState.compileDatalogToSparkPlan(
        program.toDatabaseString, program.rules.map(_.toString.replace("mavg", "mmin")).mkString("\n"), program.query, program.hints)
      val _qe = sessionState.executePlan(logicalPlan)
      _qe.executedPlan
      out.logPlan(_qe.toString)
      _qe
    })

    val results = out.printTime("Execution (Collect)", {
        qe.executedPlan.executeCollectPublic()
      })

    out.printUsedTime("Total", System.currentTimeMillis() - start)
    out.printResultSize(results.length)
    out.close()

    sessionState.reset()
    session.stop()

    results
  }
}
