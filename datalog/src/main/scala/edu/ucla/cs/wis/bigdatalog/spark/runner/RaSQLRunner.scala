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

class RaSQLRunner extends Runner {
  private val dir = "query/rasql/"
  private val ext = ".sql"

  def run(args: Seq[String]): Array[Row] = {
    val argsMap = toMap(args)
    val program = RaSQLProgram(dir + argsMap("program") + ext).initArgs(argsMap)
    val options = SessionOptions.create(program, argsMap)

    val appName = AppName.build(program.name, program.relations, options)
    val session = BigDatalogSessionBuilder.build(appName, options)
    val sessionState = session.sessionState.asInstanceOf[BigDatalogSessionState]
    val out = sessionState.out
    val outputFile = options.get("output")

    val start = System.currentTimeMillis()
    val createdViews = new scala.collection.mutable.ArrayBuffer[String]()

    out.printTime("\nLoad", {
      program.relations.foreach(r => {
        val dataFilePath = options(r.name)
        sessionState.loadRelation(r.name, r.getSchema, dataFilePath)
      })
    })

    var compileTime = 0L
    var execTime = 0L

    val results = program.sqls.toArray.flatMap(sql => {
      // Hack: add createdViews to the databaseString, e.g. coalesce.sql
      val relations = program.relations.map(_.toString) ++ createdViews

      val compileStart = System.currentTimeMillis()
      val qe = sessionState.executeSql(sql, "database({" + relations.mkString(", ") + "}).", program.hints)
      qe.executedPlan
      compileTime += System.currentTimeMillis() - compileStart

      if (!qe.isCreateView) {
        out.logPlan(qe.toString)
      }

      val execStart = System.currentTimeMillis()
      val result = qe.executedPlan.executeCollectPublic() // qe.toRdd
      execTime += System.currentTimeMillis() - execStart

      if (qe.isCreateView) {
        val viewSchema = result(0).getString(0)
        createdViews += viewSchema
        out.println(s"[Success - View Created] $viewSchema")
        Array[Row]()
      } else {
        out.println(s"[Success - Execution Finished]")
        result
      }
    })

    out.printUsedTime("Compile RamSQL", compileTime)
    out.printUsedTime("Execution (Collect)", execTime)
    out.printUsedTime("Total", System.currentTimeMillis() - start)
    out.printResultSize(results.length)
    out.close()

    OutputFileWriter.write(outputFile, results)

    sessionState.reset()
    session.stop()

    results
  }
}

object RaSQLRunner {
  def main(args: Array[String]): Unit = {
    new RaSQLRunner().run(args)
  }
}
