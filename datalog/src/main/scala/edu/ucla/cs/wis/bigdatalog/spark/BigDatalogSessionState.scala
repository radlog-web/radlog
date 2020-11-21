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

package edu.ucla.cs.wis.bigdatalog.spark

import java.util.regex.{Matcher, Pattern}

import scala.collection.mutable
import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm
import edu.ucla.cs.wis.bigdatalog.interpreter.{Hints, OperatorProgram}
import edu.ucla.cs.wis.bigdatalog.spark.execution.BigDatalogQueryExecution
import edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates._
import edu.ucla.cs.wis.bigdatalog.spark.logical.LogicalPlanGenerator
import edu.ucla.cs.wis.bigdatalog.spark.logical.patch._
import edu.ucla.cs.wis.bigdatalog.spark.ramsql.RamSQLCompiler
import edu.ucla.cs.wis.bigdatalog.system.{DeALSContext, ReturnStatus}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.StructType


/**
 * Holds most RaSQL related session-specific state in a given [[SparkSession]].
 * This class will be used if "spark.sql.sessionState" is set to `rasql`.
 */
class BigDatalogSessionState(val sparkSession: SparkSession)
  extends SessionState(sparkSession) with Logging {

  lazy val out = new InfoLogger(sparkSession.sparkContext.getConf.get("spark.app.name"))

  private val recursiveRDDs = mutable.HashMap.empty[String, RDD[InternalRow]]

  private val mmin = FunctionRegistry.expression[MMin]("mmin")
  functionRegistry.registerFunction("mmin", mmin._2._1, mmin._2._2)
  private val mmax = FunctionRegistry.expression[MMax]("mmax")
  functionRegistry.registerFunction("mmax", mmax._2._1, mmax._2._2)
  private val sample = FunctionRegistry.expression[Sample]("sample")
  functionRegistry.registerFunction("sample", sample._2._1, sample._2._2)

  private val mcount = FunctionRegistry.expression[MCount]("mcount")
  functionRegistry.registerFunction("mcount", mcount._2._1, mcount._2._2)
  private val msum = FunctionRegistry.expression[MSum]("msum")
  functionRegistry.registerFunction("msum", msum._2._1, msum._2._2)

  private val cmax = FunctionRegistry.expression[CMax]("cmax")
  functionRegistry.registerFunction("cmax", cmax._2._1, cmax._2._2)
  private val cmin = FunctionRegistry.expression[CMin]("cmin")
  functionRegistry.registerFunction("cmin", cmin._2._1, cmin._2._2)
  private val mavg = FunctionRegistry.expression[MAvg]("mavg")
  functionRegistry.registerFunction("mavg", mavg._2._1, mavg._2._2)

  @transient private val deALSContext = new DeALSContext()
  deALSContext.initialize()

  override val planner: BigDatalogPlanner =
    new BigDatalogPlanner(sparkSession, conf, experimentalMethods.extraStrategies)

  override lazy val analyzer: BigDatalogAnalyzer =
    new BigDatalogAnalyzer(catalog, conf, maxIterations = 100) {}

  // ====== RamSQL Specific ======

  private val RamSQLCompiler = new RamSQLCompiler(this)

  override def executeSql(sql: String): QueryExecution = executeSql(sql, null, null)

  override def executePlan(plan: LogicalPlan): BigDatalogQueryExecution = new BigDatalogQueryExecution(sparkSession, plan)

  /**
   * WithRec query can only be compiled using this API, as it needs to be transformed into Datalog first.
   *
   * @param sql a RaSQL query, can be a WithRec query or a normal SQL
   * @param databaseString EDB relations and views if query is WithRec, null otherwise
   * @param hints joinType if query is WithRec, null otherwise
   * @return [[BigDatalogQueryExecution]]
   */
  def executeSql(sql: String, databaseString: String, hints: Hints): BigDatalogQueryExecution = {
    val plan = RamSQLCompiler.compile(sql, databaseString, hints)
    executePlan(plan)
  }

  // =============================

  def loadRelation(name: String, schema: StructType, dataFilePath: String): Unit = {
    val rowRDD = FileUtils.loadRowRDDFromFile(sparkSession, dataFilePath, schema, conf.numShufflePartitions)
    val df = sparkSession.internalCreateDataFrame(rowRDD, schema)
    df.createOrReplaceTempView(name)
  }

  def loadDataSource(name: String, schema: StructType, dataFilePath: String): Unit = {
    val df = FileUtils.readDataSource(sparkSession, dataFilePath, schema)
    df.createOrReplaceTempView(name)
  }

  def compileDatalogToSparkPlan(databaseString: String, rules: String, queryText: String, hints: Hints): LogicalPlan = {
    val objectText = databaseString + "\n" + rules + "\n"
    out.logObjectText(objectText)
    out.logQuery(queryText)

    val scr = deALSContext.loadDatabaseObjects(objectText)
    if (scr.getStatus != ReturnStatus.SUCCESS) {
      throw new RuntimeException("loadProgram failed: " + scr.getMessage)
    }

    // compiled plan is an unresolved plan, which needs to be further analyzed
    val unresolvedPlan = compileDatalogQuery(queryText, hints)
    unresolvedPlan
  }

  private def compileDatalogQuery(queryText: String, hints: Hints): LogicalPlan = {

    val pcg = deALSContext.compileQueryToPCGTree(queryText)
    if (pcg != null) {
      out.logPCGTree(pcg.toStringTree)
    }

    val scr = deALSContext.compileQueryToOperators(queryText)
    if (scr.getStatus != ReturnStatus.SUCCESS) {
      throw new RuntimeException("compileQueryToOperators failed: " + scr.getMessage)
    }

    val opProgram = scr.getObject.asInstanceOf[QueryForm].getProgram.asInstanceOf[OperatorProgram]
    if (opProgram == null || opProgram.getRoot == null) {
      throw new NullPointerException("Operator is null.")
    }

    val opRoot = opProgram.getRoot
    FixUnresolvableFilter.fix(opRoot)
    FixMutualRecursiveClique.fix(opRoot)
    FixSampleVariable.fix(opRoot)
    FixChainVariable.fix(opRoot)
    opProgram.setHints(hints)

    out.logOperator(opProgram.toString)
    val planGenerator = new LogicalPlanGenerator(opProgram, sparkSession)
    val plan = planGenerator.getPlan(opRoot)

    // logInfo("** SparkPlan generated from Operator Program **\n" + plan)
    plan
  }

  def setRecursiveRDD(name: String, rdd: RDD[InternalRow]): Unit = {
    recursiveRDDs.put(name, rdd)
  }

  def getRecursiveRDD(name: String): RDD[InternalRow] = {
    // Suprisingly, We need not to wait one seconds in the cases
//    if (recursiveRDDs.get(name) == None) {
//      Thread.sleep(1000)
//    }

    // Youfu Li modified here to support the resolve of renamed relation.
    // SessionCatalog also has the same modification
    val p: Pattern = Pattern.compile("\\d+$")
    val m: Matcher = p.matcher(name)
    if (m.find()) {
      recursiveRDDs.get(name.replace(m.group(), "")) match {
        case Some(rdd) => rdd
        case _ => throw new RuntimeException(s"RDD for recursive relation: $name is not found. " +
          s"Probably the plan uses it is executed before the relation is computed.")
      }
    } else {
      recursiveRDDs.get(name) match {
        case Some(rdd) => rdd
        case _ => throw new RuntimeException(s"RDD for recursive relation: $name is not found. " +
          s"Probably the plan uses it is executed before the relation is computed.")
      }
    }
  }

  def reset(): Unit = {
    recursiveRDDs.clear()
    deALSContext.reset()
    deALSContext.initialize()
  }

}
