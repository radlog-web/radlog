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

package edu.ucla.cs.wis.bigdatalog.spark.deprecated

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

abstract class QuerySuite extends FunSuite with Logging {

  case class TestCase(
    program: String,
    query: String,
    data: Map[String, Seq[String]],
    answers: Seq[String],
    answersSize: Int) {
    def this(program: String, query: String, data: Map[String, Seq[String]], answersSize: Int) =
      this(program, query, data, null, answersSize)

    def this(program: String, query: String, data: Map[String, Seq[String]], answers: Seq[String]) =
      this(program, query, data, answers, answers.size)
  }

  def runTest(testCase: TestCase): Unit = runTests(Seq(testCase))

  def runTests(testCases: Seq[TestCase]): Unit = {
    val conf = new SparkConf()
      .set("spark.eventLog.enabled", "true")
      //.set("spark.eventLog.dir", "../logs")
      .set("spark.sql.shuffle.partitions", "5")
      .setAll(Map.empty[String, String])

    // TODO: dummy run, not doing any tests!
    val bigDatalogCtx = new SparkSession(new SparkContext("local[*]", "QuerySuite", conf))
    bigDatalogCtx.stop()

    logInfo("========== END ==========\n")
  }

}
