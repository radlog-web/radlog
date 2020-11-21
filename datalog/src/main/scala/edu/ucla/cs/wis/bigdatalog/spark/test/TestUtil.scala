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

package edu.ucla.cs.wis.bigdatalog.spark.test

import edu.ucla.cs.wis.bigdatalog.spark.runner.Runner

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class TestFailedException(msg: String) extends RuntimeException(msg)

trait TestUtil extends Logging {
  // If multiple values need to be tested for an argument, they are separated by |. Orders matter!
  private val testArgsFile = "query/test_args.txt"
  private val args = Source.fromFile(testArgsFile).getLines().toArray

  /**
   * For each argument combination, register all testCases.
   * This method should be called by the primary constructor of the Test class.
   */
  protected def run(runner: Runner, testCases: Seq[TestCase]): Unit = {
    val combs = argCombinations(args)
    for (commonArgs <- combs) {
      for (t <- testCases) {
        runTestCase(runner, commonArgs, t)
      }
    }
  }

  private def runTestCase(runner: Runner, commonArgs: Seq[String], t: TestCase): Unit = {
    val args = Seq(s"-program=${t.name}") ++ t.args ++ commonArgs
    val name = runner.getClass.getSimpleName + " " + args.mkString(" ")
    test(name) {
      val results = runner.run(args)
      checkResults(t, results)
    }
  }

  private def checkResults(testCase: TestCase, results: Array[Row]): Unit = {
    val actual = results.sortBy(_.toString()).map(_.mkString(", "))
    println("[Sorted Result]")
    actual.foreach(println)
    println(s"Compare results with answer file: ${testCase.resultFilePath} ...")
    val expected = Source.fromFile(testCase.resultFilePath).getLines().toArray
    compare(expected, actual)
    println("[Success] results are correct")
  }

  private def compare(expected: Array[String], actual: Array[String]): Unit = {
    if (expected.length != actual.length) {
      throw new TestFailedException(
        s"Result Size Mismatch - expected: ${expected.length} actual: ${actual.length}")
    }
    expected.zip(actual).foreach(e => {
      if (e._1 != e._2) {
        throw new TestFailedException(s"Expected: ${e._1}, Actual: ${e._2}")
      }
    })
  }

  // tailored for test_args.txt format
  private def argCombinations(args: Seq[String]): Seq[Seq[String]] = {
    val combs = new ArrayBuffer[Seq[String]]()
    def genCombs(args: Seq[String], formed: Seq[String]): Unit = {
      if (args.isEmpty) {
        combs += formed
      } else {
        val arr = args.head.split("=")
        val key = arr(0)
        val vals = arr(1)
        val values = vals.split("\\|")
        for (v <- values) {
          genCombs(args.tail, formed ++ Seq(s"$key=$v"))
        }
      }
    }
    genCombs(args, Seq())
    combs
  }

  // taken from FunSuiteLike.test
  private def test(testName: String)(testFun: => Unit): Unit = {
    println(s"[TestCase] $testName")
    testFun
  }
}
