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

import scala.collection.mutable.ArrayBuffer

case class TestSuite(name: String, ignored: Boolean = false) {
  val testCases = new ArrayBuffer[TestCase]()
}

/**
 * Reading the TestSuites from file, thus no need to recompile when testcase changed.
 * Prefer this class than the ScalaTest as the latter does not output logging messages.
 */
class TestBase(runner: Runner, testSuitesPath: String) extends TestUtil {

  private val lines = scala.io.Source.fromFile(testSuitesPath).getLines()
  protected val suites = new ArrayBuffer[TestSuite]()

  // tiny compiler to parse the TestSuite file
  for (line <- lines) {
    val l = line.trim
    if (l.startsWith("==")) {
      suites += TestSuite(l)
    } else if (l.startsWith("//") && l.contains("==")) {
      suites += TestSuite(l.substring(2).trim, ignored = true)
    } else if (l.startsWith("//")) {
      // comment
    } else if (l == "") {
      // empty line
    } else {
      // treat as testcase
      try {
        val arr = l.split(":")
        val name = arr(0).trim
        val args = arr(1).trim.split(" ")
        suites.last.testCases += ET(name, args)
      } catch {
        case e: Exception =>
          throw new RuntimeException(s"Error parsing testcase: $l", e)
      }
    }
  }

  def run(): Unit = {
    suites.foreach(s => {
      println(s"[TestSuite] ${s.name}")
      if (s.ignored) {
        println("Ignored ...\n")
      } else {
        println("Testing ...\n")
        run(runner, s.testCases)
      }
    })
  }

}
