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

package edu.ucla.cs.wis.bigdatalog.spark.library.graph

import edu.ucla.cs.wis.bigdatalog.spark.runner.RaDlogRunner

import scala.collection.mutable

class CountPath {
  var startVertex = 1
  val configsMap = new mutable.HashMap[String, String]()

  def initlizeConfig(): Unit = {
    configsMap.put("master", "local[*]")
    configsMap.put("partitions", "16")
    configsMap.put("program", "count_paths")
    configsMap.put("codegen", "true")
    configsMap.put("fixpointTask", "false")
    configsMap.put("startvertex", startVertex.toString)
  }

  def setStartVertex(_sv: Int): Unit = {
    startVertex = _sv
    configsMap.put("startvertex", startVertex.toString)
  }

  def setMaster(_master: String): Unit = {
    configsMap.put("master", _master)
  }

  def setNumPartitions(num_part: Int): Unit = {
    configsMap.put("partitions", num_part.toString)
  }

  def run(rcPath: String): Unit = {
    configsMap.put("rc", rcPath)
    val results = new RaDlogRunner().run(configsMap.toMap[String, String])
    println(results.size)
  }
}

object CountPath {
  def main(args: Array[String]): Unit = {
    val lc = new CountPath()
    lc.initlizeConfig()
    val rcPath = "testdata/rc1.csv"
    lc.run(rcPath)
  }
}


