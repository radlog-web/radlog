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

package edu.ucla.cs.wis.bigdatalog.spark.kddlog

import edu.ucla.cs.wis.bigdatalog.spark.runner.KDDLogRunner

import scala.collection.mutable

class LloydClustering {
  var maxIter = 10
  var numClusters = 4
  val configsMap = new mutable.HashMap[String, String]()

  def initlizeConfig(): Unit = {
    configsMap.put("master", "local[*]")
    configsMap.put("partitions", "16")
    configsMap.put("program", "Lloyd")
    configsMap.put("codegen", "true")
    configsMap.put("fixpointTask", "false")
  }

  def setMaxIter(_maxIter: Int): Unit = {
    maxIter = _maxIter
    configsMap.put("program", "Lloyd-withJ")
  }

  def setNumClusters(_numClusters: Int): Unit = {
    numClusters = _numClusters
  }

  def setMaster(_master: String): Unit = {
    configsMap.put("master", _master)
  }

  def setNumPartitions(num_part: Int): Unit = {
    configsMap.put("partitions", num_part.toString)
  }

  def train(initPath: String, pointPath: String): Unit = {
    configsMap.put("init", initPath)
    configsMap.put("point", pointPath)
    val results = new KDDLogRunner().run(configsMap.toMap[String, String])
    println(results.size)
  }
}

object LloydClustering {
  def main(args: Array[String]): Unit = {
    val lc = new LloydClustering()
    lc.initlizeConfig()
    val initPath = "testdata/cluster/init.csv"
    val pointPath = "testdata/cluster/point.csv"
    lc.train(initPath, pointPath)
  }
}
