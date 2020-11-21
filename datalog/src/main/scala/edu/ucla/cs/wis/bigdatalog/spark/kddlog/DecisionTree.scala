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

class DecisionTree {
  var treeDepth = 5
  val configsMap = new mutable.HashMap[String, String]()
  var impurity = "Gini"
  def initlizeConfig(): Unit = {
    configsMap.put("master", "local[*]")
    configsMap.put("aggrType", "new")
    configsMap.put("partitions", "16")
    configsMap.put("program", "decision-tree")
    configsMap.put("codegen", "true")
    configsMap.put("fixpointTask", "false")
  }

  def setMaster(_master: String): Unit = {
    configsMap.put("master", _master)
  }

  def setTreeDepth(_treeDepth: Int): Unit = {
    treeDepth = _treeDepth
    configsMap.put("TD", treeDepth.toString)
  }
  def setImpurity(_impurity: String): Unit = {
    impurity = _impurity
    configsMap.put("impurity", impurity)
  }

  def setNumPartitions(num_part: Int): Unit = {
    configsMap.put("partitions", num_part.toString)
  }

  def trainClassifier(initPath: String, isetPath: String, trainPath: String, decPath: String, expandPath: String): Unit = {
    configsMap.put("init", initPath)
    configsMap.put("iset", isetPath)
    configsMap.put("train", trainPath)
    configsMap.put("dec", decPath)
    configsMap.put("expand", expandPath)
    val results = new KDDLogRunner().run(configsMap.toMap[String, String])
    println(results.size)
  }
}

object DecisionTree {
  def main(args: Array[String]): Unit = {
    val dtree = new DecisionTree()
    dtree.initlizeConfig()
    dtree.setImpurity("Gini")
    val initPath = "testdata/dt/pattern.csv"
    val isetPath = "testdata/dt/iset.csv"
    val trainPath = "testdata/dt/train.csv"
    val decPath = "testdata/dt/dec.csv"
    val expandPath = "testdata/dt/expand.csv"
    dtree.trainClassifier(initPath, isetPath, trainPath, decPath, expandPath)
  }
}


