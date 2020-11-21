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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * Builder to create a BigDatalogSession with options
 */
// scalastyle:off line.size.limit
object BigDatalogSessionBuilder extends Logging {
  def build(appName: String, options: Map[String, String] = Map.empty): SparkSession = {
    logInfo(s"Runtime Options:\n${options.map(e => s"${e._1}=${e._2}").mkString("\n")}")

    // Set config through the abbreviate option key: options.getOrElse(optionKey, defaultValue)
    // If the option is not passed in, the defaultValue will be used across the system.
    // Hint: Use IDEA - "Find in Path" to check how these configs take effect in the system.
    val spark = SparkSession.builder()
      .appName(appName)
      .master(options.getOrElse("master", "local[*]"))
      .config("spark.datalog.pinRDDHostLimit", options.getOrElse("pinRDDHostLimit", "0")) // check [[org.apache.spark.scheduler.Hosts]]
      .config("spark.datalog.aggrIterType", options.getOrElse("aggrIterType", "primitive"))
      .config("spark.datalog.packedBroadcast", options.getOrElse("packedBroadcast", "false"))
      .config("spark.datalog.recursion.fixpointTask", options.getOrElse("fixpointTask", "true"))
      // .config("spark.datalog.recursion.maxIterations", options.getOrElse("maxIterations", String.valueOf(Int.MaxValue)))
      .config("spark.datalog.recursion.maxIterations", options.getOrElse("maxIterations", String.valueOf(10)))
      .config("spark.datalog.recursion.nonMonotonic", options.getOrElse("nonMonotonic", "false"))
      .config("spark.locality.wait", options.getOrElse("localityWait", "0s")) // 0s is a must in pinRDD mode
      .config("spark.sql.codegen.wholeStage", options.getOrElse("codegen", "false"))
      .config("spark.sql.shuffle.partitions", options.getOrElse("partitions", "1"))
      .config("spark.sql.sessionState", options.getOrElse("sessionState", "rasql"))
      .config("spark.datalog.shufflehashjoin.cachebuildside", options.getOrElse("cachebuildside", "true"))
      .config("spark.datalog.kddlog.aggrType", options.getOrElse("aggrType", "old"))
      .getOrCreate()

    spark.sparkContext.setLogLevel(options.getOrElse("log", "ERROR"))

    spark
  }
}
