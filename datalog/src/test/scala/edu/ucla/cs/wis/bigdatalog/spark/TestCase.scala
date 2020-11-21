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

trait TestCase {
  val name: String
  val args: Seq[String]
  val resultFilePath: String
}

case class ET(name: String, args: Seq[String]) extends TestCase {
  // resultFilePath is constructed from the input file name and program args
  val resultFilePath: String = {
    val resultFileName = name + "_" + args.map(
      _.split("=").last.split("/").last.split("\\.").head.trim
    ).mkString("_") + ".csv"
    s"testdata/results/$resultFileName"
  }
}
