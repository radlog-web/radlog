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

import edu.ucla.cs.wis.bigdatalog.spark.runner.RaSQLRunner

/**
 * Use `build/sbt "project datalog" test:compile` to compile source and test classes in datalog project.
 */
class RaSQLTestSuite extends TestSuite {

  val testCases = Seq(
    ET("test1", Seq("-s1=testdata/s1.csv")),
    ET("test2", Seq("-s1=testdata/s1.csv")),
    ET("tc_ll", Seq("-arc=testdata/arc.csv")),
    ET("sg", Seq("-rel=testdata/sg1.csv")),
    ET("sg", Seq("-rel=testdata/sg2.csv")),
    ET("apsp", Seq("-warc=testdata/s1.csv")),
    ET("sssp", Seq("-warc=testdata/s1.csv", "-startvertex=1")),
    ET("cc", Seq("-arc=testdata/cc.csv")),
    ET("reach", Seq("-rc=testdata/rc1.csv", "-startvertex=1")),
    ET("count_paths", Seq("-rc=testdata/rc1.csv", "-startvertex=1")),
    ET("count_paths", Seq("-rc=testdata/rc2.csv", "-startvertex=1")),
    ET("count_paths_all", Seq("-rc=testdata/rc2.csv")),
    ET("coalesce", Seq("-inter=testdata/coalesce.csv")),
    ET("delivery", Seq("-assbl=testdata/assbl.csv", "-basic=testdata/basic.csv"))
  )

  run(new RaSQLRunner(), testCases)

}
