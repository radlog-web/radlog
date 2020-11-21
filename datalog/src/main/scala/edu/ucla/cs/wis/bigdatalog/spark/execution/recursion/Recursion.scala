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

package edu.ucla.cs.wis.bigdatalog.spark.execution.recursion

import javax.annotation.Nullable

import edu.ucla.cs.wis.bigdatalog.spark.execution.exchange.RecDeltaRDD
import edu.ucla.cs.wis.bigdatalog.spark.logical.RecOption.RecOption
import org.apache.spark.rdd._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.catalyst.InternalRow

case class Recursion(
                      override val name: String,
                      recOptions: Seq[RecOption],
                      @Nullable left: SparkPlan,
                      right: SparkPlan,
                      partitioning: Seq[Int])
  extends RecursionBase(name, recOptions, left, right, partitioning) {

  override protected def executeRecPlan(iterationCnt: Int): RDD[InternalRow] = {
    val recRDD = right.execute()
    val executeUntilFixpoint = iterationCnt < 0
    val note = if (executeUntilFixpoint) "(execute until fixpoint)" else s"(iteration $iterationCnt)"
    logInfo(s"Recursive Rule Plan - $note right rdd:\n${recRDD.toDebugString}")

    val startTime = System.currentTimeMillis()
    recRDD match {
      case d: RecDeltaRDD =>
        var doNextIteration = true
        var deltaShuffled = d
        do {
          // blocking call
          val stats = sparkContext.submitMapStage(deltaShuffled.dependency).get()

          // next iteration will read the shuffled result from previous iteration
          // update recursive relation to [[deltaShuffled]]
          sessionState.setRecursiveRDD(this.name, deltaShuffled)

          doNextIteration = executeUntilFixpoint && stats.bytesByPartitionId.sum > 0
          if (doNextIteration) {
            deltaShuffled = right.execute().asInstanceOf[RecDeltaRDD]
          }
        } while (doNextIteration)
      case other =>
        if (!executeUntilFixpoint) {
          throw new RuntimeException("FixedPointJob cannot be used for non-iterative execution.")
        }
        if (other.isInstanceOf[ShuffledRowRDD]) {
          logWarning("Not replacing ShuffledRowRDD in the right plan.")
        }
        sparkContext.runFixedPointJob(recRDD)
    }
    sessionState.out.printUsedTime("Recursive Iterations", System.currentTimeMillis() - startTime)

    if (!executeUntilFixpoint) {
      // if no need to execute until fixpoint, return deltaShuffled RDD
      sessionState.getRecursiveRDD(this.name)
    } else {
      // executeUntilFixpoint, return allRDD
      val allRDD = new InMemoryAllRDD(sparkContext, name, sessionState.conf.numShufflePartitions)
      // when finished execution, update recursive relation to allRDD
      sessionState.setRecursiveRDD(this.name, allRDD)
      allRDD
    }
  }
}
