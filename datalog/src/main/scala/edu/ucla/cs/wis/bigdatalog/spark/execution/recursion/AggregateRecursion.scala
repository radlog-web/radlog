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

import edu.ucla.cs.wis.bigdatalog.spark.execution.exchange.{RecAggrDeltaRDD, RecAggrExchange}
import edu.ucla.cs.wis.bigdatalog.spark.logical.RecOption.RecOption

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan

case class AggregateRecursion(
                               override val name : String,
                               recOptions: Seq[RecOption],
                               @Nullable left : SparkPlan,
                               right : SparkPlan,
                               partitioning: Seq[Int])
  extends RecursionBase(name, recOptions, left, right, partitioning) {

  val maxIterations = sparkContext.getConf
    .getInt("spark.datalog.recursion.maxIterations", Int.MaxValue)
  val nonMonotonic = sparkContext.getConf
    .getBoolean("spark.datalog.recursion.nonMonotonic", false)

  override protected def executeRecPlan(iterationCnt: Int): RDD[InternalRow] = {
    val recRDD = right.execute()
    val executeUntilFixpoint = iterationCnt < 0
    val note = if (executeUntilFixpoint) "(execute until fixpoint)"
    else s"(iteration $iterationCnt)"
    logInfo(s"Recursive Rule Plan - $note right rdd:\n${recRDD.toString}")

    val startTime = System.currentTimeMillis()
    recRDD match {
      case d: RecAggrDeltaRDD =>
        var doNextIteration = true
        var deltaShuffled = d
        var loopCnt = 0
        do {
          // blocking call
          val stats = sparkContext.submitMapStage(deltaShuffled.dependency).get()

          // next iteration will read the shuffled result from previous iteration
          // update recursive relation to [[deltaShuffled]]
          sessionState.setRecursiveRDD(this.name, deltaShuffled)

          loopCnt += 1
          println("##loopcnt##" + loopCnt)
          doNextIteration = executeUntilFixpoint &&
            stats.bytesByPartitionId.sum > 0 && loopCnt < maxIterations
          if (doNextIteration) {
            deltaShuffled = right.execute().asInstanceOf[RecAggrDeltaRDD]
          }
        } while (doNextIteration)
        // Iteration stops due to maxIterations reached
        if (executeUntilFixpoint && loopCnt == maxIterations) {
          logWarning(s"Iteration stops due to maxIterations $maxIterations reached.")
        }
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported rightRDD: $recRDD")
    }
    sessionState.out.printUsedTime("Recursive Iterations", System.currentTimeMillis() - startTime)

    // if you want to print results, please use print inside the next() method of the iterator
    // as opposed to rdd.collect() because the output row is UnsafeRow which is not copied out

    if (nonMonotonic) {
      // if we use nonMonotonic, in this case we don't keep an allRDD in memory
      // the return RDD is deltaShuffled which is already set
      // refer [[SumAggregationIterator]] for more details
      logWarning("Use NonMonotonic Aggregates, return the deltaShuffled RDD.")
      sessionState.getRecursiveRDD(this.name)
    } else if (!executeUntilFixpoint) {
      // if no need to execute until fixpoint, return deltaShuffled RDD
      sessionState.getRecursiveRDD(this.name)
    } else {
      // executeUntilFixpoint and monotonic, return allRDD
      val mAggr = right.asInstanceOf[RecAggrExchange].mAggr
      val allRDD = new InMemoryAggrAllRDD(
        sparkContext,
        name,
        sessionState.conf.numShufflePartitions,
        mAggr, // mAggr.aggrIterType,
        mAggr.output.length
      )
      // when finished execution, update recursive relation to allRDD
      sessionState.setRecursiveRDD(this.name, allRDD)
      allRDD
    }
  }
}
