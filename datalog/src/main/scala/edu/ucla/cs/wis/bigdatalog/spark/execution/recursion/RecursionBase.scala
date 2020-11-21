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

import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogSessionState
import edu.ucla.cs.wis.bigdatalog.spark.logical.RecOption
import edu.ucla.cs.wis.bigdatalog.spark.logical.RecOption.RecOption
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

abstract class RecursionBase(
    val name : String,
    recOptions: Seq[RecOption],
    left : SparkPlan,
    right : SparkPlan,
    partitioning: Seq[Int]) extends BinaryExecNode {

  override def output = right.output

  @transient
  final val sessionState = SparkSession.getActiveSession.orNull.sessionState.asInstanceOf[BigDatalogSessionState]

  var iterationCnt = 0

  // 'left' is optional
  override def children = if (left == null) Seq(right) else Seq(left, right)

  override def simpleString = {
    var str = s"$nodeName " + output.mkString("[", ",", "]") + s" (${recOptions.mkString(",")}) " + "[" + name + "]"

    if (partitioning != null && partitioning != Nil)
      str += "[" + partitioning.mkString(",") + "]"
    str
  }

  override def outputPartitioning: Partitioning = {
    val numPartitions = sessionState.conf.numShufflePartitions
    if (partitioning == Nil)
      UnknownPartitioning(numPartitions)
    else
      HashPartitioning(partitioning.zip(output).filter(_._1 == 1).map(_._2), numPartitions)
  }

  override def requiredChildDistribution: Seq[ClusteredDistribution] = {
    // left is exit rule so it will not have aliased argument for arithmetic
    val rightExpressions = partitioning.zip(right.output).filter(_._1 == 1).map(_._2)

    if (left == null) {
      ClusteredDistribution(rightExpressions) :: Nil
    } else {
      val leftExpressions = partitioning.zip(left.output).filter(_._1 == 1).map(_._2)
      ClusteredDistribution(leftExpressions) :: ClusteredDistribution(rightExpressions) :: Nil
    }
  }

  protected def executeExitPlan(): RDD[InternalRow] = {
    val exitRDD = left.execute()
    logInfo("Exit Rule Plan - left rdd:\n" + exitRDD.toDebugString)
    sparkContext.runJob(exitRDD, (iter: Iterator[InternalRow]) => ())
    // update recursive relation to exitRDD
    sessionState.setRecursiveRDD(this.name, exitRDD)
    sessionState.getRecursiveRDD(this.name)
  }

  /**
   * @param iterationCnt -1 if executeUntilFixpoint otherwise denoting the current iteration
   */
  protected def executeRecPlan(iterationCnt: Int): RDD[InternalRow]

  override def doExecute(): RDD[InternalRow] = {
    val isDriver = recOptions.contains(RecOption.Driver)
    if (isDriver) {
      executeExitPlan()
      executeRecPlan(-1)
    } else {
      iterationCnt += 1
      executeRecPlan(iterationCnt)
    }
  }
}
