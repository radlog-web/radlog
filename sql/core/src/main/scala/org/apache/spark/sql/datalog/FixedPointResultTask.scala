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

package org.apache.spark.sql.datalog

import java.io.Serializable
import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ResultTask, TaskLocation}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.datalog.set.{HashSet, HashSetRowIterator, SetOp}

// scalastyle:off line.size.limit
class FixedPointResultTask[T, U](stageId: Int,
                                 stageAttemptId: Int,
                                 taskBinary: Broadcast[Array[Byte]],
                                 partition: Partition,
                                 locs: Seq[TaskLocation],
                                 outputId: Int,
                                 localProperties: Properties,
                                 metrics: TaskMetrics)
  extends ResultTask[T, U](stageId, stageAttemptId, taskBinary, partition, locs, outputId, localProperties, metrics)
    with Logging with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    logInfo("RDD is: " + rdd.toString)

    // TODO: set a special fixpointOp to include the recursive relation name
    val dataKeeper = PartitionDataKeeper.get(null)
    var diffSet: HashSet = null
    var i = 0
    do {
      i += 1
      // In each new iteration, we re-compute the rdd iterator but will get different results.
      // That is because the DeltaRDD (recursive relation) is actually getting the in-memory data that
      // we swapped in at the end of each iteration.
      val iter = time(s"Iteration $i - Compute new joined results", {
        rdd.iterator(partition, context).asInstanceOf[Iterator[InternalRow]]
      })
      val allRDDSet = dataKeeper.getAllRDDData(partition.index)
      // logInfo(s"All set size ${allRDDSet.size} (partition ${partition.index})")

      // val iter = HashSetRowIterator.create(allRDDSet)
      // iter.foreach(e => logDebug(e.toString))

      val start = System.currentTimeMillis()
      diffSet = SetOp.diff(iter, allRDDSet)
      logInfo("Diff set size %s (partition %s): %s ms"
        .format(diffSet.size, partition.index, System.currentTimeMillis() - start))

      time("Union to allRDDSet", {
        allRDDSet.union(diffSet)
      })

      dataKeeper.setDeltaRDDData(partition.index, HashSetRowIterator.create(diffSet)) // TODO: optimize row iterator
    } while (diffSet.size() > 0)

    logInfo(s"Total time w/ deser (partition ${partition.index}): ${System.currentTimeMillis() - deserializeStartTime} ms" )
    false.asInstanceOf[U]
  }

  private def time[R](jobName: String, block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    logInfo(s"$jobName (partition ${partition.index}): ${t1 - t0} ms")
    result
  }

  // This is only callable on the driver side.
  // override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "FixedPointResultTask(" + stageId + ", " + partitionId + ")"

}
