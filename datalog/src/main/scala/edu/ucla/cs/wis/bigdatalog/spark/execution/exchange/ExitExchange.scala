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

package edu.ucla.cs.wis.bigdatalog.spark.execution.exchange

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.Hosts
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.datalog.PartitionDataKeeper
import org.apache.spark.sql.datalog.set.{HashSetRowIterator, SetOp}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchange}
import org.apache.spark.sql.execution.metric.SQLMetrics

// scalastyle:off line.size.limit
case class ExitExchange(
    name: String,
    var newPartitioning: Partitioning,
    child: SparkPlan) extends Exchange {

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"))

  override def outputPartitioning: Partitioning = newPartitioning

  private val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val shuffleDependency = ShuffleExchange.prepareShuffleDependency(
      child.execute(), child.output, newPartitioning, serializer)
    // victor: insert InMemoryDeltaRDD to pin the post-shuffle RDD on specific machine
    // thus make sure data is shuffled to the desired nodes
    val shuffledRDD = new ShuffledRowRDD(shuffleDependency)
    new ExitDeltaRDD(name, output.size, shuffledRDD)
  }
}

class ExitDeltaRDD(name: String, outputLength: Int, childRDD: RDD[InternalRow]) extends RDD[InternalRow](childRDD) {

  logInfo("Create InMemoryDeltaRDD with # of partitions: " + childRDD.partitions.length)

  // we do not override partitioner, not sure if it is correct
  // override val partitioner: Option[Partitioner]  = None

  override protected def getPartitions: Array[Partition] = childRDD.partitions

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val pinRDDHostLimit = conf.getInt("spark.datalog.pinRDDHostLimit", 0)
    if (pinRDDHostLimit > 0) {
      val host = Hosts.getPinnedHost(split, pinRDDHostLimit)
      logDebug(s"$split has pinned host: $host")
      Seq(host)
    } else {
      Nil
    }
  }

  override def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val dataKeeper = PartitionDataKeeper.get(name)
    val stored = dataKeeper.getDeltaRDDData(split.index)
    if (stored == null) {
      // exit rule plan: rdd is shuffled base relation
      val iter = childRDD.iterator(split, context)
      dataKeeper.initDeltaAllRDDHashSets(split.index, iter, outputLength)

      // return an empty iterator as the one contains real data is saved in dataKeeper
      // it is ok because this branch is executed in exit rule plan and the result iterator has no use
      Iterator()
    } else {
      // recursive rule plan: after each iteration, the stored iterator will be update to new one
      // check [[FixedPointResultTask]] for details
      // There exists an interesing bug that store.toSeq.iterator is not equal the res that store

      // TODO[JIACHENG]: WE NEED A BETTER WAY TO SOLVE THIS ISSUES (FOR PERFORMANCE)
      val (stored1, stored2) = stored.duplicate
      dataKeeper.setDeltaRDDData(split.index, stored1)
      return stored2
    }
  }

  override def persist(): this.type = {
    throw new UnsupportedOperationException()
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    throw new UnsupportedOperationException()
  }

  override def checkpoint(): Unit = {
    throw new UnsupportedOperationException()
  }

  override def getOrCompute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
     throw new UnsupportedOperationException()
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    throw new UnsupportedOperationException()
  }

}
