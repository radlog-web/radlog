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

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.Hosts
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.datalog.PartitionDataKeeper
import org.apache.spark.sql.datalog.set.HashSetRowIterator


// scalastyle:off line.size.limit
class InMemoryAllRDD(sc: SparkContext, name: String, numPartitions: Int) extends RDD[InternalRow](sc, Nil) {

  logInfo("Create InMemoryAllRDD with # of partitions: " + numPartitions)

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](numPartitions) {
      i => new Partition { override def index: Int = i }
    }
  }

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
    HashSetRowIterator.create(PartitionDataKeeper.get(name).getAllRDDData(split.index))
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
