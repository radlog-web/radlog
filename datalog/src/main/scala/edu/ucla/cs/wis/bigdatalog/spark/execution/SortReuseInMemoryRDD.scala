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

package edu.ucla.cs.wis.bigdatalog.spark.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.Hosts
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.datalog.PartitionDataKeeper
import org.apache.spark.sql.execution.UnsafeExternalRowSorter
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag


class InitSortReuseInMemoryRDD[T: ClassTag](
    name: String,
    var prev: RDD[T],
    f: () => UnsafeExternalRowSorter,
    preservesPartitioning: Boolean = false)
  extends RDD[UnsafeRow](prev) {

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

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def persist(): this.type = {
    throw new UnsupportedOperationException()
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    throw new UnsupportedOperationException()
  }

  override def checkpoint(): Unit = {
    throw new UnsupportedOperationException()
  }

  override def getOrCompute(split: Partition, context: TaskContext): Iterator[UnsafeRow] = {
     throw new UnsupportedOperationException()
  }

  override def compute(split: Partition, context: TaskContext): Iterator[UnsafeRow] = {
    throw new UnsupportedOperationException()
  }

  override def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[UnsafeRow] = {
    val dataKeeper = PartitionDataKeeper.get(name)
    val sorter = dataKeeper.getSorter(split.index)
    if (sorter == null) {
      logInfo(s"Sorting Partition: ${split.index}")
      val newed = f()
      dataKeeper.putSorter(split.index, newed)
      val input = firstParent[T].iterator(split, context)
      newed.sortReuseInit(input.asInstanceOf[Iterator[UnsafeRow]])
    } else {
      throw new RuntimeException(s"sorter for partition ${split.index} is set before initialization")
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}

class SortReuseInMemoryRDD[T: ClassTag](
    name: String,
    var prev: RDD[T],
    f: () => UnsafeExternalRowSorter,
    preservesPartitioning: Boolean = false)
  extends RDD[UnsafeRow](prev) {

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

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def persist(): this.type = {
    throw new UnsupportedOperationException()
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    throw new UnsupportedOperationException()
  }

  override def checkpoint(): Unit = {
    throw new UnsupportedOperationException()
  }

  override def getOrCompute(split: Partition, context: TaskContext): Iterator[UnsafeRow] = {
     throw new UnsupportedOperationException()
  }

  override def compute(split: Partition, context: TaskContext): Iterator[UnsafeRow] = {
    throw new UnsupportedOperationException()
  }

  override def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[UnsafeRow] = {
    val dataKeeper = PartitionDataKeeper.get(name)
    val sorter = dataKeeper.getSorter(split.index)
    if (sorter == null) {
      throw new RuntimeException(s"sorter for partition ${split.index} is not found for reuse")
    } else {
      sorter.sortReuse()
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
