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

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.Hosts
import org.apache.spark.sql.datalog.PartitionDataKeeper
import org.apache.spark.sql.execution.joins.HashedRelation

import scala.reflect.ClassTag


class MapPartitionsRDDInMemory[U: ClassTag, T: ClassTag](
    name: String,
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

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

  override def getOrCompute(split: Partition, context: TaskContext): Iterator[U] = {
     throw new UnsupportedOperationException()
  }

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    throw new UnsupportedOperationException()
  }

  override def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[U] = {
    val dataKeeper = PartitionDataKeeper.get(name)

    val stored = dataKeeper.getBuildSideHashedRelation(split.index)
    if (stored == null) {
      logInfo(s"Compute BuildSideHashedRelation for Partition: ${split.index}")
      // f wraps the relation with Iterator(relation) as we need to always return an Iterator
      val iter = f(context, split.index, firstParent[T].iterator(split, context))
      val relation = iter.next()
      dataKeeper.putBuildSideHashedRelation(split.index, relation.asInstanceOf[HashedRelation])
    } else {
//      throw new RuntimeException(s"BuildSideHashedRelation for Partition: ${split.index} is called more than once")
    }
    // MapPartitionsRDDInMemory is not expected to have child RDDs, thus return empty iterator
    Iterator.empty
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
