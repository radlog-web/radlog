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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.Iterator

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.datalog.set.{HashSet, HashSetRowIterator, SetOp}
import org.apache.spark.sql.execution.UnsafeExternalRowSorter
import org.apache.spark.sql.execution.exchange.{IteratorOfIterators, PackedBroadcastRowIterator}
import org.apache.spark.sql.execution.joins.HashedRelation

// scalastyle:off line.size.limit
object PartitionDataKeeper {
  def instance(): PartitionDataKeeper = {
    TaskContext.get().taskMemoryManager().partitionData.asInstanceOf[PartitionDataKeeper]
  }

  // Recursive Relation PartitionData
  def get(name: String): PartitionData = {
    if (name == null || name.length == 0) {
      throw new IllegalArgumentException("Recursive Relation name is null or empty.")
    }
    TaskContext.get().taskMemoryManager().partitionData.asInstanceOf[PartitionDataKeeper].get(name)
  }
}

class PartitionDataKeeper {
  // BroadcastHashJoinExec HashRelation
  private def buildHashRelation(bcValue: Array[Array[Byte]], key: Seq[Expression], nFields: Int): HashedRelation = {
    // Check [[HashedRelationBroadcastMode]]
    val canonicalizedKey: Seq[Expression] = {
      key.map { e => e.canonicalized }
    }
    val iter = new IteratorOfIterators(bcValue.map { bytes =>
      new PackedBroadcastRowIterator(bytes, nFields)
    }.iterator)
    HashedRelation(iter, canonicalizedKey, bcValue.length,
      taskMemoryManager = TaskContext.get().taskMemoryManager())
  }

  @volatile private var hashedRelation = new ConcurrentHashMap[Long, HashedRelation]()

  def getOrBuildHashedRelation(bc: Broadcast[Array[Array[Byte]]], buildKeys: Seq[Expression], nFields: Int): HashedRelation = {
    // only one thread to build hashedRelation is enough as rows are broadcasted (contain all rows)
    var result = hashedRelation.get(bc.id)
    if (result == null) { // First check (no locking)
      this.synchronized {
        result = hashedRelation.get(bc.id)
        if (result == null) { // Second check (with locking)
          result = buildHashRelation(bc.value, buildKeys, nFields)
          hashedRelation.put(bc.id, result)
        }
      }
    }
    // Very important: otherwise concurrency issues!
    result.asReadOnlyCopy()
  }

  // Recursive Relation PartitionData
  private val partitionDataMap = new ConcurrentHashMap[String, PartitionData]

  def get(name: String): PartitionData = {
    var partitionData = partitionDataMap.get(name)
    if (partitionData == null) { // First check (no locking)
      this.synchronized {
        partitionData = partitionDataMap.get(name)
        if (partitionData == null) { // Second check (with locking)
          partitionData = new PartitionData(name)
          partitionDataMap.put(name, partitionData)
        }
      }
    }
    partitionData
  }
}

/**
 * Keep Data Specific to each partition
 */
class PartitionData(name: String) extends Logging {
  def initDeltaAllRDDHashSets(idx: Integer, iter: Iterator[InternalRow], outputLength: Int): Unit = {
    logInfo(s"Initialize Delta/All RDD for $name - Split: $idx (should be done only once)")

    // extract iterator's value to prevent cross-stage issues
    val arr = SetOp.initDeltaAllRDDHashSets(iter, outputLength)
    val deltaSet = arr(0)
    val allSet = arr(1)

    // TODO: store HashSet instead of Iterator
    setDeltaRDDData(idx, HashSetRowIterator.create(deltaSet))
    setAllRDDData(idx, allSet)
  }

  /** ###################################
   * Youfu Li added here to support non-linear recursion
   */
  private val tempCount = new ConcurrentHashMap[Integer, IntraCount]
  def getTempCount(idx: Integer): IntraCount = {
    tempCount.get(idx)
  }
  def setTempCount(idx: Integer, obj: IntraCount): Unit = {
    tempCount.put(idx, obj)
  }

  private val nonLinearMap = new ConcurrentHashMap[Integer, HashSet]

  def getNonLinearSet(idx: Integer): HashSet = nonLinearMap.get(idx)

  def setNonLinearSet(idx: Integer, h_set: HashSet): Unit = {
    val res = nonLinearMap.get(idx)
    //    if (res != null) throw new RuntimeException("Partition: " + idx + " sharedAggregationMap already set!")
    nonLinearMap.put(idx, h_set)
  }

  // ########################################

  // deltaRDDData
  private val deltaRDDData = new ConcurrentHashMap[Integer, Iterator[InternalRow]]

  def getDeltaRDDData(idx: Integer): Iterator[InternalRow] = {
    deltaRDDData.get(idx)
  }

  def setDeltaRDDData(idx: Integer, obj: Iterator[InternalRow]): Unit = {
    deltaRDDData.put(idx, obj)
  }

  // allRDDData
  private val allRDDData = new ConcurrentHashMap[Integer, HashSet]

  def getAllRDDData(idx: Integer): HashSet = {
    val res = allRDDData.get(idx)
    if (res == null) throw new NullPointerException(s"RecRelation: $name (allRDDData) Split: $idx")
    res
  }

  def setAllRDDData(idx: Integer, obj: HashSet): Unit = {
    val res = allRDDData.get(idx)
//    if (res != null) throw new RuntimeException(s"RecRelation: $name (allRDDData) Split: $idx is already set")
    allRDDData.put(idx, obj)
  }

  // AggregationMap, which is any of the following actual types
  // 1. Int2IntOpenHashMap -- PrimitiveAggregationIterator (MMin/MMax)
  // 2. (Long2IntOpenHashMap Long2IntOpenHashMap/GroupingWithSubKeyMap) -- PrimitiveMCountAggregationIterator (MCount)
  // 3. (UnsafeFixedWidthAggregationMap, ResultProjection) -- TungstenMonotonicAggregationIterator
  private val aggregationMaps = new ConcurrentHashMap[Integer, Object]

  def getAggregationMap(idx: Integer): Object = aggregationMaps.get(idx)

  def setAggregationMap(idx: Integer, map: Object): Unit = {
    val res = aggregationMaps.get(idx)
//    if (res != null) throw new RuntimeException("Partition: " + idx + " sharedAggregationMap already set!")
    aggregationMaps.put(idx, map)
  }

  // cached buildSide HashedRelation
  // TODO: support more than one cached hash relation for each recursive relation
  private val buildSideHashedRelations = new ConcurrentHashMap[Integer, HashedRelation]

  def getBuildSideHashedRelation(idx: Integer): HashedRelation = buildSideHashedRelations.get(idx)

  def putBuildSideHashedRelation(idx: Integer, relation: HashedRelation): Unit = {
    val res = buildSideHashedRelations.get(idx)
    if (res != null) throw new RuntimeException("Partition: " + idx + " buildSideHashedRelation already set!")
    buildSideHashedRelations.put(idx, relation)
  }

  // cached sorter
  // TODO: support more than one sort reuse operator for each recursive relation
  private val sorters = new ConcurrentHashMap[Integer, UnsafeExternalRowSorter]

  def getSorter(idx: Integer): UnsafeExternalRowSorter = sorters.get(idx)

  def putSorter(idx: Integer, sorter: UnsafeExternalRowSorter): Unit = {
    val res = sorters.get(idx)
    if (res != null) throw new RuntimeException("Partition: " + idx + " sorter already set!")
    sorters.put(idx, sorter)
  }
}
