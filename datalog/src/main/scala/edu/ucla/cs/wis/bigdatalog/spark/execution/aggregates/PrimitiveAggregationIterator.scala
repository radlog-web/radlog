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

package edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.datalog.PartitionDataKeeper

// scalastyle:off line.size.limit
/**
 * Assume grouping column and aggregate value are all int types, thus simplify our implementation a lot.
 * Warning: potential int overflow
 *
 * TODO: support long type and more than one grouping column case
 */
class PrimitiveAggregationIterator(
    name: String,
    mAggr: MonotonicAggregate,
    inputIter: Iterator[InternalRow],
    // numOutputRows: SQLMetric,
    partitionIndex: Int,
    create: Boolean)
  extends Iterator[UnsafeRow] with AggrIter with Logging {

  private val resultExpressions = mAggr.resultExpressions
  private val aggrFunc = mAggr.aggregateExpressions.head.aggregateFunction

  private val isMin = aggrFunc match {
    case _: MMin => true
    case _: MMax => false
    case other => throw new UnsupportedOperationException(s"AggregateFunction $other is not supported")
  }

  private val firstAggr = resultExpressions.map(_.toString).zipWithIndex.find(
    e => e._1.contains(aggrFunc.prettyName)
  )

  private val firstAggrPos = firstAggr match {
    case Some(e) if e._2 > 0 => e._2
    case Some(e) => throw new UnsupportedOperationException(s"Should have at least one grouping column: $resultExpressions")
    case None => throw new RuntimeException(s"Unable to find aggregate $aggrFunc in $resultExpressions")
  }

  // victor: HACK - we store and fetch the aggregation map from dataKeeper (shared by all tasks)
  private val dataKeeper = PartitionDataKeeper.get(name)
  private var aggregationMap = dataKeeper.getAggregationMap(partitionIndex)

  if (create) {
    logInfo(s"Partition: $partitionIndex - create AggregationMap")
    if (aggregationMap != null) {
      throw new RuntimeException("Partition: " + partitionIndex + " sharedAggregationMap already set!")
    }
  } else {
    logInfo(s"Partition: $partitionIndex - reuse existing AggregationMap")
    if (aggregationMap == null) {
      throw new RuntimeException("Partition: " + partitionIndex + " sharedAggregationMap is null!")
    }
  }

  if (aggregationMap == null) {
    aggregationMap = new Int2IntOpenHashMap()
    dataKeeper.setAggregationMap(partitionIndex, aggregationMap)
  }

  val hashMap = aggregationMap.asInstanceOf[Int2IntOpenHashMap]

  override val before: Int = hashMap.size()

  // The function used to read and process input rows.
  def processInputs(): Int2IntOpenHashMap = {
    val deltaSet = new Int2IntOpenHashMap()

    var i = 0
    while (inputIter.hasNext) {
      val newInput = inputIter.next()
      // TODO: support more than one grouping column
      val groupingKey = newInput.getInt(0)
      val v = newInput.getInt(1)

      // will use default value faster ?
      val prevAggr = if (hashMap.containsKey(groupingKey)) {
        hashMap.get(groupingKey)
      } else {
        if (isMin) Int.MaxValue else Int.MinValue
      }

      val newAggr = if (isMin) {
        if (prevAggr > v) v else prevAggr
      } else {
        if (prevAggr > v) prevAggr else v
      }
      hashMap.put(groupingKey, newAggr)

      if (newAggr != prevAggr) {
        deltaSet.put(groupingKey, newAggr)
      }

      i += 1
    }

    logInfo(s"inputIter size: $i")
    deltaSet
  }

  /**
   * Start processing input rows.
   */
  private val deltaSet = processInputs()

  override val after: Int = hashMap.size()

  override val deltaSize: Int = deltaSet.size()

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Iterator's public methods.
  ///////////////////////////////////////////////////////////////////////////

  val rawIter = deltaSet.int2IntEntrySet().fastIterator()
  val numFields = 2

  val unsafeRow = new UnsafeRow(numFields)
  val bufferHolder = new BufferHolder(unsafeRow)
  val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, numFields)

  override final def hasNext(): Boolean = {
    rawIter.hasNext
  }

  override final def next(): UnsafeRow = {
    bufferHolder.reset()
    val next = rawIter.next()

    unsafeRowWriter.write(0, next.getIntKey)
    unsafeRowWriter.write(1, next.getIntValue)

    unsafeRow.setTotalSize(bufferHolder.totalSize())
    unsafeRow
  }
}

class Int2IntAggregationMapIterator(name: String, splitIdx: Int) extends Iterator[UnsafeRow] with Logging {
  val map = PartitionDataKeeper.get(name).getAggregationMap(splitIdx).asInstanceOf[Int2IntOpenHashMap]
  val rawIter = map.int2IntEntrySet().fastIterator()
  val numFields = 2

  val unsafeRow = new UnsafeRow(numFields)
  val bufferHolder = new BufferHolder(unsafeRow)
  val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, numFields)

  override final def hasNext(): Boolean = {
    rawIter.hasNext
  }

  override final def next(): UnsafeRow = {
    bufferHolder.reset()
    val next = rawIter.next()

    unsafeRowWriter.write(0, next.getIntKey)
    unsafeRowWriter.write(1, next.getIntValue)

    unsafeRow.setTotalSize(bufferHolder.totalSize())

    // println("$$ " + unsafeRow)
    unsafeRow
  }
}
