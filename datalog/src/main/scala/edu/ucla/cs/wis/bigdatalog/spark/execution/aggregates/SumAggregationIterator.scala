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

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}

// scalastyle:off line.size.limit
/**
 * Nonmonotonic Sum, no need to use allRDD. The current application is PageRank.
 *
 * We support number of input columns 2. The column layout will be:
 * 0: Grouping  1: Aggr
 *
 * The Grouping is Integer type, and aggregation value is Double type.
 */
class SumAggregationIterator(
    mAggr: MonotonicAggregate,
    inputIter: Iterator[InternalRow])
  extends Iterator[UnsafeRow] with AggrIter with Logging {

  private val aggrFunc = mAggr.aggregateExpressions.head.aggregateFunction

  if (!aggrFunc.isInstanceOf[MSum]) throw new RuntimeException(s"aggrFunc: $aggrFunc is not MSum.")

  override val before: Int = 0

  if (mAggr.output.length != 2) {
    throw new RuntimeException("output length is not 2.")
  }

  // The function used to read and process input rows.
  def processInputs(): Int2DoubleOpenHashMap = {
    val deltaMap = new Int2DoubleOpenHashMap()

    var i = 0

    while (inputIter.hasNext) {
      val row = inputIter.next()

      val key = row.getInt(0) // 0: Grouping Key 1: Aggregate Value
      val value = row.getDouble(1)

      val prevAggr = if (deltaMap.containsKey(key)) deltaMap.get(key) else 0
      val newAggr = prevAggr + value
      deltaMap.put(key, newAggr)

      i += 1
    }

    logInfo(s"inputIter size: $i")
    deltaMap
  }

  /**
   * Start processing input rows.
   */
  private val deltaMap = processInputs()

  override val after: Int = deltaMap.size()

  override val deltaSize: Int = deltaMap.size()

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Iterator's public methods.
  ///////////////////////////////////////////////////////////////////////////

  val rawIter = deltaMap.int2DoubleEntrySet().fastIterator()
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

    // One Grouping Column
    unsafeRowWriter.write(0, next.getIntKey)
    unsafeRowWriter.write(1, next.getDoubleValue)

    unsafeRow.setTotalSize(bufferHolder.totalSize())
    unsafeRow
  }
}
