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

import java.util.{HashMap => JavaHashMap}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.datalog.PartitionDataKeeper
import org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap
import org.apache.spark.sql.execution.aggregate.AggregationIterator
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.KVIterator

/**
 * An iterator used to evaluate aggregate functions. It operates on [[UnsafeRow]]s.
 *
 * This iterator first uses hash-based aggregation to process input rows. It uses
 * a hash map to store groups and their corresponding aggregation buffers. If we
 * this map cannot allocate memory from memory manager, it spill the map into disk
 * and create a new one. After processed all the input, then merge all the spills
 * together using external sorter, and do sort-based aggregation.
 *
 * The process has the following step:
 *  - Step 0: Do hash-based aggregation.
 *  - Step 1: Sort all entries of the hash map based on values of grouping expressions and
 *            spill them to disk.
 *  - Step 2: Create an external sorter based on the spilled sorted map entries and reset the map.
 *  - Step 3: Get a sorted [[KVIterator]] from the external sorter.
 *  - Step 4: Repeat step 0 until no more input.
 *  - Step 5: Initialize sort-based aggregation on the sorted iterator.
 * Then, this iterator works in the way of sort-based aggregation.
 *
 * The code of this class is organized as follows:
 *  - Part 1: Initializing aggregate functions.
 *  - Part 2: Methods and fields used by setting aggregation buffer values,
 *            processing input rows from inputIter, and generating output
 *            rows.
 *  - Part 3: Methods and fields used by hash-based aggregation.
 *  - Part 4: Methods and fields used when we switch to sort-based aggregation.
 *  - Part 5: Methods and fields used by sort-based aggregation.
 *  - Part 6: Loads input and process input rows.
 *  - Part 7: Public methods of this iterator.
 *  - Part 8: A utility function used to generate a result when there is no
 *            input and there is no grouping expression.
 *
 * @param groupingExpressions
 *   expressions for grouping keys
 * @param aggregateExpressions
 * [[AggregateExpression]] containing [[AggregateFunction]]s with mode [[Partial]],
 * [[PartialMerge]], or [[Final]].
 * @param aggregateAttributes the attributes of the aggregateExpressions'
 *   outputs when they are stored in the final aggregation buffer.
 * @param resultExpressions
 *   expressions for generating output rows.
 * @param newMutableProjection
 *   the function used to create mutable projections.
 * @param originalInputAttributes
 *   attributes of representing input rows from `inputIter`.
 * @param inputIter
 *   the iterator containing input [[UnsafeRow]]s.
 */
class TungstenMonotonicAggregationIterator(
    name: String,
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    originalInputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow],
    // numOutputRows: SQLMetric,
    partitionIndex: Int,
    create: Boolean)
  extends AggregationIterator(
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) with AggrIter with Logging {

  // Creates a new aggregation buffer and initializes buffer values.
  // This function should be only called at most two times (when we create the hash map,
  // and when we create the re-used buffer for sort-based aggregation).
  private def createNewAggregationBuffer(): UnsafeRow = {
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val buffer: UnsafeRow = UnsafeProjection.create(bufferSchema.map(_.dataType))
      .apply(new GenericMutableRow(bufferSchema.length))
    // Initialize declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initialize imperative aggregates' buffer values
    aggregateFunctions.collect { case f: ImperativeAggregate => f }.foreach(_.initialize(buffer))
    buffer
  }

  // Creates a function used to generate output rows.
  override def generateResultProjection(): (UnsafeRow, MutableRow) => UnsafeRow = {
    val modes = aggregateExpressions.map(_.mode).distinct
    if (modes.nonEmpty && !modes.contains(Final) && !modes.contains(Complete)) {
      // Fast path for partial aggregation, UnsafeRowJoiner is usually faster than projection
      val groupingAttributes = groupingExpressions.map(_.toAttribute)
      val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
      val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
      val bufferSchema = StructType.fromAttributes(bufferAttributes)
      val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)

      (currentGroupingKey: UnsafeRow, currentBuffer: MutableRow) => {
        unsafeRowJoiner.join(currentGroupingKey, currentBuffer.asInstanceOf[UnsafeRow])
      }
    } else {
      super.generateResultProjection()
    }
  }

  // An aggregation buffer containing initial buffer values. It is used to
  // initialize other aggregation buffers.
  // private[this]
  val initialAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  // This is the hash map used for hash-based aggregation. It is backed by an
  // UnsafeFixedWidthAggregationMap and it is used to store
  // all groups and their corresponding aggregation buffers for hash-based aggregation.

  // victor: HACK - we store and fetch the aggregation map from dataKeeper (shared by all tasks)
  private val dataKeeper = PartitionDataKeeper.get(name)
  private val stored = dataKeeper.getAggregationMap(partitionIndex)

  if (create) {
    logInfo(s"Partition: $partitionIndex - create AggregationMap")
    if (stored != null) {
      throw new RuntimeException("Partition: " + partitionIndex + " sharedAggregationMap already set!")
    }
  } else {
    logInfo(s"Partition: $partitionIndex - reuse existing AggregationMap")
    if (stored == null) {
      throw new RuntimeException("Partition: " + partitionIndex + " sharedAggregationMap is null!")
    }
  }

  private var aggregationMap = if (stored != null) {
    stored.asInstanceOf[(UnsafeFixedWidthAggregationMap, _)]._1
  } else {
    null
  }

  if (aggregationMap == null) {
    aggregationMap = new UnsafeFixedWidthAggregationMap(
      initialAggregationBuffer,
      StructType.fromAttributes(aggregateFunctions.flatMap(_.aggBufferAttributes)),
      StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
      TaskContext.get().taskMemoryManager(),
      1024 * 16, // initial capacity
      TaskContext.get().taskMemoryManager().pageSizeBytes,
      false) // disable tracking of performance metrics
    val obj = (aggregationMap, generateOutput)
    dataKeeper.setAggregationMap(partitionIndex, obj)
  } else {
    aggregationMap.setInitialAggregationBuffer(initialAggregationBuffer)
  }

  val hashMap = aggregationMap

  override val before: Int = hashMap.size()

  // The function used to read and process input rows. When processing input rows,
  // it first uses hash-based aggregation by putting groups and their buffers in
  // hashMap. If there is not enough memory, it will multiple hash-maps, spilling
  // after each becomes full then using sort to merge these spills, finally do sort
  // based aggregation.
  def processInputs(fallbackStartsAt: (Int, Int)): JavaHashMap[UnsafeRow, UnsafeRow] = {
    val deltaSet = new JavaHashMap[UnsafeRow, UnsafeRow]() /* APS */
    if (groupingExpressions.isEmpty) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      // Note that it would be better to eliminate the hash map entirely in the future.
      val groupingKey = groupingProjection.apply(null)
      val buffer: UnsafeRow = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        processRow(buffer, newInput)
        deltaSet.put(groupingKey.copy(), buffer.copy()) /* APS */
      }
    } else {
      // create an UnsafeRow to store the previous buffer to see if it changed after evaluation
      var before: UnsafeRow = null
      var i = 0
      // logInfo("## hashMap keys: " + hashMap.size())
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        val groupingKey = groupingProjection.apply(newInput)
        var buffer: UnsafeRow = null
        if (i < fallbackStartsAt._2) {
          buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
        }
        /* APS: never changes to sort-based aggregation  */
        if (buffer == null) {
          // failed to allocate the first page
          throw new OutOfMemoryError("No enough memory for aggregation")
        }
        // APS - to reduce object creation only copy() on the first item
        if (i == 0) {
          before = buffer.copy()
        } else {
          before.copyFrom(buffer)
        }

        // logInfo("## Buffer: " + buffer + " newInput: " + newInput)
        processRow(buffer, newInput)

        // deltaSet is returned and to form the nextDeltaPrimeRDD
        // we need to copy the key/value to store them into separate hashmap
        // because the groupingKey/buffer are reused here
        // APS - if the value has been updated, record the tuple changed in the deltaSet
        // TODO - use optimized hashmap - these copy()s create alot of objects
        if (!before.equals(buffer)) {
          deltaSet.put(groupingKey.copy(), buffer.copy())
        }

        i += 1
      }
      // logInfo(s"inputIter size: $i")
    }
    deltaSet
  }

  /**
    * Start processing input rows.
    */
  val deltaSet = processInputs((Int.MaxValue, Int.MaxValue))

  override val after: Int = hashMap.size()

  override val deltaSize: Int = deltaSet.size()

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Iterator's public methods.
  ///////////////////////////////////////////////////////////////////////////

  // TODO: optimize with fastutil map
  val deltaSetIterator = deltaSet.entrySet().iterator()

  override final def hasNext: Boolean = {
    deltaSetIterator.hasNext
  }

  override def next(): UnsafeRow = {
    if (hasNext) {
      val entry = deltaSetIterator.next()
      val result = generateOutput(entry.getKey, entry.getValue)

      // numOutputRows += 1
      // logInfo("## Monotonic Iter next(): " + result)
      result
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }
}

class InMemoryAggregationMapIterator(name: String, splitIdx: Int) extends Iterator[UnsafeRow] with Logging {
  private val (aggregationMap, generateOutput) =
    PartitionDataKeeper.get(name).getAggregationMap(splitIdx)
      .asInstanceOf[(UnsafeFixedWidthAggregationMap, (UnsafeRow, MutableRow) => UnsafeRow)]

  private val aggregationBufferMapIterator = aggregationMap.nonDestructiveIterator()
  // Pre-load the first key-value pair from the aggregationBufferMapIterator.
  private var mapIteratorHasNext = aggregationBufferMapIterator.next()

  override final def hasNext: Boolean = {
    // victor: non-sortBased
    mapIteratorHasNext
  }

  override def next(): UnsafeRow = {
    if (hasNext) {
      val result =
        generateOutput(
          aggregationBufferMapIterator.getKey,
          aggregationBufferMapIterator.getValue)

      // Pre-load next key-value pair form aggregationBufferMapIterator to make hasNext
      // idempotent.
      mapIteratorHasNext = aggregationBufferMapIterator.next()

      // logInfo("$$: " + result)
      result
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }
}
