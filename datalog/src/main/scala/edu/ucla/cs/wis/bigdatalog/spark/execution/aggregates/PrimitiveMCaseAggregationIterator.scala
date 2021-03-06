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

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.datalog.{Convert, IntraCount, PartitionDataKeeper}

// scalastyle:off line.size.limit
/**
 * Assume grouping column and aggregate value are all int types, thus simplify our implementation a lot.
 * Warning: potential int overflow
 *
 * TODO: support long type and more than one grouping column case
 */
class PrimitiveMCaseAggregationIterator(
    name: String,
    mAggr: MonotonicAggregate,
    inputIter: Iterator[InternalRow],
    // numOutputRows: SQLMetric,
    partitionIndex: Int,
    create: Boolean)
  extends Iterator[UnsafeRow] with AggrIter with Logging {

  private val tol: Double = 1e-3
  private val resultExpressions = mAggr.resultExpressions
  private val aggrFunc = mAggr.aggregateExpressions.head.aggregateFunction

  private val isMin = aggrFunc match {
    case _: MMin => true
    case other => false
  }

  private val isMax = aggrFunc match {
    case _: MMax => true
    case other => false
  }

  private val isMinMax = isMin || isMax

  private val isMCount = aggrFunc match {
    case _: MCount => true
    case other => false
  }

  private val isMSum = aggrFunc match {
    case _: MSum => true
    case other => false
  }

  private val isCMin = aggrFunc match {
    case _: CMin => true
    case other => false
  }

  private val isCMax = aggrFunc match {
    case _: CMax => true
    case other => false
  }

  private val isChain = isCMin || isCMax

  private val isMAvg = aggrFunc match {
    case _: MAvg => true
    case other => false
  }

  // victor: HACK - we store and fetch the aggregation map from dataKeeper (shared by all tasks)
  private val dataKeeper = PartitionDataKeeper.get(name)
  private var storedMaps = dataKeeper.getAggregationMap(partitionIndex)
  private var tempCount = dataKeeper.getTempCount(partitionIndex)

  if (create) {
    logInfo(s"Partition: $partitionIndex - create AggregationMap")
    if (storedMaps != null) {
//      throw new RuntimeException("Partition: " + partitionIndex + " sharedAggregationMap already set!")
    }
  } else {
    logInfo(s"Partition: $partitionIndex - reuse existing AggregationMap")
    if (storedMaps == null) {
      throw new RuntimeException("Partition: " + partitionIndex + " sharedAggregationMap is null!")
    }
  }

  if(tempCount == null) {
    val occ = name match {
      case "model" => 2
      case "pattern" => 1
      case "distc" => 1
      case "fitset" => 1
      case "csetX" => 1
      case "csets" => 1
      case "ovlp" => 1
      case _ => 4
    }
    tempCount = new IntraCount(occ)
    dataKeeper.setTempCount(partitionIndex, tempCount)
  }

  if (storedMaps == null) {
    storedMaps = (new Long2DoubleOpenHashMap(), new Long2DoubleOpenHashMap(), new Long2DoubleOpenHashMap())
    dataKeeper.setAggregationMap(partitionIndex, storedMaps)
  }

  val hashMap = storedMaps.asInstanceOf[(Long2DoubleOpenHashMap, _, _)]._1
  val subKeyMap = storedMaps.asInstanceOf[(_, Long2DoubleOpenHashMap, _)]._2
  var previousDeltaMap = storedMaps.asInstanceOf[(_, _, Long2DoubleOpenHashMap)]._3

  override val before: Int = hashMap.size

  // input has one more column (occurrence variable) than output
  val inputLen = mAggr.output.length +  (if (isMinMax || isChain || isMAvg) 0 else 1)
  if (!isMinMax && inputLen < 3) throw new RuntimeException("inputLen is less than 3.")

  def processInputs(): Long2DoubleOpenHashMap = {
    val deltaMap = new Long2DoubleOpenHashMap()
    var i = 0
//    println(name)
    while (inputIter.hasNext) {
      val row = inputIter.next()
//      println(row)
      if (isMinMax && (!isChain)) {
        val groupingKey: Long = inputLen match {
          case 2 => row.getInt(0) // 0: Grouping 1: Sub 2: Aggr
          case 3 => Convert.intsToLong(row.getInt(0), row.getInt(1)) // 0,1: Grouping 2: Sub 3: Aggr
          case 4 => Convert.shortsToLong(Array(row.getInt(0), row.getInt(1), row.getInt(2), 0)) // 0,1,2: Grouping; 3: Aggr
          case 5 => Convert.shortsToLong(Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3))) // 0,1,2,3: Grouping; 4: Aggr
          case _ => throw new RuntimeException(s"inputLen $inputLen is not supported.")
        }
//        var v = row.getInt(inputLen - 1)
        val v = row.getDouble(inputLen - 1)
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
          deltaMap.put(groupingKey, newAggr)
        }
      } else if (isChain) {
        val groupingKey: Long = inputLen match {
          case 2 => row.getInt(0) // 0: Grouping 1: chain 2: Aggr
          case 3 => Convert.intsToLong(row.getInt(0), row.getInt(1)) // 0,1: Grouping 2: chain 3: Aggr
          case 4 => Convert.shortsToLong(Array(row.getInt(0), row.getInt(1), row.getInt(2), 0)) // 0,1,2: Grouping 3: chain 4: Aggr
          case 5 => Convert.shortsToLong(Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3))) // 0,1,2: Grouping 3: chain 4: Aggr
          case _ => throw new RuntimeException(s"inputLen $inputLen is not supported.")
        }

        val groupingKeyWithoutChain: Long = inputLen match {
          case 3 => row.getInt(0) // 0: Grouping 1: chain 2: Aggr
          case 4 => Convert.intsToLong(row.getInt(0), row.getInt(1)) // 0,1: Grouping 2: chain 3: Aggr
          case 5 => Convert.shortsToLong(Array(row.getInt(0), row.getInt(1), row.getInt(2), 0)) // 0,1,2: Grouping 3: chain 4: Aggr
          case _ => throw new RuntimeException(s"inputLen $inputLen is not supported.")
        }

//        var v = row.getInt(inputLen - 1)
//        if (v < 1E-100 || v >= 1E10) {
//          v = Math.round(row.getDouble(inputLen - 1)).toInt
//        }
        val v = row.getDouble(inputLen - 1)

        val prevSubAggr: Double = if (subKeyMap.containsKey(groupingKeyWithoutChain)) {
          subKeyMap.get(groupingKeyWithoutChain)
        } else {
          if (isCMin) Int.MaxValue else Int.MinValue
        }

        val newAggr = if (isCMin) {
          if (prevSubAggr > v) v else prevSubAggr
        } else {
          if (prevSubAggr > v) prevSubAggr else v
        }
        // val newAggr = v
        //        hashMap.put(groupingKey, newAggr)
        subKeyMap.put(groupingKeyWithoutChain, newAggr)

        if (newAggr != prevSubAggr) {
          deltaMap.put(groupingKey, newAggr)
        }
      } else if (isMAvg) {
        val groupingKey: Long = inputLen match {
          case 2 => row.getInt(0) // 0: Grouping 1: Sub 2: Aggr
          case 3 => Convert.intsToLong(row.getInt(0), row.getInt(1)) // 0,1: Grouping 2: Sub 3: Aggr
          case 4 => Convert.shortsToLong(Array(row.getInt(0), row.getInt(1), row.getInt(2), 0)) // 0,1,2: Grouping; 3: Aggr
          case 5 => Convert.shortsToLong(Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3))) // 0,1,2,3: Grouping 4: Aggr
          case _ => throw new RuntimeException(s"inputLen $inputLen is not supported.")
        }
//        var v = row.getInt(inputLen - 1)
        val v = row.getDouble(inputLen - 1)
        val pre_avg: Double = if (hashMap.containsKey(groupingKey)) {
          hashMap.get(groupingKey)
        } else {
          0
        }
        val pre_count = if (subKeyMap.containsKey(groupingKey)) {
          subKeyMap.get(groupingKey)
        } else {
          0
        }

        val pre_avg_sum = pre_avg * pre_count

        val new_count = pre_count + 1

        val new_avg = (pre_avg_sum + v) / new_count


        hashMap.put(groupingKey, new_avg)
        subKeyMap.put(groupingKey, new_count)

        deltaMap.put(groupingKey, new_avg)

      } else {
        val groupingKey: Long = inputLen match {
          case 3 => row.getInt(0) // 0: Grouping 1: Sub 2: Aggr
          case 4 => Convert.intsToLong(row.getInt(0), row.getInt(1)) // 0,1: Grouping 2: Sub 3: Aggr
          case 5 => Convert.shortsToLong(Array(row.getInt(0), row.getInt(1), row.getInt(2), 0)) // 0,1,2: Grouping 3: Sub 4: Aggr
          case 6 => Convert.shortsToLong(Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3))) // 0,1,2, 3: Grouping 4: Sub 5: Aggr
          case _ => throw new RuntimeException(s"inputLen $inputLen is not supported.")
        }

        val groupingKeyWithSub: Long = inputLen match {
          case 3 => Convert.intsToLong(row.getInt(0), row.getInt(1))
          // must use the same accessor (getInt) as groupingKey
          case 4 => Convert.shortsToLong(Array(row.getInt(0), row.getInt(1), row.getInt(2), 0))
          case 5 => Convert.shortsToLong(Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3)))
          case 6 => Convert.shortsToLong(Array(row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3), row.getInt(4)))
          case _ => throw new RuntimeException(s"inputLen $inputLen is not supported.")
        }

//        var v = row.getInt(inputLen - 1)
        val v = row.getDouble(inputLen - 1)

        // println(s"### Split: $partitionIndex MSum Input: ${row.getInt(0)} ${row.getInt(1)} $v ($create)")
        val prevAggr = if (hashMap.containsKey(groupingKey)) {
          hashMap.get(groupingKey)
        } else {
          hashMap.put(groupingKey, 0)
          0
        }

        val prevSubAggr = if (subKeyMap.containsKey(groupingKeyWithSub)) {
          subKeyMap.get(groupingKeyWithSub)
        } else {
          subKeyMap.put(groupingKeyWithSub, 0)
          0
        }

        val deltaV = v - prevSubAggr
        if (Math.abs(deltaV) > tol) {
          subKeyMap.put(groupingKeyWithSub, v)

          // println(s"### Split: $partitionIndex Group: ${row.getInt(0)} Sub: ${row.getInt(1)} Aggr: ${prevSubAggr} -> $v ($create)")
          val newAggr = prevAggr + deltaV
          hashMap.put(groupingKey, newAggr)
          deltaMap.put(groupingKey, newAggr)
        }
      }
      i += 1
    }

    logInfo(s"inputIter size: $i")
    // TODO: TEMPORARY HARD CODE FIX, IS NOT PERVASIZE SUITABLE
    // THE CORE REASON IS THAT WE NEED THE Hashmap WONT BE MODIFIED IN ONE ITEARATION
    // AND MODIFIED UNTIL THE LAST ACCESS IN ONE ITERATION
    tempCount.incUsedCount()
    val recursiveRelations = Array("model", "pattern", "distc", "ovlp", "csets", "csetX", "fitset") // "pattern", "distc", "fdistc", "cntdecyn", "cntdecy", "cntdec", "gini"
    if (recursiveRelations.contains(name)) {
      if (deltaMap.isEmpty && tempCount.isLastUsedInIteration) {
        previousDeltaMap
      } else {
        previousDeltaMap.clear()
        previousDeltaMap.putAll(deltaMap)
        deltaMap
      }
    } else {
      deltaMap
    }
  }

  /**
   * Start processing input rows.
   */
  private val deltaMap = processInputs()

  override val after: Int = hashMap.size

  override val deltaSize: Int = deltaMap.size

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Iterator's public methods.
  ///////////////////////////////////////////////////////////////////////////

  val rawIter = deltaMap.long2DoubleEntrySet().fastIterator()
  val numFields = mAggr.output.length // victor: as we don't need occurrence variable in output row

  val unsafeRow = new UnsafeRow(numFields)
  val bufferHolder = new BufferHolder(unsafeRow)
  val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, numFields)

  override final def hasNext(): Boolean = {
    rawIter.hasNext
  }

  override final def next(): UnsafeRow = {
    bufferHolder.reset()
    val next = rawIter.next()
    val value = next.getDoubleValue // .round.toInt
    // One Grouping Column
    // unsafeRowWriter.write(0, next.getLongKey.asInstanceOf[Int])
    // unsafeRowWriter.write(1, next.getDoubleValue)


    numFields match {
      case 2 => // One Grouping Column
        unsafeRowWriter.write(0, next.getLongKey.asInstanceOf[Int])
        unsafeRowWriter.write(1, value)
      case 3 => // Two Grouping Columns
        val ints = new Array[Int](2)
        Convert.longToInts(next.getLongKey, ints)
        unsafeRowWriter.write(0, ints(0))
        unsafeRowWriter.write(1, ints(1))
        unsafeRowWriter.write(2, value)
      case 4 => // three Grouping Columns
        val ints = new Array[Int](4)
        Convert.longToShorts(next.getLongKey, ints)
        unsafeRowWriter.write(0, ints(0))
        unsafeRowWriter.write(1, ints(1))
        unsafeRowWriter.write(2, ints(2))
        unsafeRowWriter.write(3, value)
      case 5 => // four Grouping Columns
        val ints = new Array[Int](4)
        Convert.longToShorts(next.getLongKey, ints)
        unsafeRowWriter.write(0, ints(0))
        unsafeRowWriter.write(1, ints(1))
        unsafeRowWriter.write(2, ints(2))
        unsafeRowWriter.write(3, ints(3))
        unsafeRowWriter.write(4, value)
      case _ =>
        throw new RuntimeException(s"Output row length: $numFields is not supported.")
    }


    unsafeRow.setTotalSize(bufferHolder.totalSize())
    unsafeRow.copy()
  }
}

class Long2DoubleMCaseAggregationMapIterator(name: String, splitIdx: Int, rowLen: Int) extends Iterator[UnsafeRow] with Logging {
  val map = PartitionDataKeeper.get(name).getAggregationMap(splitIdx).asInstanceOf[(Long2DoubleOpenHashMap, _, _)]._1
  val rawIter = map.long2DoubleEntrySet().fastIterator()
  val numFields = rowLen

  val unsafeRow = new UnsafeRow(numFields)
  val bufferHolder = new BufferHolder(unsafeRow)
  val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, numFields)

  override final def hasNext(): Boolean = {
    rawIter.hasNext
  }

  override final def next(): UnsafeRow = {
    bufferHolder.reset()
    val next = rawIter.next()
    val value = next.getDoubleValue // .round.toInt
//    val ints = new Array[Int](2)
    // One Grouping Column
    // unsafeRowWriter.write(0, next.getLongKey.asInstanceOf[Int])
    // unsafeRowWriter.write(1, next.getDoubleValue)

    numFields match {
      case 2 => // One Grouping Column
        unsafeRowWriter.write(0, next.getLongKey.asInstanceOf[Int])
        unsafeRowWriter.write(1, value)
      case 3 => // Two Grouping Columns
        val ints = new Array[Int](2)
        Convert.longToInts(next.getLongKey, ints)
        unsafeRowWriter.write(0, ints(0))
        unsafeRowWriter.write(1, ints(1))
        unsafeRowWriter.write(2, value)
      case 4 => // three Grouping Columns
        val ints = new Array[Int](4)
        Convert.longToShorts(next.getLongKey, ints)
        unsafeRowWriter.write(0, ints(0))
        unsafeRowWriter.write(1, ints(1))
        unsafeRowWriter.write(2, ints(2))
        unsafeRowWriter.write(3, value)
      case 5 => // four Grouping Columns
        val ints = new Array[Int](4)
        Convert.longToShorts(next.getLongKey, ints)
        unsafeRowWriter.write(0, ints(0))
        unsafeRowWriter.write(1, ints(1))
        unsafeRowWriter.write(2, ints(2))
        unsafeRowWriter.write(3, ints(3))
        unsafeRowWriter.write(4, value)
      case _ =>
        throw new RuntimeException(s"Output row length: $numFields is not supported.")
    }

    unsafeRow.setTotalSize(bufferHolder.totalSize())

    // println("$$ " + unsafeRow)
    unsafeRow.copy()
  }
}