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

package org.apache.spark.sql.execution.exchange

import java.io._

import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.SparkPlan

/**
 * We borrow following code piece from [[SparkPlan.executeCollect]]
 * for ease of packed broadcast implementation
 */
object PackedBroadcastUtil {

  /**
   * Packing the UnsafeRows into byte array for faster serialization.
   * The byte arrays are in the following format:
   * [size of row] [bytes of UnsafeRow] [bytes of UnsafeRow] ...
   *
   * UnsafeRow is highly compressible (at least 8 bytes for any column), the byte array is also
   * compressed.
   *
   * victor: unlike [[SparkPlan.getByteArrayRdd]], we do not write size of each row
   * thus it should be provided and fixed for all rows
   */
  def getByteArrayRdd(plan: SparkPlan): RDD[Array[Byte]] = {
    plan.execute().mapPartitionsInternal { iter =>
      var count = 0
      val buffer = new Array[Byte](4 << 10)  // 4K
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val bos = new ByteArrayOutputStream()
      val out = new DataOutputStream(codec.compressedOutputStream(bos))
      var first = true
      while (iter.hasNext) {
        val row = iter.next().asInstanceOf[UnsafeRow]
        if (first) {
          out.writeInt(row.getSizeInBytes)
          first = false
        }
        row.writeToStream(out, buffer)
        count += 1
      }
      out.flush()
      out.close()
      // result iterator only contains one elem, which is the byteArray
      Iterator(bos.toByteArray)
    }
  }
}

/**
 * Decode the byte arrays back to UnsafeRows and put them into buffer.
 */
class PackedBroadcastRowIterator(bytes: Array[Byte], nFields: Int)
  extends Iterator[InternalRow] {

  private val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
  val bis = new ByteArrayInputStream(bytes)
  val ins = new DataInputStream(codec.compressedInputStream(bis))

  var bs: Array[Byte] = _
  var row: UnsafeRow = _
  var nextRow: UnsafeRow = _
  var first = true

  private def bufferNext(): UnsafeRow = {
    try {
      if (first) {
        val sizeOfRow = ins.readInt()
        bs = new Array[Byte](sizeOfRow)
        row = new UnsafeRow(nFields)
        row.pointTo(bs, sizeOfRow)
        first = false
      }
      ins.readFully(bs)
      nextRow = row
      nextRow
    } catch {
      case _: EOFException => null
    }
  }

  override final def hasNext: Boolean = {
    (nextRow != null) || (bufferNext() != null)
  }

  override final def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException

    val result = nextRow
    nextRow = null
    result
  }
}

class IteratorOfIterators(iters: Iterator[PackedBroadcastRowIterator])
  extends Iterator[InternalRow] {

  var curr: Iterator[InternalRow] = Iterator.empty

  override def hasNext: Boolean = {
    preload()
    curr.hasNext
  }

  private def preload(): Unit = {
    if (!curr.hasNext) {
      if (iters.hasNext) {
        curr = iters.next()
        preload()
      }
    }
  }

  override def next(): InternalRow = {
    preload()
    curr.next()
  }
}
