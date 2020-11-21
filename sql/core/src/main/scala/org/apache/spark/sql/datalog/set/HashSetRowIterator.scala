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

package org.apache.spark.sql.datalog.set

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}

// scalastyle:off line.size.limit
class ObjectHashSetRowIterator(set: ObjectHashSet) extends Iterator[InternalRow] {
  val rawIter = set.iterator()

  override final def hasNext(): Boolean = {
    rawIter.hasNext
  }

  override final def next(): InternalRow = {
    rawIter.next().copy()
  }
}

/**
 * victor: rewrite v1.6 code, apply new logic similar to
 * [[org.apache.spark.sql.execution.datasources.text.TextFileFormat.buildReader]]
 */
class IntKeysHashSetRowIterator(set: IntKeysHashSet) extends Iterator[InternalRow] {
  val rawIter = set.iterator()

  val unsafeRow = new UnsafeRow(1)
  val bufferHolder = new BufferHolder(unsafeRow)
  val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, 1)

  override final def hasNext(): Boolean = rawIter.hasNext

  override final def next(): InternalRow = {
    bufferHolder.reset()
    unsafeRowWriter.write(0, rawIter.next())
    unsafeRow.setTotalSize(bufferHolder.totalSize())
    unsafeRow.copy()
  }
}

/**
 * victor: rewrite v1.6 code, apply new logic similar to
 * [[org.apache.spark.sql.execution.datasources.text.TextFileFormat.buildReader]]
 */
class LongKeysHashSetRowIterator(set: LongKeysHashSet) extends Iterator[InternalRow] {
  val rawIter = set.iterator()
  val numFields = set.schemaInfo.arity

  val unsafeRow = new UnsafeRow(numFields)
  val bufferHolder = new BufferHolder(unsafeRow)
  val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, numFields)

  override final def hasNext(): Boolean = {
    rawIter.hasNext
  }

  override final def next(): InternalRow = {
    bufferHolder.reset()
    val value = rawIter.nextLong()
    if (numFields == 2) {
      unsafeRowWriter.write(0, (value >> 32).toInt)
      unsafeRowWriter.write(1, value.toInt)
    } else {
      unsafeRowWriter.write(0, value)
    }
    unsafeRow.setTotalSize(bufferHolder.totalSize())
    unsafeRow.copy()
  }
}

class TwoColumnLongKeysHashSetRowIterator(set: TwoColumnLongKeysHashSet) extends Iterator[InternalRow] {
  val rawIter = set.iterator()
  val numFields = 2

  val unsafeRow = new UnsafeRow(numFields)
  val bufferHolder = new BufferHolder(unsafeRow)
  val unsafeRowWriter = new UnsafeRowWriter(bufferHolder, numFields)

  override final def hasNext(): Boolean = {
    rawIter.hasNext
  }

  override final def next(): InternalRow = {
    bufferHolder.reset()
    val value = rawIter.nextLong()

    unsafeRowWriter.write(0, (value >> 32).toInt)
    unsafeRowWriter.write(1, value.toInt)

    unsafeRow.setTotalSize(bufferHolder.totalSize())
    unsafeRow.copy()
    // TODO[JIACHENG]: Originally need not to copy due to it is only traverse once, but now we sometimes need to
    // traverse more times, it is better to judge whether need to traverse again or not in SparkPlan
  }
}

object HashSetRowIterator {
  def create(set: HashSet): Iterator[InternalRow] = {
    set match {
      // case set: UnsafeFixedWidthSet => set.iterator().asScala
      case set: IntKeysHashSet => new IntKeysHashSetRowIterator(set)
      case set: LongKeysHashSet => new LongKeysHashSetRowIterator(set)
      case set: TwoColumnLongKeysHashSet => new TwoColumnLongKeysHashSetRowIterator(set)
      case set: ObjectHashSet => new ObjectHashSetRowIterator(set)
    }
  }
}
