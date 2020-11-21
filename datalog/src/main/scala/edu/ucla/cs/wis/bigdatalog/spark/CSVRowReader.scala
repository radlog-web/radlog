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

package edu.ucla.cs.wis.bigdatalog.spark

import java.sql.Date
import java.util.Random

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object CSVRowReader {
  def main(args: Array[String]): Unit = {
    // performance tests
    val sep = ' '
    val numOfFields = args(0).toInt
    val inputSize = args(1).toInt
    val print = args(2).toBoolean
    val runs = args(3).toInt

    // init
    val numOfDataColumns = numOfFields - 1 // test auto add weight
    val rnd = new Random()
    val input = (0 until inputSize).map { _ =>
      (0 until numOfDataColumns).map { _ =>
        val i = rnd.nextInt()
        if (i < 0) -i else i
      }.mkString(sep.toString)
    }

    val schema = StructType((0 until numOfFields).map(i => StructField("c" + i, IntegerType)))

    // tests
    (0 until runs).foreach { _ =>
      val reader = new CSVRowReader(sep, new GenericMutableRow(numOfFields), schema)
      val s1 = System.nanoTime()
      (0 until inputSize).foreach(i => {
        val res = reader.updateRow(input(i))
        if (print) println(res)
      })
      println("=============")
      val s2 = System.nanoTime()
      (0 until inputSize).foreach(i => {
        val res = reader.updateRowUsingSplit(input(i))
        if (print) println(res)
      })
      val s3 = System.nanoTime()
      System.out.println("fast: " + (s2 - s1) / 1000000 + " usingSplit: " + (s3 - s2) / 1000000)
    }
  }
}

class CSVRowReader(sep: Char, row: GenericMutableRow, schema: StructType, addWeight: Boolean = true) {
  private val schemaFieldsLength = schema.fields.length
  private lazy val fs: Array[(String => Any)] = schema.map(s => createStringColumnConverter(s.dataType)).toArray

  /**
   * WARN: support only non-negative int
   */
  def updateRow(line: String): GenericMutableRow = {
    var columnIndex = 0
    var index = 0
    var c = '0'
    val len = line.length
    while (true) {
      // we assume each line is non-empty
      var num = 0
      while (index < len && {c = line.charAt(index); c != sep}) {
        num = (c - '0') + num * 10
        index += 1
      }
      // process num
      row.update(columnIndex, num)
      if (index == len) { // reach the end of line
        if (columnIndex == schemaFieldsLength - 1) { // filled all columns
          return row
        } else if (addWeight && columnIndex == schemaFieldsLength - 2) { // missing one column
          row.update(columnIndex + 1, 1)
          return row
        } else {
          throw new RuntimeException(s"Data size does not match schema size")
        }
      } else if (columnIndex == schemaFieldsLength - 1) { // not reach the end of line, but filled all columns
        return row
      }
      // maybe need to change to while loop to skip continuous sep chars
      columnIndex += 1
      index += 1
    }
    row
  }

  def updateRowUsingSplit(line: String): GenericMutableRow = {
    var i: Int = 0
    val splitLine = line.split(sep)
    while (i < row.numFields) {
      if (addWeight && i == splitLine.length) {
        row.update(i, 1)
      } else if (i < splitLine.length) {
        row.update(i, fs(i)(splitLine(i)))
      } else {
        throw new RuntimeException(s"Data size does not match schema size")
      }
      i += 1
    }
    row
  }

  private def createStringColumnConverter(dataType: org.apache.spark.sql.types.DataType): (String => Any) = {
    dataType match {
      case IntegerType => ((item: String) => item.toInt)//(item)
      case LongType => ((item: String) => item.toLong)
      case StringType => ((item: String) => UTF8String.fromString(item))
      case DoubleType => ((item: String) => item.toDouble)
      case FloatType => ((item: String) => item.toFloat)
      case ShortType => ((item: String) => item.toShort)
      case ByteType => ((item: String) => item.toByte)
      case DateType => ((item: String) => DateTimeUtils.fromJavaDate(Date.valueOf(item)))
      case _ => throw new Exception("Unsupported datatype being converted!")
    }
  }
}
