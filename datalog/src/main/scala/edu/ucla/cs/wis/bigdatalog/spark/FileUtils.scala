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

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

object FileUtils extends Logging {

  // returns a RDD[InternalRow] that will load, parse and convert (into InternalRow) the given filePath
  def loadRowRDDFromFile(sparkSession: SparkSession, filePath: String, schema: StructType, numPartitions: Int): RDD[InternalRow] = {
    assert(numPartitions > 0)

    val ext = filePath.split("\\.").last
    val sep = ext match {
      case "csv" => ','
      case "tsv" => '\t'
      case "txt" => ' '
      case _ => throw new UnsupportedOperationException(s"File extension: $ext")
    }
    val fastMode = schema.forall(_.dataType == IntegerType)
    if (fastMode) {
      logWarning("Use FastMode in row reader as all fields are IntegerType")
    } else {
      logWarning("Use String.split in row reader which may be very slow")
    }

    // .coalesce(numPartitions)
    // .filter(line => !line.trim.isEmpty && (line(0) != '%'))
    sparkSession.sparkContext.textFile(filePath, numPartitions).mapPartitions { iter =>
      // we will use 1 row object and hope the caller copies out of it
      // this should have the same behavior as the Catalyst operators pipelining
      val row = new GenericMutableRow(schema.length)
      val intRowReader = new CSVRowReader(sep, row, schema)
      if (fastMode) {
        iter.map(intRowReader.updateRow)
      } else { // TODO: fast ways to split other types
        iter.map(intRowReader.updateRowUsingSplit)
      }
    }
  }

  def readDataSource(sparkSession: SparkSession, filePath: String, schema: StructType): DataFrame = {
    val ext = filePath.split("\\.").last
    val sep = ext match {
      case "csv" => ","
      case "tsv" => "\t"
      case "txt" => " "
      case _ => throw new UnsupportedOperationException(s"File extension: $ext")
    }
    sparkSession.read.format("csv")
      .option("sep", sep) // CSVOptions
      .option("addWeight", true)
      .schema(schema)
      .load(filePath)
  }

}
