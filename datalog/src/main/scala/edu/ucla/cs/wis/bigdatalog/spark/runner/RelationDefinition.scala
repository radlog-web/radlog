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

package edu.ucla.cs.wis.bigdatalog.spark.runner

import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

class RelationDefinition(val name: String, val columns: Seq[(String, String)]) {
  private def getDataType(s: String): DataType = {
    s.toLowerCase() match {
      case "integer" => org.apache.spark.sql.types.IntegerType
      case "long" => org.apache.spark.sql.types.LongType
      case "float" => org.apache.spark.sql.types.FloatType
      case "double" => org.apache.spark.sql.types.DoubleType
      case "string" => org.apache.spark.sql.types.StringType
      case _ => throw new UnsupportedOperationException(s"Unsupported Type: $s")
    }
  }

  def getSchema: StructType = {
    StructType(columns.map(e => {
      StructField(e._1, getDataType(e._2))
    }))
  }

  override def toString: String = {
    name + "(" + columns.map(pair => s"${pair._1}: ${pair._2}").mkString(", ") + ")"
  }
}

object RelationDefinition {
  def parseCreates(creates: Seq[String]): Seq[RelationDefinition] = {
    creates.map(l => {
      val leftPar = l.indexOf("(")
      val rightPar = l.indexOf(")")
      // name is extracted as the last word before the leftPar
      val name = l.substring(0, leftPar).split(" ").filter(_ != "").last
      val columnExtract = l.substring(leftPar + 1, rightPar)
      val columns = columnExtract.split(",").map(s => {
        val nameType = s.split(" ").filter(_ != "")
        (nameType(0).trim, nameType(1).trim)
      }).toSeq
      new RelationDefinition(name, columns)
    })
  }

  def parseDBString(dbString: String): Seq[RelationDefinition] = {
    val defs = new ArrayBuffer[RelationDefinition]()
    // l contains relations between database({ ... }).
    var l = dbString.substring(dbString.indexOf("{") + 1, dbString.indexOf("}")).trim
    while (l != "") {
      val leftPar = l.indexOf("(")
      val rightPar = l.indexOf(")")
      // name is extracted as the string between the right par of previous relation
      // and left par of the current relation, be careful to drop the comma separator
      val name = l.substring(0, leftPar).trim.dropWhile(_ == ',').trim
      val columnExtract = l.substring(leftPar + 1, rightPar)
      val columns = columnExtract.split(",").map(s => {
        val nameType = s.split(":")
        (nameType(0).trim, nameType(1).trim)
      }).toSeq
      defs += new RelationDefinition(name, columns)
      // remove all seen chars
      l = l.substring(rightPar + 1)
    }
    defs
  }
}
