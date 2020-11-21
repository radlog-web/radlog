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

import edu.ucla.cs.wis.bigdatalog.interpreter.Hints

import scala.collection.mutable
import scala.io.Source

case class RaSQLProgram(
  name: String,
  hints: Hints,
  relations: Seq[RelationDefinition],
  sqls: Seq[String]) extends Program {

  def initArgs(opts: Map[String, String]): RaSQLProgram = {
    val _sqls = sqls.map(TemplateProgramArgReplacer.replace(_, opts))
    new RaSQLProgram(name, hints, relations, _sqls)
  }

  def toDatabaseString: String = "database({" + relations.mkString(", ") + "})."
}

object RaSQLProgram {
  private val commentStart = "--"

  def apply(path: String): RaSQLProgram = {
    var hints = new Hints()
    var stat = ""
    val creates = new mutable.ArrayBuffer[String]()
    val sqls = new mutable.ArrayBuffer[String]()

    def process(s: String): Unit = {
      val trimmed = s.trim
      val query = if (trimmed.endsWith(";")) trimmed.dropRight(1) else trimmed
      if (query.toLowerCase.matches("create\\s+table.*")) {
        creates += query
      } else { // treat as SQLs
        sqls += query
      }
    }

    for (line <- Source.fromFile(path).getLines) {
      val l = line.trim
      l match {
        case _ if l.startsWith(commentStart) =>
          if (HintsUtil.isHintComment(commentStart, l)) hints = HintsUtil.extractHints(l)
        case _ if l.endsWith(";") =>
          stat += "\n" + l
          process(stat)
          stat = ""
        case other =>
          stat += "\n" + other
      }
    }

    // last statement may not end with ;
    if (stat.trim != "") process(stat)

    new RaSQLProgram(
      QueryName.extract(path),
      hints,
      RelationDefinition.parseCreates(creates),
      sqls
    )
  }
}
