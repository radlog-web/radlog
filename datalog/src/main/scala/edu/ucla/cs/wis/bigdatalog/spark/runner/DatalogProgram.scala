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

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

trait Program {
  def name: String
  def relations: Seq[RelationDefinition]
  def hints: Hints
}

case class DatalogProgram(
  name: String,
  hints: Hints,
  relations: Seq[RelationDefinition],
  rules: Seq[String],
  query: String) extends Program {

  def initArgs(opts: Map[String, String]): DatalogProgram = {
    val _rules = rules.map(TemplateProgramArgReplacer.replace(_, opts))
    val _query = TemplateProgramArgReplacer.replace(query, opts)
    new DatalogProgram(name, hints, relations, _rules, _query)
  }

  def toDatabaseString: String = "database({" + relations.mkString(", ") + "})."
}

object DatalogProgram {
  private val commentStart = "--"

  def apply(path: String): DatalogProgram = {
    var hints = new Hints()
    var stat = ""
    var dbString = ""
    val rules = new ArrayBuffer[String]()
    var query = ""

    for (line <- Source.fromFile(path).getLines) {
      val l = line.trim
      l match {
        case _ if l.startsWith(commentStart) =>
          if (HintsUtil.isHintComment(commentStart, l)) hints = HintsUtil.extractHints(l)
        case _ if l.endsWith(".") =>
          stat = (stat + l).trim
          if (stat.toLowerCase.startsWith("database")) {
            dbString = stat
          } else if (stat.toLowerCase.startsWith("query")) {
            query = stat.substring("query".length).trim
          } else { // treat as datalog rule
            rules += stat
          }
          stat = ""
        case other =>
          stat += other
      }
    }

    new DatalogProgram(
      QueryName.extract(path),
      hints,
      RelationDefinition.parseDBString(dbString),
      rules,
      query
    )
  }
}
