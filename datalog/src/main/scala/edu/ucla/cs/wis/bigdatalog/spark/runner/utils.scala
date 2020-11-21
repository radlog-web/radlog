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

import java.io.PrintWriter
import java.util.regex.Pattern

import edu.ucla.cs.wis.bigdatalog.interpreter.Hints
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

// scalastyle:off line.size.limit
// scalastyle:off println
object HintsUtil {
  def isHintComment(commentStart: String, line: String): Boolean = {
    line.substring(commentStart.length).trim.toLowerCase.startsWith("hint")
  }

  def extractHints(line: String): Hints = {
    val startIndex = line.toLowerCase.indexOf("hint") + "hint:".length
    val content = line.substring(startIndex)
    val hints = content.split(",").map(s => s.trim)
    new Hints(hints)
  }
}

object QueryName {
  def extract(path: String): String = path.split("/").last.split("\\.")(0)
}

object TemplateProgramArgReplacer {
  def replace(in: String, options: Map[String, String]): String = {
    // replace any ${var} in datalog, datalogQuery or sqls by actual args passed
    val p = Pattern.compile("\\{(.*?)\\}")
    val m = p.matcher(in)
    var res = in
    while (m.find) {
      val key = m.group(1)
      val regex = "\\{" + key + "\\}"
      options.get(key) match {
        case Some(v) => res = res.replaceFirst(regex, v)
        case _ => throw new IllegalArgumentException(
          s"use -$key option, this arg needs to be replaced in query")
      }
    }
    res
  }
}

object SessionOptions {
  def create(program: Program, argsMap: Map[String, String]): Map[String, String] = {
    val inputPaths = expandInputArg(argsMap, program.relations)
    program.hints.toStringMap.asScala.toMap ++ argsMap ++ inputPaths
  }

  private def expandInputArg(options: Map[String, String], relations: Seq[RelationDefinition]): Map[String, String] = {
    // attach dataPath specified through 'input' arg to relation name arg
    options.get("input").map(dataPath =>
      if (relations.length == 1) {
        val name = relations.head.name
        if (options.contains(name)) {
          throw new IllegalArgumentException(s"'-input' option cannot be used if input specified through '-$name'")
        } else {
          (name, dataPath)
        }
      } else {
        throw new IllegalArgumentException("'-input' option cannot be used if not one input relation")
      }
    ).toMap
  }
}

object OutputFileWriter extends Logging {
  def write(outputFile: Option[String], results: Array[Row]): Unit = {
    if (outputFile.isDefined) {
      val tsvFileName = outputFile.get match {
        case s if !s.endsWith("tsv") =>
          val renamed = s + ".tsv"
          logWarning(s"Rename provided output fileName $s to $renamed")
          renamed
        case s => s
      }
      logWarning("Saving results to " + tsvFileName)
      val writer = new PrintWriter(tsvFileName)
      results.map(_.mkString("\t")).foreach(writer.println)
      writer.close()
    }
  }
}

object AppName {
  def build(name: String, relations: Seq[RelationDefinition], options: Map[String, String]): String = {
    val fileNames = relations.map(r => {
      r.name + "=" + options(r.name).split("/").last.split("\\.").head
    })

    val hideOptions = relations.map(_.name) ++ Seq("master", "sleep", "program")
    val showOptions = options.filter(e => !hideOptions.contains(e._1)).map(e => e._1 + "=" + e._2).toSeq

    // WARN: we do not add any confEntry which is set through --conf to appName
    // as we recommend using program options to set these confs
    (Seq(name) ++ fileNames ++ showOptions).mkString("/")
  }
}
