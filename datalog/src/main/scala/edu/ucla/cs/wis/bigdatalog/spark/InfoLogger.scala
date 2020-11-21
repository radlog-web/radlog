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

import java.io.{File, FileOutputStream, PrintStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.internal.SQLConf

trait KeyValueLogger {
  val ps: PrintStream
  val appName: String
  val enable: Boolean

  private val startDateTime: String = {
    val dtf = DateTimeFormatter.ofPattern("MM/dd HH:mm:ss")
    val now = LocalDateTime.now()
    dtf.format(now)
  }

  val headString: String = startDateTime + " " + appName

  def printHead(): Unit

  def close(): Unit

  def println(): Unit = {
    if (enable) ps.println()
  }

  def println(s: String): Unit = {
    if (enable) ps.println(s)
  }

  def print(s: String): Unit = {
    if (enable) ps.print(s)
  }

  def printKV(key: String, value: String, oneLine: Boolean = false): Unit = {
    if (oneLine) {
      println(s"$key: $value")
    } else {
      println(key)
      println(value)
    }
  }
}

class ConsoleLogger(val appName: String, val enable: Boolean = true) extends KeyValueLogger {
  val ps = java.lang.System.out

  def printHead(): Unit = {
    // do nothing
  }

  override def close(): Unit = {
    // do nothing
  }
}

class FileLogger(val appName: String, fileName: String, val enable: Boolean = true) extends KeyValueLogger {
  val ps = new PrintStream(new FileOutputStream(new File(fileName), true))

  def printHead(): Unit = {
    println(s"\n====== $headString ======")
  }

  override def close(): Unit = {
    ps.close()
  }
}

class ResultLogger(appName: String, fileName: String, enable: Boolean = true)
  extends FileLogger(appName, fileName, enable) {

  var count: Int = 0

  override def printHead(): Unit = {
    print("\n\n" + headString)
  }

  def printKVInline(key: String, value: String): Unit = {
    if (count > 0) print(", ")
    print(s"$key: $value")
    count += 1
  }
}

// `print` methods print to result, info and console
// `log` methods print to info and console
class InfoLogger(val appName: String) {
  private val rowLimit = 20

  private val console = new ConsoleLogger(appName) // everything
  private val info = new FileLogger(appName, ".expInfo.txt") // plan & results
  private val result = new ResultLogger(appName, ".expResult.txt") // execution time

  console.printHead()
  info.printHead()
  result.printHead()

  def println(s: String): Unit = {
    console.println(s)
    info.println(s)
  }

  def printResultSize(count: Long): Unit = {
    val key = "Result Size"
    val cnt = s"$count"
    result.printKVInline(key, cnt)
    console.printKV(key, cnt, oneLine = true)
    info.printKV(key, cnt, oneLine = true)
  }

  // Overall Time Measure
  def printTime[R](name: String, block: => R): R = {
    val t0 = System.currentTimeMillis()
    val ret = block    // call-by-name
    val t1 = System.currentTimeMillis()
    printUsedTime(name, t1 - t0)
    ret
  }

  def printUsedTime(name: String, passedMillis: Long): Unit = {
    val used = s"$passedMillis ms"
    result.printKVInline(name, used)
    console.printKV(name, used, oneLine = true)
    info.printKV(name, used, oneLine = true)
  }

  // Detailed Time Measure
  def logTime[R](name: String, block: => R): R = {
    val t0 = System.currentTimeMillis()
    val ret = block    // call-by-name
    val t1 = System.currentTimeMillis()
    val used = s"${t1 - t0} ms"
    console.printKV(name, used, oneLine = true)
    info.printKV(name, used, oneLine = true)
    ret
  }

  def logSQLConf(sqlConf: SQLConf): Unit = {
    val k = "\n== SQLConf =="
    val v = sqlConf.getAllConfs.toArray.sorted.map{case (key, value) => key + "=" + value}.mkString("\n")
    console.printKV(k, v)
    info.printKV(k, v)
  }

  def logInputSQL(v: String): Unit = {
    console.println(v)
    info.println(v)
  }

  def logObjectText(v: String): Unit = {
    val k = "\n== Datalog ObjectText =="
    console.printKV(k, v)
    info.printKV(k, v)
  }

  def logQuery(v: String): Unit = {
    val k = "\n== Datalog Query =="
    console.printKV(k, v)
    info.printKV(k, v)
  }

  def logOperator(v: String): Unit = {
    val k = "\n== Operator Program =="
    console.printKV(k, v)
    info.printKV(k, v)
  }

  def logPCGTree(v: String): Unit = {
    val k = "\n== Compiled PCG Tree =="
    console.printKV(k, v)
    info.printKV(k, v)
  }


  def logPlan(v: String): Unit = {
    console.println("\n" + v + "\n")
    info.println("\n" + v + "\n")
  }

  def close(): Unit = {
    console.close()
    info.close()
    result.close()
  }
}
