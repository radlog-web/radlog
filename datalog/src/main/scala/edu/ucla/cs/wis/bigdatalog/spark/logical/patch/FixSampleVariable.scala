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

package edu.ucla.cs.wis.bigdatalog.spark.logical.patch

import edu.ucla.cs.wis.bigdatalog.compiler.ParseResultRewriter.{SAMPLE_FUNCTION_NAME, SAMPLE_VAR_SUFFIX}
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.{AggregateArgument, AliasedArgument}
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator._

import scala.collection.JavaConversions._

object FixSampleVariable {
  private val aliasPrefix = "MSAggr_"

  def findVariable(root: Operator, name: String): Option[Variable] = {
    if (root == null) {
      return None
    }

    val argList = root.getArgumentsAsArrayList
    val foundInArgs = argList flatMap {
      case v: Variable if v.getName == name => Some(v)
      case _ => None
    }

    // visit children
    var children = root.getChildren.toSeq
    root match {
      case c: CliqueOperator =>
        children = Seq(c.getExitRulesOperator, c.getRecursiveRulesOperator)
      case _ => // pass
    }
    val foundInChildrenOps = children.flatMap(c => findVariable(c, name))
    val res = foundInArgs ++ foundInChildrenOps
    // If multiple vars found, we only need to return the first.
    // This is because even if two vars with the same name are different objects,
    // they should represent the same var semantically within a Datalog Rule.
    res.headOption
  }

  def fix(root: Operator): Unit = {
    if (root == null) {
      return
    }
    // victor: [Hack] we need to fix the SampleVariable reference
    var i = 0
    var sampleVarCnt = 0
    val argList = root.getArgumentsAsArrayList
    for (t <- argList) {
      t match {
        case v: Variable if v.getName.endsWith(SAMPLE_VAR_SUFFIX) =>
          // operators other than AGGREGATE_FS, i.e. Project, can also contain sample variable
          val opType = root.getOperatorType
          val originalVar = findVariable(root, v.getName.dropRight(SAMPLE_VAR_SUFFIX.length))
          originalVar match {
            case Some(ov) if (opType == OperatorType.AGGREGATE_FS) || (opType == OperatorType.AGGREGATE) =>
              // replace sample variable by sample aggregate function in AGGREGATE_FS
              sampleVarCnt += 1
              if (sampleVarCnt > 1) throw new RuntimeException(s"More than one sample variable exists in Arguments: $argList")
              val argument = new AggregateArgument(SAMPLE_FUNCTION_NAME, ov)
              val alias = new Variable(aliasPrefix + sampleVarCnt, ov.getDataType)
              argList.set(i, new AliasedArgument(argument, alias))
            case Some(ov) =>
              // replace sample variable by originalVar in other operators
              argList.set(i, ov)
            case _ =>
              throw new RuntimeException("Cannot find original variable for sample var: " + v.getName)
          }
        case _ => // pass
      }
      i += 1
    }

    // fix children
    var children = root.getChildren
    root match {
      case c: CliqueOperator =>
        children = Array(c.getExitRulesOperator, c.getRecursiveRulesOperator)
      case _ => // pass
    }
    children.foreach(fix)
  }
}
