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

package edu.ucla.cs.wis.bigdatalog.spark.ramsql

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}

object ExprUtils {
  def testFun(f: UnresolvedFunction): Boolean = {
    f match {
      case uf @ UnresolvedFunction(FunctionIdentifier("max", _), children, _) => true
      case uf @ UnresolvedFunction(FunctionIdentifier("min", _), children, _) => true
      case uf @ UnresolvedFunction(FunctionIdentifier("count", _), children, _) => true
      case uf @ UnresolvedFunction(FunctionIdentifier("sum", _), children, _) => true
      case other => throw new RuntimeException(other + " function not supported in syntax.")
    }
  }

  // TODO: we ignore some cases such as binary exprs, a complete test should include these
  def containsMaggr(expr: Expression): Boolean = {
    expr match {
      case Alias(c, _) => containsMaggr(c)
      case f: UnresolvedFunction => testFun(f)
      case _ => false
    }
  }
}
