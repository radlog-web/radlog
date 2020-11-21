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

package edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates

trait AggrIter {
  val deltaSize: Int
  val before: Int
  val after: Int
}

trait AggrIterType {
  val name: String
}

case object SumAggrIterType extends AggrIterType {
  override val name = "NonMonotonic-Sum"
}

case object PrimitiveMSumAggrIterType extends AggrIterType {
  override val name = "MSum"
}

case object PrimitiveMCountAggrIterType extends AggrIterType {
  override val name = "MCount"
}

case object PrimitiveAggrIterType extends AggrIterType {
  override val name = "Primitive"
}

case object TungstenMAggrIterType extends AggrIterType {
  override val name = "Tungsten"
}
