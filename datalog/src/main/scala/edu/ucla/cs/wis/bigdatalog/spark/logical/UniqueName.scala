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

package edu.ucla.cs.wis.bigdatalog.spark.logical

import java.util.concurrent.atomic.AtomicInteger

/**
 * Clique plan uses the same name for variables/alias that are equal, which causes ambiguous resolution issues.
 * For example:
 *   Join (0.A1, 1.A1)
 *     s1(A as A1, B)
 *     s2(A as A1, C)
 *
 * This class generates unique names for the alias.
 */
object UniqueName {

  var uniqueId = new AtomicInteger(1)

  def gen(name: String): String = {
    val prefix = name.takeWhile(c => !c.isDigit)
    prefix + uniqueId.getAndIncrement()
  }

}
