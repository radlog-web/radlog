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

package org.apache.spark.sql.datalog

class IntraCount(occurrence: Int = 4) {
  var usedCount : Int = 0 // BECAUSE BEFORE LOOP IERATION IT WILL EXECUTE ONCE
  //  var emptyCount : Int = 0
  val totalOccurrence: Int = occurrence // TODO: FIX HERE SINCE this means the total occurrence of table in one iteration

  def incUsedCount(): Unit = {
    usedCount += 1
  }

  def getUsedCount(): Int = usedCount

  def isLastUsedInIteration: Boolean = {
    usedCount % totalOccurrence == 0
  }

  //    def incEmptyCount(): Unit = {
  //      emptyCount += 1
  //    }
  //
  //    def clrEmptyCount(): Unit = {
  //      emptyCount = 0
  //    }
  //
}
