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

package org.apache.spark.scheduler

import org.apache.spark.Partition

object Hosts {
  private val workers = Map(
    "scai02" -> "131.179.64.21",  // 02
    "scai03" -> "131.179.64.22",  // 03
    "scai04" -> "131.179.64.23",  // 04
    "scai05" -> "131.179.64.26",  // 05
    "scai06" -> "131.179.64.28",  // 06
    "scai07" -> "131.179.64.29",  // 07
    "scai08" -> "131.179.64.30",  // 08
    "scai09" -> "131.179.64.31",  // 09
    "scai10" -> "131.179.64.32",  // 10
    "scai11" -> "131.179.64.33",  // 11
    "scai12" -> "131.179.64.34",  // 12
    "scai13" -> "131.179.64.35",  // 13
    "scai14" -> "131.179.64.36",  // 14
    "scai15" -> "131.179.64.37",  // 15
    "scai16" -> "131.179.64.38"   // 16
  )

  private val workersIP = workers.values.toList

  def toPinnedHostIP(address: String): String = {
    val key = address.toLowerCase.split("\\.").head
    val ip = if (key.startsWith("scai")) {
      workers.get(key) match {
        case Some(v) => v
        case _ => throw new RuntimeException(s"IP not found for address: $address")
      }
    } else {
      address
    }
    TaskLocation.pinnedHostTag + ip
  }

  /**
   * Check DAGScheduler.getPreferredLocsInternal
   */
  def getPinnedHost(split: Partition, lim: Int): String = {
    val limit = Math.min(workersIP.length, lim)
    val hostIndex = split.index % limit
    TaskLocation.pinnedHostTag + workersIP(hostIndex)
  }
}
