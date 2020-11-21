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

package org.apache.spark.scheduler.fixedpoint

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._

class FixedPointJobListener[T](rdd: RDD[T]) extends JobListener {

  val timeout = 50 // millis
  val totalTasks = rdd.partitions.length

  var finishedTasks: Int = 0
  var failure: Option[Exception] = None
  var jobResult : Option[JobResult] = None

  override def taskSucceeded(index: Int, result: Any): Unit = synchronized {
    if (result.isInstanceOf[Boolean]) {
      // true means the deltaS for the partition is not empty
      if (!result.asInstanceOf[Boolean]) finishedTasks += 1

      if (finishedTasks == totalTasks) {
        // Notify any waiting thread that may have called awaitResult
        this.notifyAll()
      }
    } else {
      throw new UnsupportedOperationException(
        "Now only support single-job PSN thus fixpoint task should return boolean result," +
          "but get: " + result)
    }
  }

  override def jobFailed(exception: Exception) {
    synchronized {
      failure = Some(exception)
      this.notifyAll()
    }
  }

  def awaitResult(): JobResult = synchronized {
    while (true) {
      if (failure.isDefined) {
        throw failure.get
      } else if (finishedTasks == totalTasks) {
        return JobSucceeded
      } else {
        this.wait(timeout)
      }
    }
    return null
  }
}
