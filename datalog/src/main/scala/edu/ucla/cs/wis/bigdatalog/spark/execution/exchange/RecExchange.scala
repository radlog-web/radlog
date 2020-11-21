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

package edu.ucla.cs.wis.bigdatalog.spark.execution.exchange

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.Hosts
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.datalog.set.{HashSetRowIterator, ObjectHashSet, SetOp}
import org.apache.spark.sql.datalog.{IntraCount, PartitionDataKeeper}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchange}
import org.apache.spark.sql.execution.metric.SQLMetrics

// scalastyle:off line.size.limit
/**
 * Produce [[RecDeltaRDD]] that will be the input of the next iteration.
 *
 * Performs a shuffle that will result in the desired `newPartitioning`.
 * Similar to [[org.apache.spark.sql.execution.exchange.ShuffleExchange]], but don't support ExchangeCoordinator
 */
case class RecExchange(
    name: String,
    var initSets: Boolean,
    var newPartitioning: Partitioning,
    child: SparkPlan) extends Exchange {

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"))

  override def outputPartitioning: Partitioning = newPartitioning

  private val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val shuffleDependency = ShuffleExchange.prepareShuffleDependency(
      child.execute(), child.output, newPartitioning, serializer)
    val rdd = new RecDeltaRDD(name, initSets, output.size, shuffleDependency)
    initSets = false
    rdd
  }
}

/**
 * [[RecDeltaRDD]] is a post-shuffled RDD, which is similar to [[ShuffledRowRDD]].
 * Thus it is served as the starting RDD of a (next) stage and its `compute` method
 * begins by fetching shuffled data (MapOutput) from the previous stage.
 */
class RecDeltaRDD(name: String, initSets: Boolean, outputLength: Int, var dependency: ShuffleDependency[Int, InternalRow, InternalRow])
  extends RDD[InternalRow](dependency.rdd.context, Nil) {

  private[this] val numPreShufflePartitions = dependency.partitioner.numPartitions

  private[this] val partitionStartIndices: Array[Int] = (0 until numPreShufflePartitions).toArray

  private[this] val part: Partitioner =
    new CoalescedPartitioner(dependency.partitioner, partitionStartIndices)

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override val partitioner: Option[Partitioner] = Some(part)

  override def getPartitions: Array[Partition] = {
    assert(partitionStartIndices.length == part.numPartitions)
    Array.tabulate[Partition](partitionStartIndices.length) { i =>
      val startIndex = partitionStartIndices(i)
      val endIndex =
        if (i < partitionStartIndices.length - 1) {
          partitionStartIndices(i + 1)
        } else {
          numPreShufflePartitions
        }
      new ShuffledRowRDDPartition(i, startIndex, endIndex)
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val pinRDDHostLimit = conf.getInt("spark.datalog.pinRDDHostLimit", 0)
    if (pinRDDHostLimit > 0) {
      val host = Hosts.getPinnedHost(split, pinRDDHostLimit)
      logDebug(s"$split has pinned host: $host")
      Seq(host)
    } else {
      Nil
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val shuffledRowPartition = split.asInstanceOf[ShuffledRowRDDPartition]
    // The range of pre-shuffle partitions that we are fetching at here is
    // [startPreShufflePartitionIndex, endPreShufflePartitionIndex - 1].
    val reader =
      SparkEnv.get.shuffleManager.getReader(
        dependency.shuffleHandle,
        shuffledRowPartition.startPreShufflePartitionIndex,
        shuffledRowPartition.endPreShufflePartitionIndex,
        context)

    // victor
    val iter: Iterator[InternalRow] = reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
    val dataKeeper = PartitionDataKeeper.get(name)

    var tempCount = dataKeeper.getTempCount(split.index)
    if(tempCount == null) {
      val occ = name match {
        case "npat" => 1
        case "fdistc" => 1
        case "cntdec" => 1
        case "cntdecy" => 1
        case "uns" => 1
        case "fovlp" => 1
        case _ => 4
      }
      tempCount = new IntraCount(occ)
      dataKeeper.setTempCount(split.index, tempCount)
    }

    if (initSets) {
      // if no exit rule plan is executed, we init sets here, same as [[ExitDeltaRDD]]
      dataKeeper.initDeltaAllRDDHashSets(split.index, iter, outputLength)
      val delta_rdd = dataKeeper.getDeltaRDDData(split.index)
      delta_rdd
    } else {
      // obtain existing allRDD set for diff and union, assuming it is already initialized
      val allRDDSet = dataKeeper.getAllRDDData(split.index)

      val s1 = System.currentTimeMillis()
      val diffSet = SetOp.diff(iter, allRDDSet)
      logInfo(s"Split ${split.index} - Iterate took: ${System.currentTimeMillis() - s1} ms, DiffSet size: ${diffSet.size}")

      val s2 = System.currentTimeMillis()
      val beforeSize = allRDDSet.size()
      allRDDSet.union(diffSet)
      logInfo(s"Split ${split.index} - Union took: ${System.currentTimeMillis() - s2} ms, AllRDDSet size: $beforeSize -> ${allRDDSet.size()}")


      var previousDeltaSet = dataKeeper.getNonLinearSet(split.index)
      if (previousDeltaSet == null) {
        previousDeltaSet = new ObjectHashSet()
        dataKeeper.setNonLinearSet(split.index, previousDeltaSet)
      }

      tempCount.incUsedCount()
      val recursiveRelations = Array("npat", "fdistc", "cntdec", "cntdecy", "uns", "fovlp") // "pattern", "distc", "fdistc", "cntdecyn", "cntdecy", "cntdec", "gini"

      if (recursiveRelations.contains(name)) {
        if (diffSet.size() == 0 && tempCount.isLastUsedInIteration) {
          HashSetRowIterator.create(previousDeltaSet)
        } else {
          previousDeltaSet.clear()
          previousDeltaSet.union(diffSet)
          HashSetRowIterator.create(diffSet)
        }
      } else {
        HashSetRowIterator.create(diffSet)
      }

      // TODO: HERE EXITS READ TWICE PROBLEM CANNOT FIX SIMPLY DUE TO USED IN BOTH TWO CASES (tc_nl company_control)
//      HashSetRowIterator.create(diffSet)
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}
