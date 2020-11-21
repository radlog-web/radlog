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

import edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.Hosts
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchange}
import org.apache.spark.sql.execution.metric.SQLMetrics

// scalastyle:off line.size.limit
/**
 * Produce [[RecAggrDeltaRDD]] that aggregates the current iteration's results
 * and will be the input of the next iteration.
 *
 * Performs a shuffle that will result in the desired `newPartitioning`.
 * Similar to [[org.apache.spark.sql.execution.exchange.ShuffleExchange]], but don't support ExchangeCoordinator
 */
case class RecAggrExchange(
    name: String,
    mAggr: MonotonicAggregate,
    var initAggrMap: Boolean,
    newPartitioning: Partitioning,
    child: SparkPlan) extends Exchange {

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"))

  override def output: Seq[Attribute] = mAggr.resultExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = newPartitioning

  private val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val shuffleDependency = ShuffleExchange.prepareShuffleDependency(
      child.execute(), child.output, newPartitioning, serializer)
    val rdd = new RecAggrDeltaRDD(name, mAggr, initAggrMap, shuffleDependency)
    initAggrMap = false
    rdd
  }

  override def simpleString: String = {
    val allAggregateExpressions = mAggr.aggregateExpressions
    val keyString = mAggr.groupingExpressions.mkString("[", ",", "]")
    val functionString = allAggregateExpressions.mkString("[", ",", "]")
    val outputString = output.mkString("[", ",", "]")
    val iterTypeString = mAggr.aggrIterType.name
    s"RecAggrExchange(key=$keyString, functions=$functionString, output=$outputString, iterType=$iterTypeString) $newPartitioning"
  }
}

class RecAggrDeltaRDD(
    name: String,
    mAggr: MonotonicAggregate,
    initAggrMap: Boolean,
    var dependency: ShuffleDependency[Int, InternalRow, InternalRow])
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

    val s1 = System.currentTimeMillis()

//    println("#######################")
//    println(mAggr.kddmodel)
    val isNewAggr = if (mAggr.aggrType == "new") true else false

    val resultIter = mAggr.aggrIterType match {
      case SumAggrIterType =>
        new SumAggregationIterator(mAggr, iter)
      case PrimitiveMSumAggrIterType => if (isNewAggr) new PrimitiveMCaseAggregationIteratorNew(name, mAggr, iter, split.index, create = true)
        else new PrimitiveMCaseAggregationIterator(name, mAggr, iter, split.index, create = true)
      case PrimitiveMCountAggrIterType => if (isNewAggr) new PrimitiveMCaseAggregationIteratorNew(name, mAggr, iter, split.index, create = true)
        else new PrimitiveMCaseAggregationIterator(name, mAggr, iter, split.index, create = true)
      case PrimitiveAggrIterType => if (isNewAggr) new PrimitiveMCaseAggregationIteratorNew(name, mAggr, iter, split.index, create = true)
        else new PrimitiveMCaseAggregationIterator(name, mAggr, iter, split.index, create = true)
      case _ =>
        new TungstenMonotonicAggregationIterator(
          name,
          mAggr.groupingExpressions,
          mAggr.aggregateExpressions,
          mAggr.aggregateAttributes,
          mAggr.initialInputBufferOffset,
          mAggr.resultExpressions,
          (expressions, inputSchema) =>
            mAggr.newMutableProjection(expressions, inputSchema, mAggr.subexpressionEliminationEnabled),
          mAggr.child.output,
          iter,
          split.index,
          create = initAggrMap)
    }

     // we omit !hasInput && groupingExpressions.isEmpty situation
    logInfo(s"Split ${split.index} - ${resultIter.getClass.getSimpleName} took: ${System.currentTimeMillis() - s1} ms, " +
      s"Delta size: ${resultIter.deltaSize}, All size: ${resultIter.before} -> ${resultIter.after}")

    resultIter
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}
