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

package edu.ucla.cs.wis.bigdatalog.spark.execution

import org.apache.spark.SparkEnv
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Performs (external) sorting.
 *
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 */
case class SortReuseExec(
    name: String,
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  private val enableRadixSort = sqlContext.conf.enableRadixSort

  override lazy val metrics = Map(
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "sort time"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  def createSorter(): UnsafeExternalRowSorter = {
    val ordering = newOrdering(sortOrder, output)

    // The comparator for comparing prefix
    val boundSortExpression = BindReferences.bindReference(sortOrder.head, output)
    val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

    val canUseRadixSort = enableRadixSort && sortOrder.length == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

    // The generator for prefix
    val prefixProjection = UnsafeProjection.create(Seq(SortPrefix(boundSortExpression)))
    val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
      override def computePrefix(row: InternalRow): Long = {
        prefixProjection.apply(row).getLong(0)
      }
    }

    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    val sorter = new UnsafeExternalRowSorter(
      schema, ordering, prefixComparator, prefixComputer, pageSize, canUseRadixSort)

    sorter
  }

  private var childExecuted: RDD[InternalRow] = _

  protected override def doExecute(): RDD[InternalRow] = {
    // by-pass exchange - child is exchange operator
    if (childExecuted == null) {
      childExecuted = child.execute()
      new InitSortReuseInMemoryRDD(name, childExecuted, () => { createSorter() })
        .asInstanceOf[RDD[InternalRow]]
    } else {
      new SortReuseInMemoryRDD(name, childExecuted, () => { throw new RuntimeException("should not be called") })
        .asInstanceOf[RDD[InternalRow]]
    }
  }

  override def usedInputs: AttributeSet = AttributeSet(Seq.empty)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  // Name of sorter variable used in codegen.
  private var sorterVariable: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    if (true) throw new UnsupportedOperationException("SortReuseExec does not support Codegen")

    val needToSort = ctx.freshName("needToSort")
    ctx.addMutableState("boolean", needToSort, s"$needToSort = true;")

    // Initialize the class member variables. This includes the instance of the Sorter and
    // the iterator to return sorted rows.
    val thisPlan = ctx.addReferenceObj("plan", this)
    sorterVariable = ctx.freshName("sorter")
    ctx.addMutableState(classOf[UnsafeExternalRowSorter].getName, sorterVariable,
      s"$sorterVariable = $thisPlan.createSorter();")
    val metrics = ctx.freshName("metrics")
    ctx.addMutableState(classOf[TaskMetrics].getName, metrics,
      s"$metrics = org.apache.spark.TaskContext.get().taskMetrics();")
    val sortedIterator = ctx.freshName("sortedIter")
    ctx.addMutableState("scala.collection.Iterator<UnsafeRow>", sortedIterator, "")

    val addToSorter = ctx.freshName("addToSorter")
    ctx.addNewFunction(addToSorter,
      s"""
        | private void $addToSorter() throws java.io.IOException {
        |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
        | }
      """.stripMargin.trim)

    // The child could change `copyResult` to true, but we had already consumed all the rows,
    // so `copyResult` should be reset to `false`.
    ctx.copyResult = false

    val outputRow = ctx.freshName("outputRow")
    val peakMemory = metricTerm(ctx, "peakMemory")
    val spillSize = metricTerm(ctx, "spillSize")
    val spillSizeBefore = ctx.freshName("spillSizeBefore")
    val sortTime = metricTerm(ctx, "sortTime")
    s"""
       | if ($needToSort) {
       |   long $spillSizeBefore = $metrics.memoryBytesSpilled();
       |   $addToSorter();
       |   $sortedIterator = $sorterVariable.sort();
       |   $sortTime.add($sorterVariable.getSortTimeNanos() / 1000000);
       |   $peakMemory.add($sorterVariable.getPeakMemoryUsage());
       |   $spillSize.add($metrics.memoryBytesSpilled() - $spillSizeBefore);
       |   $metrics.incPeakExecutionMemory($sorterVariable.getPeakMemoryUsage());
       |   $needToSort = false;
       | }
       |
       | while ($sortedIterator.hasNext()) {
       |   UnsafeRow $outputRow = (UnsafeRow)$sortedIterator.next();
       |   ${consume(ctx, null, outputRow)}
       |   if (shouldStop()) return;
       | }
     """.stripMargin.trim
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    s"""
       |${row.code}
       |$sorterVariable.insertRow((UnsafeRow)${row.value});
     """.stripMargin
  }
}
