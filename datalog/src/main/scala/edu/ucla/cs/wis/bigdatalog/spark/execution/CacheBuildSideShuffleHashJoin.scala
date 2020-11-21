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

import edu.ucla.cs.wis.bigdatalog.spark.execution.recursion.RecursionBase
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.datalog.PartitionDataKeeper
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.LongType

/* We have resurrected this class from earlier versions of Spark because for
* recursion, caching the build-side of the join and reusing it each iteration is likely
* better than performing a sort-merge-join each iteration.
* TODO - take a look at optimizing sort-merge-join for recursive use.
*/
case class CacheBuildSideShuffleHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryExecNode with HashJoin with CodegenSupport {

  private val cacheBuildSide = SparkSession.getActiveSession.orNull.sparkContext
    .getConf.getBoolean("spark.datalog.shufflehashjoin.cachebuildside", true)

  var cached: Boolean = false

  private lazy val name = {
    val recRelation = streamedPlan.find(node => {node.isInstanceOf[RecursiveRelation] || node.isInstanceOf[RecursionBase]})
    recRelation match {
      case Some(r) => {
        r match {
          case relation: RecursiveRelation => relation.name
          case recursion: RecursionBase => recursion.name
          case _ => throw new RuntimeException("StreamedPlan does not contain RecursiveRelation.")
        }
      }
      case _ => throw new RuntimeException("StreamedPlan does not contain RecursiveRelation.")
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  private def buildHashedRelation(iter: Iterator[InternalRow]): HashedRelation = {
    // victor: comment out metric to resolve null sparkContext issue, not quite sure about the reason,
    // probably due to not doing the closure clean

    // val buildDataSize = longMetric("buildDataSize")
    // val buildTime = longMetric("buildTime")
    val start = System.nanoTime()
    val context = TaskContext.get()
    val relation = HashedRelation(iter, buildKeys, taskMemoryManager = context.taskMemoryManager())
    val hashedRelationBuildTime = (System.nanoTime() - start) / 1000000
    // buildTime += hashedRelationBuildTime
    // buildDataSize += relation.estimatedSize
    logInfo(s"buildHashedRelation took: $hashedRelationBuildTime ms")
    // victor: if cache, we cannot close on task completion as recursion may have multiple tasks
    // TODO: find the right time to close the relation to avoid memory leak
    // This relation is usually used until the end of task.
    if (!cacheBuildSide) {
      context.addTaskCompletionListener(_ => relation.close())
    }
    relation
  }

  private def doCacheBuildSide(): Unit = {
    if (!cached) {
      val buildRDD = buildPlan.execute()
      new MapPartitionsRDDInMemory(name, buildRDD,
        (context: TaskContext, index: Int, iter: Iterator[InternalRow]) => {
          if (iter.nonEmpty) {
            logDebug(s"Partition: $index builds nonEmpty HashedRelation")
          }
          // In MapPartitionsRDDInMemory, we will extract the hashedRelation and put it in memoryManager for reuse
          Iterator(buildHashedRelation(iter))
        }).count()
      cached = true
    }
  }

  /**
    * victor: check [[org.apache.spark.sql.execution.joins.ShuffledHashJoinExec]]
    */
  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    if (cacheBuildSide) {
      doCacheBuildSide()
      streamedPlan.execute().mapPartitionsInternalWithIndex { (index, streamedIter) =>
        // TODO: [bug] if there are more than one CacheBuildSideShuffleHashJoin operator
        val hashed = PartitionDataKeeper.get(name).getBuildSideHashedRelation(index)
        if (hashed == null) {
          throw new RuntimeException(s"BuildSideHashedRelation not found for partition: $index")
        }
        if (streamedIter.nonEmpty) {
          logDebug(s"streamedPlan partition: $index nonEmpty")
        }
        join(streamedIter, hashed, numOutputRows)
      }
    } else {
      streamedPlan.execute().zipPartitions(buildPlan.execute(), true) { (streamIter, buildIter) =>
        val hashed = buildHashedRelation(buildIter)
        join(streamIter, hashed, numOutputRows)
      }
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override def doProduce(ctx: CodegenContext): String = {
    streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    joinType match {
      case Inner => codegenInner(ctx, input)
      case x => throw new UnsupportedOperationException(s"Codegen JoinType: $x")
    }
  }

  /**
   * Returns the code for generating join key for stream side, and expression of whether the key
   * has any null in it or not.
   */
  private def genStreamSideJoinKey(
      ctx: CodegenContext,
      input: Seq[ExprCode]): (ExprCode, String) = {
    ctx.currentVars = input
    if (streamedKeys.length == 1 && streamedKeys.head.dataType == LongType) {
      val ev = streamedKeys.head.genCode(ctx)
      (ev, ev.isNull)
    } else {
      logWarning("generate the join key as UnsafeRow (maybe slow)")
      val ev = GenerateUnsafeProjection.createCode(ctx, streamedKeys)
      (ev, s"${ev.value}.anyNull()")
    }
  }

  /**
   * Generates the code for variable of build side.
   */
  private def genBuildSideVars(ctx: CodegenContext, matched: String): Seq[ExprCode] = {
    ctx.currentVars = null
    ctx.INPUT_ROW = matched
    buildPlan.output.zipWithIndex.map { case (a, i) => BoundReference(i, a.dataType, a.nullable).genCode(ctx) }
  }

  /**
   * Generate the (non-equi) condition used to filter joined rows. This is used in Inner, Left Semi
   * and Left Anti joins.
   */
  private def getJoinCondition(
      ctx: CodegenContext,
      input: Seq[ExprCode]): (String, String, Seq[ExprCode]) = {
    val matched = ctx.freshName("matched")
    val buildVars = genBuildSideVars(ctx, matched)
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)
      // filter the output via condition
      ctx.currentVars = input ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamedPlan.output ++ buildPlan.output).genCode(ctx)
      val skipRow = s"${ev.isNull} || !${ev.value}"

      s"""
         |$eval
         |${ev.code}
         |if ($skipRow) continue;
       """.stripMargin
    } else {
      ""
    }
    (matched, checkCondition, buildVars)
  }

  private def prepareBuildSideRelation(ctx: CodegenContext): (Boolean, String) = {
    // cache build side relation
    doCacheBuildSide()

    // create a name for HashedRelation
    val relationTerm = ctx.freshName("relation")
    val dataKeeperTerm = ctx.freshName("dataKeeper")
    val partitionDataTerm = ctx.freshName("partitionData")
    val clsName = "org.apache.spark.sql.execution.joins.HashedRelation"
    val recName = ctx.addReferenceObj("recName", name)
    // TODO: [bug] if there are more than one CacheBuildSideShuffleHashJoin operator
    ctx.addMutableState(clsName, relationTerm,
      s"""
         | org.apache.spark.sql.datalog.PartitionDataKeeper $dataKeeperTerm = (org.apache.spark.sql.datalog.PartitionDataKeeper) org.apache.spark.TaskContext$$.MODULE$$.get().taskMemoryManager().partitionData;
         | org.apache.spark.sql.datalog.PartitionData $partitionDataTerm = $dataKeeperTerm.get($recName);
         | $relationTerm = $partitionDataTerm.getBuildSideHashedRelation(partitionIndex);
       """.stripMargin)
    (false, relationTerm)
  }

  /**
   * Generates the code for Inner join.
   */
  private def codegenInner(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (keyIsUnique, relationTerm) = prepareBuildSideRelation(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, buildVars) = getJoinCondition(ctx, input)
    val numOutput = metricTerm(ctx, "numOutputRows")

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }
    if (keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |if ($matched == null) continue;
         |$checkCondition
         |$numOutput.add(1);
         |${consume(ctx, resultVars)}
       """.stripMargin

    } else {
      ctx.copyResult = true
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm.get(${keyEv.value});
         |if ($matches == null) continue;
         |while ($matches.hasNext()) {
         |  UnsafeRow $matched = (UnsafeRow) $matches.next();
         |  $checkCondition
         |  $numOutput.add(1);
         |  ${consume(ctx, resultVars)}
         |}
       """.stripMargin
    }
  }
}
