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

import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogSessionState
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{CodegenSupport, LeafExecNode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

abstract class RecursiveRelation(val name: String, output: Seq[Attribute], partitioning: Seq[Int])
  extends LeafExecNode with CodegenSupport {

  @transient
  final val sessionState = SparkSession.getActiveSession.orNull.sessionState.asInstanceOf[BigDatalogSessionState]

  override def simpleString = s"$nodeName " + output.mkString("[", ",", "]") + "(" + name + ")"

  override def outputPartitioning: Partitioning = {
    if (partitioning == null || partitioning.isEmpty)
      UnknownPartitioning(0)
    else
      new HashPartitioning(partitioning.zip(output).filter(_._1 == 1).map(_._2),
        sessionState.conf.numShufflePartitions)
  }

  private def recursiveRDD: RDD[InternalRow] = sessionState.getRecursiveRDD(name)

  override def doExecute(): RDD[InternalRow] = recursiveRDD

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    recursiveRDD :: Nil
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    // PhysicalRDD always just has one input
    val input = ctx.freshName("input")
    ctx.addMutableState("scala.collection.Iterator", input, s"$input = inputs[0];")
    val exprRows = output.zipWithIndex.map{ case (a, i) =>
      new BoundReference(i, a.dataType, a.nullable)
    }
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsRowInput = exprRows.map(_.genCode(ctx))
    val inputRow = row // if (outputUnsafeRows) row else null
    s"""
       |while ($input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  ${consume(ctx, columnsRowInput, inputRow).trim}
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }
}

case class LinearRecursiveRelation(override val name : String, output : Seq[Attribute], partitioning: Seq[Int])
  extends RecursiveRelation(name, output, partitioning)

case class NonLinearRecursiveRelation(override val name : String, output : Seq[Attribute], partitioning: Seq[Int])
  extends RecursiveRelation(name, output, partitioning)

case class AggregateRelation(override val name : String, output : Seq[Attribute], partitioning: Seq[Int])
  extends RecursiveRelation(name, output, partitioning)
