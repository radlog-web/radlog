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

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types._

// scalastyle:off line.size.limit
// scalastyle:off println
case class CreateRamSQLViewCommand(
    tableDesc: CatalogTable,
    child: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    isTemporary: Boolean)
  extends RunnableCommand {

  override val output: Seq[Attribute] = {
    AttributeReference("viewSchema", StringType, nullable = false)() :: Nil
  }

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(child)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = sparkSession.sessionState.executePlan(child)
    qe.assertAnalyzed()
    val executedPlan = qe.executedPlan
    logInfo("Analyzed Plan:\n" + executedPlan)

    if (tableDesc.schema != Nil && tableDesc.schema.length != executedPlan.output.length) {
      throw new AnalysisException(s"The number of columns produced by the SELECT clause " +
        s"(num: `${executedPlan.output.length}`) does not match the number of column names " +
        s"specified by CREATE VIEW (num: `${tableDesc.schema.length}`).")
    }

    // Get the correct DataType from output
    val schema = StructType(executedPlan.output.zip(tableDesc.schema).map {
      case (attr, col) => StructField(col.name, attr.dataType)
    })
    val tableName = tableDesc.identifier.table
    val nameSchema = s"$tableName(${schema.map(e => e.name + ": " + getDBTypeString(e.dataType)).mkString(", ")})"

    logInfo(s"Execute Plan to create DataFrame: $nameSchema")
    val df = sparkSession.internalCreateDataFrame(qe.toRdd, schema)
    df.createOrReplaceTempView(tableName)

    Seq(Row(nameSchema))
  }

  private def getDBTypeString(s: DataType): String = {
    s match {
      case IntegerType => "integer"
      case LongType => "long"
      case FloatType => "float"
      case DoubleType => "double"
      case StringType => "string"
      case _ => throw new UnsupportedOperationException(s"Unsupported Type: $s")
    }
  }

  private def createTemporaryView(
      table: TableIdentifier, sparkSession: SparkSession, analyzedPlan: LogicalPlan): Unit = {

    val sessionState = sparkSession.sessionState
    val catalog = sessionState.catalog

    // Projects column names to alias names
    val logicalPlan = {
      if (tableDesc.schema.isEmpty) {
        analyzedPlan
      } else {
        val projectList = analyzedPlan.output.zip(tableDesc.schema).map {
          case (attr, col) => Alias(attr, col.name)()
        }
        sparkSession.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
      }
    }

    catalog.createTempView(table.table, logicalPlan, replace)
  }
}
