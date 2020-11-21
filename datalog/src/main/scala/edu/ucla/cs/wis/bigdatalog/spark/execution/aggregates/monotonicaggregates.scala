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

package edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

abstract class MonotonicAggregateFunction extends DeclarativeAggregate with Serializable {}

/**
 * Sample can only be used together with [[MMax]] or [[MMin]]
 */
case class Sample(child: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function sample")

  private lazy val sample = AttributeReference("sample", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = sample :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(Literal.create(null, child.dataType))

  override lazy val updateExpressions: Seq[Expression] = Seq(child)

  override lazy val mergeExpressions: Seq[Expression] = Seq(sample.right)

  override lazy val evaluateExpression: AttributeReference = sample
}

case class MMax(child: Expression) extends MonotonicAggregateFunction {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function mmax")

  private lazy val mmax = AttributeReference("mmax", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = mmax :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* mmax = */ Literal.create(null, child.dataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* mmax = */ Greatest(Seq(mmax, child))
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* mmax = */ Greatest(Seq(mmax.left, mmax.right))
    )
  }

  override lazy val evaluateExpression: AttributeReference = mmax
}

case class MMin(child: Expression) extends MonotonicAggregateFunction {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function mmin")

  private lazy val mmin = AttributeReference("mmin", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = mmin :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* mmin = */ Literal.create(null, child.dataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* mmin = */ Least(Seq(mmin, child))
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* mmin = */ Least(Seq(mmin.left, mmin.right))
    )
  }

  override lazy val evaluateExpression: AttributeReference = mmin
}

case class MCount(subGroupingKey: Expression,
                  aggregateArgument: Expression,
                  mutableAggBufferOffset: Int,
                  inputAggBufferOffset: Int)
  extends MonotonicAggregateFunction {

  def this(subGroupingKey: Expression, aggregateArgument: Expression) =
    this(subGroupingKey, aggregateArgument, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)

  override def children: Seq[Expression] = aggregateArgument :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, IntegerType))

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(aggregateArgument.dataType, "function mcount")

  private lazy val resultType = aggregateArgument.dataType

  private lazy val subAggregationDataType = aggregateArgument.dataType

  private lazy val mcountDataType = resultType

  private lazy val mcount = AttributeReference("mcount", mcountDataType)()

  var max: AttributeReference = _

  private lazy val zero = Cast(Literal(0), mcountDataType)

  override lazy val aggBufferAttributes = mcount :: Nil

  def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): MonotonicAggregateFunction =
    MCount(subGroupingKey, aggregateArgument, newMutableAggBufferOffset, inputAggBufferOffset)

  def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): MonotonicAggregateFunction =
    MCount(subGroupingKey, aggregateArgument, mutableAggBufferOffset, newInputAggBufferOffset)

  override lazy val initialValues: Seq[Expression] = Seq(
    Literal.create(null, mcountDataType)
  )

  lazy val initialSubKeyValues: Seq[Expression] = Seq(
    Literal.create(null, subAggregationDataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    Coalesce(Seq(Add(Coalesce(Seq(mcount, zero)), Cast(aggregateArgument, mcountDataType)), mcount))
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    val add = Add(Coalesce(Seq(mcount.left, zero)), Cast(max, mcountDataType))
    Seq(Coalesce(Seq(add, mcount.left)))
  }

  lazy val updateFun: (MutableRow, InternalRow) => Unit = {
    if (subAggregationDataType == IntegerType) {
      (mutableAggBuffer: MutableRow, inputRow: InternalRow) => {
        val previousValue = mutableAggBuffer.getInt(mutableAggBufferOffset)
        val deltaValue = inputRow.getInt(inputAggBufferOffset)
        mutableAggBuffer.setInt(mutableAggBufferOffset, previousValue + deltaValue)
      }
    } else {
      (mutableAggBuffer: MutableRow, inputRow: InternalRow) => {
        val previousValue = mutableAggBuffer.getInt(mutableAggBufferOffset)
        val deltaValue = inputRow.getInt(inputAggBufferOffset)
        mutableAggBuffer.setInt(mutableAggBufferOffset, previousValue + deltaValue)
      }
    }
  }

  def update(mutableAggBuffer: MutableRow, inputRow: InternalRow): Unit = {
    updateFun(mutableAggBuffer, inputRow)
  }

  override lazy val evaluateExpression: Expression = Cast(mcount, resultType)

  /** String representation used in explain plans. */
  override def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    prettyName + Seq(subGroupingKey, aggregateArgument).mkString(start, ", ", ")")
  }
}

case class MSum(subGroupingKey: Expression,
                aggregateArgument: Expression,
                mutableAggBufferOffset: Int,
                inputAggBufferOffset: Int)
  extends MonotonicAggregateFunction {

  def this(subGroupingKey: Expression, aggregateArgument: Expression) =
    this(subGroupingKey, aggregateArgument, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)

  override def children: Seq[Expression] = aggregateArgument :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, DoubleType))

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(aggregateArgument.dataType, "function msum")

  private lazy val resultType = aggregateArgument.dataType

  private lazy val subAggregationDataType = aggregateArgument.dataType

  private lazy val msumDataType = resultType

  private lazy val msum = AttributeReference("msum", msumDataType)()

  var max: AttributeReference = _

  private lazy val zero = Cast(Literal(0), msumDataType)

  override lazy val aggBufferAttributes = msum :: Nil

  def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): MonotonicAggregateFunction =
    MSum(subGroupingKey, aggregateArgument, newMutableAggBufferOffset, inputAggBufferOffset)

  def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): MonotonicAggregateFunction =
    MSum(subGroupingKey, aggregateArgument, mutableAggBufferOffset, newInputAggBufferOffset)

  override lazy val initialValues: Seq[Expression] = Seq(
    Literal.create(null, msumDataType)
  )

  lazy val initialSubKeyValues: Seq[Expression] = Seq(
    Literal.create(null, subAggregationDataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    Coalesce(Seq(Add(Coalesce(Seq(msum, zero)), Cast(aggregateArgument, msumDataType)), msum))
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    val add = Add(Coalesce(Seq(msum.left, zero)), Cast(max, msumDataType))
    Seq(Coalesce(Seq(add, msum.left)))
  }

  lazy val updateFun: (MutableRow, InternalRow) => Unit = {
    if (subAggregationDataType == IntegerType) {
      (mutableAggBuffer: MutableRow, inputRow: InternalRow) => {
        val previousValue = mutableAggBuffer.getInt(mutableAggBufferOffset)
        val deltaValue = inputRow.getInt(inputAggBufferOffset)
        mutableAggBuffer.setInt(mutableAggBufferOffset, previousValue + deltaValue)
      }
    } else {
      (mutableAggBuffer: MutableRow, inputRow: InternalRow) => {
        val previousValue = mutableAggBuffer.getInt(mutableAggBufferOffset)
        val deltaValue = inputRow.getInt(inputAggBufferOffset)
        mutableAggBuffer.setInt(mutableAggBufferOffset, previousValue + deltaValue)
      }
    }
  }

  def update(mutableAggBuffer: MutableRow, inputRow: InternalRow): Unit = {
    updateFun(mutableAggBuffer, inputRow)
  }

  override lazy val evaluateExpression: Expression = Cast(msum, resultType)

  /** String representation used in explain plans. */
  override def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    prettyName + Seq(subGroupingKey, aggregateArgument).mkString(start, ", ", ")")
  }
}

/* case class MAvg(child: Expression) extends DeclarativeAggregate {

  override def prettyName: String = "mavg"

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function average")

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(p, s) =>
      DecimalType.bounded(p + 4, s + 4)
    case _ => DoubleType
  }

  private lazy val sumDataType = child.dataType match {
    case _ @ DecimalType.Fixed(p, s) => DecimalType.bounded(p + 10, s)
    case _ => DoubleType
  }

  private lazy val sum = AttributeReference("sum", sumDataType)()
  private lazy val count = AttributeReference("count", LongType)()

  private lazy val one = Cast(Literal(1), LongType)

  override lazy val aggBufferAttributes = sum :: count :: Nil

  override lazy val initialValues = Seq(
    /* sum = */ Cast(Literal(0), sumDataType),
    /* count = */ Literal(0L)
  )

  override lazy val updateExpressions = Seq(
    /* sum = */
    Add(
      sum,
      Coalesce(Cast(child, sumDataType) :: Cast(Literal(0), sumDataType) :: Nil)),
    /* count = */ If(IsNull(child), count, Add(count, one))
  )

  override lazy val mergeExpressions = Seq(
    /* sum = */ Add(sum.left, sum.right),
    /* count = */ Add(count.left, count.right)
  )

  // If all input are nulls, count will be 0 and we will get null after the division.
  override lazy val evaluateExpression = child.dataType match {
    case DecimalType.Fixed(p, s) =>
      // increase the precision and scale to prevent precision loss
      val dt = DecimalType.bounded(p + 14, s + 4)
      Cast(Divide(Cast(sum, dt), Cast(count, dt)), resultType)
    case _ =>
      Divide(Cast(sum, resultType), Cast(count, resultType))
  }
} */

case class MAvg(child: Expression) extends MonotonicAggregateFunction {

  override def prettyName: String = "mavg"

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function average")

  private lazy val resultType = child.dataType

  private lazy val mavg = AttributeReference("mavg", resultType)()
  private lazy val count = AttributeReference("count", LongType)()

  private lazy val one = Cast(Literal(1), LongType)

  override lazy val aggBufferAttributes = mavg :: count :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    /* mavg = */ Cast(Literal(0), resultType),
    /* count = */ Literal(0L)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* mavg = */
    Cast(Divide(Add(Multiply(mavg, count), child), Add(count, one)), resultType),
    /* count = */ If(IsNull(child), count, Add(count, one))
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    /* sum = */
    Cast(Divide(Add(Multiply(mavg.left, count.left), Multiply(mavg.right, count.right)), Add(count.left, count.right)), resultType),
    /* count = */ Add(count.left, count.right)
  )

  // If all input are nulls, count will be 0 and we will get null after the division.
  override lazy val evaluateExpression: Expression = Cast(mavg, resultType)
}

case class CMax(subGroupingKey: Expression,
                aggregateArgument: Expression,
                mutableAggBufferOffset: Int,
                inputAggBufferOffset: Int)
  extends MonotonicAggregateFunction {

  def this(subGroupingKey: Expression, aggregateArgument: Expression) =
    this(subGroupingKey, aggregateArgument, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)

  override def children: Seq[Expression] = subGroupingKey :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = subAggregationDataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, DoubleType))

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(subGroupingKey.dataType, "function cmax")

  private lazy val resultType = aggregateArgument.dataType

  private lazy val subAggregationDataType = subGroupingKey.dataType

  private lazy val cmaxDataType = subAggregationDataType

  private lazy val cmax = AttributeReference("cmax", cmaxDataType)()

  private lazy val max = AttributeReference("max", resultType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = cmax :: max :: Nil

  def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): MonotonicAggregateFunction =
    CMin(subGroupingKey, aggregateArgument, newMutableAggBufferOffset, inputAggBufferOffset)

  def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): MonotonicAggregateFunction =
    CMin(subGroupingKey, aggregateArgument, mutableAggBufferOffset, newInputAggBufferOffset)

  override lazy val initialValues: Seq[Expression] = Seq(
    Literal.create(Int.MaxValue, cmaxDataType),
    Literal.create(Double.MaxValue, resultType)
  )

  val ordering = TypeUtils.getInterpretedOrdering(max.dataType)

  override lazy val updateExpressions: Seq[Expression] = {
    Seq(If(GreaterThan(max, aggregateArgument), cmax, subGroupingKey),
      If(GreaterThan(max, aggregateArgument), max, aggregateArgument))
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(If(GreaterThan(max.left, max.right), cmax.left, cmax.right),
      If(GreaterThan(max.left, max.right), max.left, max.right))
  }

  override lazy val evaluateExpression: Expression = Cast(cmax, subAggregationDataType)

  lazy val updateFun: (MutableRow, InternalRow) => Unit = {
    if (subAggregationDataType == IntegerType) {
      (mutableAggBuffer: MutableRow, inputRow: InternalRow) => {
        val previousValue = mutableAggBuffer.getInt(mutableAggBufferOffset)
        val deltaValue = inputRow.getInt(inputAggBufferOffset)
        mutableAggBuffer.setInt(mutableAggBufferOffset, previousValue + deltaValue)
      }
    } else {
      (mutableAggBuffer: MutableRow, inputRow: InternalRow) => {
        val previousValue = mutableAggBuffer.getInt(mutableAggBufferOffset)
        val deltaValue = inputRow.getInt(inputAggBufferOffset)
        mutableAggBuffer.setInt(mutableAggBufferOffset, previousValue + deltaValue)
      }
    }
  }

  def update(mutableAggBuffer: MutableRow, inputRow: InternalRow): Unit = {
    updateFun(mutableAggBuffer, inputRow)
  }

  /** String representation used in explain plans. */
  override def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    prettyName + Seq(subGroupingKey, aggregateArgument).mkString(start, ", ", ")")
  }
}


case class CMin(subGroupingKey: Expression,
                aggregateArgument: Expression,
                mutableAggBufferOffset: Int,
                inputAggBufferOffset: Int)
  extends MonotonicAggregateFunction {

  def this(subGroupingKey: Expression, aggregateArgument: Expression) =
    this(subGroupingKey, aggregateArgument, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)

  override def children: Seq[Expression] = subGroupingKey :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = subAggregationDataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, DoubleType))

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(subGroupingKey.dataType, "function cmin")

  private lazy val resultType = aggregateArgument.dataType

  private lazy val subAggregationDataType = subGroupingKey.dataType

  private lazy val cminDataType = subAggregationDataType

  private lazy val cmin = AttributeReference("cmin", cminDataType)()

  private lazy val min = AttributeReference("min", resultType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = cmin :: min :: Nil

  def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): MonotonicAggregateFunction =
    CMin(subGroupingKey, aggregateArgument, newMutableAggBufferOffset, inputAggBufferOffset)

  def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): MonotonicAggregateFunction =
    CMin(subGroupingKey, aggregateArgument, mutableAggBufferOffset, newInputAggBufferOffset)

  override lazy val initialValues: Seq[Expression] = Seq(
    Literal.create(Int.MaxValue, cminDataType),
    Literal.create(Double.MaxValue, resultType)
  )

  val ordering = TypeUtils.getInterpretedOrdering(min.dataType)

  override lazy val updateExpressions: Seq[Expression] = {
    Seq(If(GreaterThan(min, aggregateArgument), subGroupingKey, cmin),
      If(GreaterThan(min, aggregateArgument), aggregateArgument, min))
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(If(GreaterThan(min.left, min.right), cmin.right, cmin.left),
      If(GreaterThan(min.left, min.right), min.right, min.left))
  }

  override lazy val evaluateExpression: Expression = Cast(cmin, subAggregationDataType)

  lazy val updateFun: (MutableRow, InternalRow) => Unit = {
    if (subAggregationDataType == IntegerType) {
      (mutableAggBuffer: MutableRow, inputRow: InternalRow) => {
        val previousValue = mutableAggBuffer.getInt(mutableAggBufferOffset)
        val deltaValue = inputRow.getInt(inputAggBufferOffset)
        mutableAggBuffer.setInt(mutableAggBufferOffset, previousValue + deltaValue)
      }
    } else {
      (mutableAggBuffer: MutableRow, inputRow: InternalRow) => {
        val previousValue = mutableAggBuffer.getInt(mutableAggBufferOffset)
        val deltaValue = inputRow.getInt(inputAggBufferOffset)
        mutableAggBuffer.setInt(mutableAggBufferOffset, previousValue + deltaValue)
      }
    }
  }

  def update(mutableAggBuffer: MutableRow, inputRow: InternalRow): Unit = {
    updateFun(mutableAggBuffer, inputRow)
  }

  /** String representation used in explain plans. */
  override def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    prettyName + Seq(subGroupingKey, aggregateArgument).mkString(start, ", ", ")")
  }
}

case class MonotonicAggregateExpression(aggregateFunction: MonotonicAggregateFunction,
                                        mode: AggregateMode,
                                        isDistinct: Boolean)
  extends Expression
    with Unevaluable {

  override def children: Seq[Expression] = aggregateFunction :: Nil
  override def dataType: DataType = aggregateFunction.dataType
  override def foldable: Boolean = false
  override def nullable: Boolean = aggregateFunction.nullable

  override def references: AttributeSet = {
    val childReferences = mode match {
      case Partial | Complete => aggregateFunction.references.toSeq
      case PartialMerge | Final => aggregateFunction.aggBufferAttributes
    }

    AttributeSet(childReferences)
  }

  override def toString: String = s"(${aggregateFunction},mode=$mode,isDistinct=$isDistinct)"
}
