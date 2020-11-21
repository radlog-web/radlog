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

package edu.ucla.cs.wis.bigdatalog.spark.logical

import java.math.BigInteger

import edu.ucla.cs.wis.bigdatalog.`type`.DataType
import edu.ucla.cs.wis.bigdatalog.database.`type`._
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable

import org.apache.spark.unsafe.types.UTF8String

object TypeUtils {

  def getDbTypeBaseValue(t : DbTypeBase) : Any = t match {
    case i : DbInteger => i.getValue
    case l : DbLong => l.getValue
    case s : DbString => s.getValue
    case d : DbDouble => d.getValue
    case f : DbFloat => f.getValue
    case si : DbShort => si.getValue
    case b : DbByte => b.getValue
    case ll : DbLongLong => new BigInteger(ll.getBytes)
    case llll : DbLongLongLongLong => new BigInteger(llll.getBytes())
    case dt : DbDateTime => new java.sql.Date(dt.getValue.getTime)
  }

  def createDbTypeBaseConverter(dataType: edu.ucla.cs.wis.bigdatalog.`type`.DataType): DbTypeBase => Any = {
    dataType match {
      case DataType.INT => (item: DbTypeBase) => item.asInstanceOf[DbInteger].getValue
      case DataType.LONG => (item: DbTypeBase) => item.asInstanceOf[DbLong].getValue
      case DataType.STRING => (item: DbTypeBase) => UTF8String.fromString(item.asInstanceOf[DbString].getValue)
      case DataType.DOUBLE => (item: DbTypeBase) => item.asInstanceOf[DbDouble].getValue
      case DataType.FLOAT => (item: DbTypeBase) => item.asInstanceOf[DbFloat].getValue
      case DataType.SHORT => (item: DbTypeBase) => item.asInstanceOf[DbShort].getValue
      case DataType.BYTE=> (item: DbTypeBase) => item.asInstanceOf[DbByte].getValue
      case DataType.LONGLONG => (item: DbTypeBase) => new BigInteger(item.asInstanceOf[DbLongLong].getBytes)
      case DataType.LONGLONGLONGLONG => (item: DbTypeBase) => new BigInteger(item.asInstanceOf[DbLongLongLongLong].getBytes())
      case DataType.DATETIME => (item: DbTypeBase) => new java.sql.Date(item.asInstanceOf[DbDateTime].getValue.getTime)
      case _ => throw new Exception("Unsupported datatype being converted!")
    }
  }

  def getSparkDataType(dealDataType: edu.ucla.cs.wis.bigdatalog.`type`.DataType): org.apache.spark.sql.types.DataType = {
    dealDataType match {
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.BYTE => org.apache.spark.sql.types.ByteType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.SHORT => org.apache.spark.sql.types.ShortType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.INT => org.apache.spark.sql.types.IntegerType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.LONG => org.apache.spark.sql.types.LongType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.FLOAT => org.apache.spark.sql.types.FloatType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.DOUBLE => org.apache.spark.sql.types.DoubleType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.STRING => org.apache.spark.sql.types.StringType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.DATETIME => org.apache.spark.sql.types.DateType
      case _ => throw new Exception("Unsupported datatype being converted!")
    }
  }

  def getSparkDataType(variable: Variable): org.apache.spark.sql.types.DataType = {
    variable.getDataType match {
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.BYTE => org.apache.spark.sql.types.ByteType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.SHORT => org.apache.spark.sql.types.ShortType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.INT => org.apache.spark.sql.types.IntegerType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.LONG => org.apache.spark.sql.types.LongType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.FLOAT => org.apache.spark.sql.types.FloatType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.DOUBLE => org.apache.spark.sql.types.DoubleType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.STRING => org.apache.spark.sql.types.StringType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.DATETIME => org.apache.spark.sql.types.DateType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.ANY => org.apache.spark.sql.types.IntegerType // Youfu Li hacked here, added this line.
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.UNKNOWN => org.apache.spark.sql.types.DoubleType // Youfu Li hacked here, added this line.
      case other => throw new UnsupportedOperationException(s"Variable: $variable DataType: $other")
    }
  }

}
