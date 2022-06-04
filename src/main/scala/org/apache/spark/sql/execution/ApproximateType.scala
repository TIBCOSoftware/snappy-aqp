/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.execution

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

class ApproximateType extends UserDefinedType[Approximate] {
  /** Underlying storage type for this UDT */
  override def sqlType: DataType = ApproximateType.internalType

  override def serialize(obj: Approximate): InternalRow =
    obj match {
      case approx: Approximate => InternalRow(approx.lowerBound,
        approx.estimate, approx.max, approx.probabilityWithinBounds)
    }

  override def userClass: Class[Approximate] = classOf[Approximate]

  /** Convert a SQL datum to the user type */
  override def deserialize(datum: Any): Approximate =
    datum match {
      case row: InternalRow =>
        new Approximate(row.getLong(0), row.getLong(1),
          row.getLong(2), row.getDouble(3))
    }

  override def typeName: String = "Approximate"

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
    ("class" -> classOf[ApproximateType].getName) ~
    ("pyClass" -> pyUDT) ~
    ("sqlType" -> sqlType.jsonValue)
  }
}

object ApproximateType extends ApproximateType {
  private val internalType: DataType = {
    StructType(
      StructField("lowerBound", LongType, nullable = false) ::
      StructField("estimate", LongType, nullable = false) ::
      StructField("max", LongType, nullable = false) ::
      StructField("confidence", DoubleType, nullable = false) :: Nil)
  }
}
