/*
 * Copyright (c) 2017-2021 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.execution.common

import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, Cast, Coalesce, ExprId, Expression, If, IsNull, Literal, Multiply, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.aggregate.GroupAggregate
import org.apache.spark.sql.execution.bootstrap.DoubleStructCreator
import org.apache.spark.sql.types._

case class AQPAverage(weight: MapColumnToWeight, actualColumn: Expression) extends
  DeclarativeAggregate {

  override def prettyName: String = "avg"

  override def children: Seq[Expression] = weight :: actualColumn ::Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(actualColumn.dataType, "function average")

  private lazy val resultType = actualColumn.dataType match {
    case DecimalType.Fixed(p, s) =>
      DecimalType.bounded(p + 4, s + 4)
    case _ => DoubleType
  }



  private lazy val sum = AttributeReference("sum", DoubleType)()
  private lazy val count = AttributeReference("count", DoubleType, nullable = false)()

  override lazy val aggBufferAttributes = sum :: count :: Nil

  override lazy val initialValues = Seq(
    /* sum = */ Cast(Literal(0), DoubleType),
    /* count = */ Cast(Literal(0), DoubleType)
  )

  override lazy val updateExpressions = Seq(
    /* sum = */
    Add(
      sum,
      Coalesce(Multiply(weight, actualColumn) :: Cast(Literal(0), DoubleType) :: Nil)),
     If(IsNull(actualColumn), count, Add(count , weight))
  )

  override lazy val mergeExpressions = Seq(
    /* sum = */ sum.left + sum.right,
    /* count = */ count.left + count.right
  )

  // If all input are nulls, count will be 0 and we will get null after the division.
  override lazy val evaluateExpression = sum / count
}

case class AQPCount(weight: MapColumnToWeight, columns: Seq[Expression]) extends
  DeclarativeAggregate {

  override def nullable: Boolean = false

  override def children: Seq[Expression] = weight +: columns

  // Return data type.
  override def dataType: DataType = DoubleType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = DoubleType +: Seq.fill(columns.size)(AnyDataType)

  private lazy val count = AttributeReference("count", DoubleType, nullable = false)()

  override lazy val aggBufferAttributes = count :: Nil

  override lazy val initialValues = Seq(
    /* count = */ Cast(Literal(0d), DoubleType)
  )

  override lazy val updateExpressions = {
    val nullableChildren = columns.filter(_.nullable)
    if (nullableChildren.isEmpty) {
      Seq(Add(count, weight))
    } else {
      Seq(
       If(nullableChildren.map(IsNull).reduce(Or), count, Add(count, weight))
      )
    }
  }

  override lazy val mergeExpressions = Seq(count.left + count.right)

  override lazy val evaluateExpression = count

  override def defaultResult: Option[Literal] = Option(Literal(0d))
}


case class AQPSum(weight: MapColumnToWeight, child: Expression) extends DeclarativeAggregate with
  GroupAggregate {

  override def children: Seq[Expression] = weight :: child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(DoubleType, TypeCollection(LongType, DoubleType, DecimalType))

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function sum")

  private lazy val resultType = DoubleType

  private lazy val sumDataType = resultType

  private lazy val sum = AttributeReference("sum", sumDataType)()

  private lazy val zero = Cast(Literal(0), sumDataType)


  @transient override lazy val initialValuesForGroup: Seq[Expression] = {
    if (child.nullable) Seq(Literal.create(null, sumDataType))
    else Seq(Literal.create(0, sumDataType))
  }


  override lazy val aggBufferAttributesForGroup: Seq[AttributeReference] = {
    if (child.nullable) aggBufferAttributes
    else aggBufferAttributes.map(attr => attr.copy(nullable = false)(
      attr.exprId, attr.qualifier, attr.isGenerated))
  }

  override lazy val aggBufferAttributes = sum :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    /* sum = */ Literal.create(null, sumDataType)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    if (child.nullable) {
      Seq(
        /* sum = */
        Coalesce(Seq(Add(Coalesce(Seq(sum, zero)), Multiply(weight, child)), sum))
      )
    } else {
      Seq(
        /* sum = */
        Add(Coalesce(Seq(sum, zero)), Multiply(weight, child))
      )
    }
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* sum = */
      Coalesce(Seq(Add(Coalesce(Seq(sum.left, zero)), sum.right), sum.left))
    )
  }

  override lazy val evaluateExpression: Expression = sum
}

