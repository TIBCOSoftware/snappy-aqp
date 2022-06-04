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
package org.apache.spark.sql.execution.common

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, UnaryExpression, Unevaluable}
import org.apache.spark.sql.types.{DataType, DoubleType}

/**
 * Created by ashahid on 3/17/16.
 */

trait ErrorEstimateFunction extends UnaryExpression {
  def getParamName: String
  def getDoEval: Boolean
  def setDoEval(param : Boolean): ErrorEstimateFunction
}

/**
 * Expression that returns the dsid of the server containing the row.
 */

@ExpressionDescription(
  usage = "_FUNC_() - Indicates absolute error present in the estimate (approx answer) " +
      "calculated using error estimation method (ClosedForm or Bootstrap).  [enterprise]",
  extended =
    """
     Examples:
      > SELECT sum(ArrDelay) ArrivalDelay, absolute_error(ArrivalDelay),
      Month_ from airline group by Month_ order by Month_ desc with error 0.10;
      1117.6, 43.4, Jan
    """)
case class AbsoluteError(child: Expression) extends ErrorEstimateFunction {
  def this() = this(null)
  private var doEval : Boolean = false
  def getDoEval: Boolean = doEval
  def setDoEval(param : Boolean): ErrorEstimateFunction = {
    doEval = param
    this
  }
  override def prettyName: String = "Absolute_Error"
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  def getParamName: String = child.toString

  override def eval(input: InternalRow): Any = if (doEval) {
    ConstantAbsoluteError.nullSafeEval(input)
  } else {
    UnevaluableErrorEstimate.eval(input)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = if (doEval) {
    ConstantAbsoluteError.doGenCode(ctx, ev)
  } else {
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")
  }
}

@ExpressionDescription(
  usage = "_FUNC_() - Indicates ratio of absolute error to estimate (approx answer).  " +
    "calculated using error estimation method (ClosedForm or Bootstrap). [enterprise]",
  extended =
    """
    Examples:
    > SELECT sum(ArrDelay) ArrivalDelay, relative_error(ArrivalDelay),
      Month_ from airline group by Month_ order by Month_ desc with error 0.10;
       1117.6, 0.3, Jan
    """"
)
case class RelativeError(child: Expression) extends ErrorEstimateFunction {
  def this() = this(null)
  private var doEval : Boolean = false
  def getDoEval: Boolean = doEval
  def setDoEval(param : Boolean): ErrorEstimateFunction = {
    doEval = param
    this
  }
  override def prettyName: String = "Relative_Error"
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  def getParamName: String = child.toString

  override def eval(input: InternalRow): Any = if (doEval) {
    ConstantRelativeError.nullSafeEval(input)
  } else {
    UnevaluableErrorEstimate.eval(input)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = if (doEval) {
    ConstantRelativeError.doGenCode(ctx, ev)
  } else {
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")
  }
}

@ExpressionDescription(
  usage = "_FUNC_() - Lower value of an estimate interval for a given confidence." +
    "calculated using error estimation method (ClosedForm or Bootstrap).  [enterprise]",
  extended =
    """
    Examples:
    > SELECT sum(ArrDelay) ArrivalDelay, lower_bound(ArrivalDelay),
      Month_ from airline group by Month_ order by Month_ desc with error 0.10;
   1117.6, 11101.5, Jan
""""
)
case class LowerBound(child: Expression) extends ErrorEstimateFunction {
  def this() = this(null)
  private var doEval : Boolean = false
  def getDoEval: Boolean = doEval
  def setDoEval(param : Boolean): ErrorEstimateFunction = {
    doEval = param
    this
  }
  override def prettyName: String = "Lower_Bound"
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  def getParamName: String = child.toString

  override def eval(input: InternalRow): Any = if (doEval) {
    ConstantLowerBound.nullSafeEval(input)
  } else {
    UnevaluableErrorEstimate.eval(input)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = if (doEval) {
    ConstantLowerBound.doGenCode(ctx, ev)
  } else {
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")
  }
}

@ExpressionDescription(
  usage = "_FUNC_() - Upper value of an estimate interval for a given confidence." +
    "calculated using error estimation method (ClosedForm or Bootstrap).  [enterprise]",
  extended =
    """
    Examples:
    > SELECT sum(ArrDelay) ArrivalDelay, upper_bound(ArrivalDelay),
      Month_ from airline group by Month_ order by Month_ desc with error 0.10;
   1117.6, 11135.5, Jan
""""
)
case class UpperBound(child: Expression) extends ErrorEstimateFunction {
  def this() = this(null)
  private var doEval : Boolean = false
  def getDoEval: Boolean = doEval
  def setDoEval(param : Boolean): ErrorEstimateFunction = {
    doEval = param
    this
  }
  override def prettyName: String = "Upper_Bound"
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  def getParamName: String = child.toString

  override def eval(input: InternalRow): Any = if (doEval) {
    ConstantUpperBound.nullSafeEval(input)
  } else {
    UnevaluableErrorEstimate.eval(input)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = if (doEval) {
    ConstantUpperBound.doGenCode(ctx, ev)
  } else {
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")
  }
}

object UnevaluableErrorEstimate extends UnaryExpression with Unevaluable {
  override def dataType: DataType = DoubleType
  override def child: Expression = null
  override def productElement(n: Int): Any = null
  override def productArity: Int = 0
  override def canEqual(that: Any): Boolean = false
}

trait ConstantErrorEstimate extends UnaryExpression {
  override def dataType: DataType = DoubleType
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => c)

  override def child: Expression = null
  override def productElement(n: Int): Any = null
  override def productArity: Int = 0
  override def canEqual(that: Any): Boolean = false
}

object ConstantAbsoluteError extends ConstantErrorEstimate {
  override def nullSafeEval(input: Any): Any = 0.0
}

object ConstantRelativeError extends ConstantErrorEstimate {
  override def nullSafeEval(input: Any): Any = 0.0
}

object ConstantLowerBound extends ConstantErrorEstimate {
  override def nullSafeEval(input: Any): Any = input
}

object ConstantUpperBound extends ConstantErrorEstimate {
  override def nullSafeEval(input: Any): Any = input
}
