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
package org.apache.spark.sql.execution.bootstrap


import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, Sum}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.aggregate.GroupAggregate
import org.apache.spark.sql.execution.closedform.ErrorAggregate
import org.apache.spark.sql.execution.common.{AQPSum, HAC, MapColumnToWeight}
import org.apache.spark.sql.types._

/**
 * Created by ashahid on 6/2/16.
 */

case class BootstrapSum(val actualColumn: Expression,
    @transient _numBootstrapTrials: Int, @transient _confidence: Double,
    @transient _error: Double, _errorEstimateProjs: Seq[(Int, ErrorAggregate.Type,
  String, String, ExprId, NamedExpression)], @transient _isAQPDebug: Boolean,
  @transient _behavior: HAC.Type)
    extends DeclarativeBootstrapAggregateFunction(_numBootstrapTrials,
      _confidence, _error, _isAQPDebug, _behavior, _errorEstimateProjs, ErrorAggregate.Sum) with
      GroupAggregate {


  def getExprIDReserveQuota: Int = 1


  override def children: Seq[Expression] = actualColumn :: Nil

  override def nullable: Boolean = true


  override def inputTypes: Seq[AbstractDataType] = Nil

  override def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  // TypeUtils.checkForNumericExpr(precomputedCol.dataType, "function sum")

  @transient override lazy val initialValues: Seq[Expression] = Seq(initVal)

  @transient override lazy val initialValuesForGroup: Seq[Expression] = {
    if (actualColumn.nullable) Seq(initVal)
    else Seq(DoubleStructCreator(this.numBootstrapTrials, isAQPDebug))
  }

  @transient override lazy val aggBufferAttributes: Seq[AttributeReference] =
    Seq.tabulate[AttributeReference](getExprIDReserveQuota)(i =>
      AttributeReference("sum", commonArrayDataType)(ExprId(baseExprID.id + i,
        baseExprID.jvmId)))

  override lazy val aggBufferAttributesForGroup: Seq[AttributeReference] = {
    if (actualColumn.nullable) aggBufferAttributes
    else aggBufferAttributes.map(attr => attr.copy(nullable = false)(
      attr.exprId, attr.qualifier, attr.isGenerated))
  }

  @transient override lazy val updateExpressions: Seq[Expression] = aggBufferAttributes.map(
      buffer => BootstrapAddTrialsAsArray(BootstrapAddTrialsAsArray.castToDouble(actualColumn),
        buffer, numBootstrapTrials, BootstrapAddTrialsAsArray.BootstrapSum, isAQPDebug,
        this.commonArrayDataType)
    )



  @transient override lazy val mergeExpressions: Seq[Expression] = aggBufferAttributes.map(buffer =>
    ArrayOpsExpression(buffer.left, buffer.right, ArrayOpsExpression.ADD,
      ArrayOpsExpression.BEHAVIOUR_BOOTSTRAP_SUM, numBootstrapTrials, this.commonArrayDataType)
  )

  override def trialResults: Expression = aggBufferAttributes(0)

  override def convertToBypassErrorCalcFunction(weight: MapColumnToWeight): AggregateFunction =
    AQPSum(weight, Cast(actualColumn, DoubleType))

}

