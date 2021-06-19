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
package org.apache.spark.sql.execution.bootstrap

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.execution.closedform.ErrorAggregate
import org.apache.spark.sql.execution.common.{AQPAverage, HAC, MapColumnToWeight}
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.aggregate.GroupAggregate

/**
 * Created by ashahid on 5/4/16.
 */

case class BootstrapAverage(val columnExpression: Expression, @transient _numBootstrapTrials: Int,
    @transient _confidence: Double, @transient _error: Double,
    _errorEstimateProjs: Seq[(Int, ErrorAggregate.Type, String, String, ExprId, NamedExpression)],
    @transient _isAQPDebug: Boolean, @transient _behavior: HAC.Type)
    extends DeclarativeBootstrapAggregateFunction(_numBootstrapTrials, _confidence, _error,
      _isAQPDebug, _behavior, _errorEstimateProjs, ErrorAggregate.Avg) with GroupAggregate {

  def getExprIDReserveQuota: Int = 2


  override def prettyName: String = "bootstrap avg"

  override def children: Seq[Expression] = columnExpression :: Nil

  override def nullable: Boolean = true


  override def inputTypes: Seq[AbstractDataType] = Nil

  override def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  // TypeUtils.checkForNumericExpr(preComputedColumn.dataType, "function average")

  @transient override lazy val aggBufferAttributes = Seq.tabulate[
    AttributeReference](getExprIDReserveQuota)(i => AttributeReference(
    "sum or count _" + i, commonArrayDataType)(ExprId(baseExprID.id + i,
    baseExprID.jvmId)))

  override lazy val aggBufferAttributesForGroup: Seq[AttributeReference] = {
    if (columnExpression.nullable) aggBufferAttributes
    else aggBufferAttributes.map(attr => attr.copy(nullable = false)(
      attr.exprId, attr.qualifier, attr.isGenerated))
  }


  @transient override lazy val initialValues = Seq.fill[Expression](
    getExprIDReserveQuota)(zeroStruct)


  @transient override lazy val initialValuesForGroup = initialValues

  @transient override lazy val updateExpressions = Seq(BootstrapAddTrialsAvgSumPart(
      aggBufferAttributes(0), this.commonArrayDataType),
      BootstrapAddTrialsAvgCountPart(BootstrapAddTrialsAsArray.castToDouble(columnExpression),
        aggBufferAttributes(1), numBootstrapTrials, aggBufferAttributes(0),
        this.commonArrayDataType)
    )





  @transient override lazy val mergeExpressions = aggBufferAttributes.zipWithIndex.map {
    case (buffer, i) => ArrayOpsExpression(buffer.left, buffer.right, ArrayOpsExpression.ADD,
      ArrayOpsExpression.BEHAVIOUR_DEFAULT, numBootstrapTrials, this.commonArrayDataType)
  }


  override def trialResults: Expression = ArrayOpsExpression(aggBufferAttributes(0),
    aggBufferAttributes(1), ArrayOpsExpression.DIVIDE,
    ArrayOpsExpression.BEHAVIOUR_DEFAULT, numBootstrapTrials, this.commonArrayDataType)

  def convertToBypassErrorCalcFunction(weight: MapColumnToWeight): AggregateFunction =
    AQPAverage(weight, Cast(columnExpression, DoubleType))


}
