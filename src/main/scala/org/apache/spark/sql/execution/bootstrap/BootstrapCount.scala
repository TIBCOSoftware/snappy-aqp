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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, If, IsNull, Literal, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.execution.closedform.ErrorAggregate
import org.apache.spark.sql.execution.common.{AQPCount, HAC, MapColumnToWeight}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions._


case class BootstrapCount(val columns: Seq[Expression],
    @transient _numBootstrapTrials: Int, @transient _confidence: Double,
    @transient _error: Double, _errorEstimateProjs: Seq[(Int, ErrorAggregate.Type,
  String, String, ExprId, NamedExpression)],
    @transient _isAQPDebug: Boolean, @transient _behavior: HAC.Type)
    extends DeclarativeBootstrapAggregateFunction(_numBootstrapTrials,
      _confidence, _error, _isAQPDebug, _behavior, _errorEstimateProjs, ErrorAggregate.Count) {

  def getExprIDReserveQuota: Int = 1


  override def children: Seq[Expression] = columns

  override def references: AttributeSet = AttributeSet((columns).flatMap(_.references.iterator))

  override def nullable: Boolean = false

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.size)(AnyDataType)

  @transient override lazy val aggBufferAttributes = Seq.tabulate[
    AttributeReference](getExprIDReserveQuota)(i => AttributeReference(
    "count", commonArrayDataType, nullable = false)(ExprId(baseExprID.id + i,
    baseExprID.jvmId)))

  @transient override lazy val initialValues: Seq[Expression] = Seq.fill[Expression](
    getExprIDReserveQuota)(this.zeroStruct)


  @transient override lazy val updateExpressions = aggBufferAttributes.map(buffer =>
    If(columns.map(IsNull).reduce(Or), buffer, BootstrapAddTrialsAsArray(Literal(1), buffer,
      numBootstrapTrials, BootstrapAddTrialsAsArray.BootstrapCount, isAQPDebug,
      this.commonArrayDataType)))


  @transient override lazy val mergeExpressions: Seq[Expression] = aggBufferAttributes.map(count =>
    ArrayOpsExpression(count.left, count.right, ArrayOpsExpression.ADD,
      ArrayOpsExpression.BEHAVIOUR_DEFAULT, numBootstrapTrials, this.commonArrayDataType))

  override def trialResults: Expression = aggBufferAttributes(0)


  override def defaultResult: Option[Literal] = Option(Literal(0L))

  def convertToBypassErrorCalcFunction(weight: MapColumnToWeight): AggregateFunction =
    AQPCount(weight, columns)
}

object BootstrapCount {
  val precomputedCountField = "precomputedCountField_" + 0
}
