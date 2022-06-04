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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.closedform.ErrorAggregate
import org.apache.spark.sql.execution.common.{ErrorAggregateFunction, HAC}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, IntegerType}

import scala.collection.mutable

abstract class DeclarativeBootstrapAggregateFunction(val numBootstrapTrials: Int,
    __confidence: Double, __error: Double, val isAQPDebug: Boolean,
    __behavior: HAC.Type, __errorEstimateProjs: Seq[(Int, ErrorAggregate.Type, String,
  String, ExprId, NamedExpression)], __aggregateType: ErrorAggregate.Type)
  extends  ErrorAggregateFunction(__confidence, __error, __behavior, __errorEstimateProjs,
    __aggregateType) {



  def trialResults: Expression

  def getExprIDReserveQuota: Int

  val baseExprID = NamedExpression.allocateExprID(getExprIDReserveQuota)

  @transient lazy val commonDataType = DoubleType
  @transient lazy val commonArrayDataType = BootstrapStructType(numBootstrapTrials, DoubleType,
    BootstrapStructType.trialField)
  @transient lazy val zero = Cast(Literal(0), commonDataType)
  @transient lazy val zeroStruct = DoubleStructCreator(numBootstrapTrials, isAQPDebug)

  @transient lazy val initVal = Literal.create(null, commonArrayDataType)

  override def dataType: DataType = evaluateExpression.dataType

  @transient lazy val evaluateExpression = if (isAQPDebug) {
    ApproxColumnDebug(trialResults, numBootstrapTrials)
  } else {
    val stateClass = classOf[ByteMutableRow].getName
    ApproxColumn(trialResults, CachedFieldWrapper(BootstrapMultiplicityAggregate
        .cachedBootstrapResultField, stateClass, isNullable = true),
      confidence, aggregateType, error, behavior)
  }

  override def toString: String = {
    s"ERROR ESTIMATE ($confidence) $aggregateType"
  }
}

object BootstrapMultiplicity {
  val groupSize = 7
  val aggregateName = "bootstrap_multiplicity_aggregate"

  val fullGroupByte = {
    var byeet = 0x00
    for(i <- 0 until groupSize) {
      byeet = byeet | (1 << i)
    }
    byeet
  }

  def calculateMultiplicityResultSize(numBootstrapCols: Int): Int = {
    numBootstrapCols / BootstrapMultiplicity.groupSize +
        (if (numBootstrapCols % BootstrapMultiplicity.groupSize == 0) {
          0
        } else {
          1
        })
  }

  def getFullySatisfiedCount(numBootstrapCols: Int): Int = {

    numBootstrapCols / BootstrapMultiplicity.groupSize * fullGroupByte +
      (if (numBootstrapCols % BootstrapMultiplicity.groupSize == 0) {
        0
      } else {
        val rem = numBootstrapCols % BootstrapMultiplicity.groupSize
        var byeet = 0x00
        for(i <- 0 until rem) {
          byeet = byeet | (1 << i)
        }
        byeet
      })
  }

  def generateBootstrapColumnReference: Attribute = AttributeReference(
      VirtualColumn.groupingIdName, IntegerType, nullable = false)()


  def getBootstrapMultiplicityAliasID: ExprId = NamedExpression.newExprId
}
