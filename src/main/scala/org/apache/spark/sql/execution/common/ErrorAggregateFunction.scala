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

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, NamedExpression}
import org.apache.spark.sql.execution.closedform.ErrorAggregate

abstract class ErrorAggregateFunction(val confidence: Double, val error: Double,
    val behavior: HAC.Type, val errorEstimateProjs: Seq[(Int, ErrorAggregate.Type, String,
  String, ExprId, NamedExpression)], val aggregateType: ErrorAggregate.Type)
  extends DeclarativeAggregate {

  final val confFactor = new NormalDistribution().
      inverseCumulativeProbability(0.5 + confidence / 2.0)

  def convertToBypassErrorCalcFunction(weight: MapColumnToWeight): AggregateFunction

  /*
  override def equals(that: Any) : Boolean = {
    that match  {
      case x:ErrorAggregateFunction => (x.aggregateType == this.aggregateType
          && this.errorEstimateProjs == x.errorEstimateProjs)
      case _ => false
    }
  } */
}
