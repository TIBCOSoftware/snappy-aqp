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
package org.apache.spark.sql.execution.closedform

import org.apache.spark.sql.catalyst.expressions.BaseGenericInternalRow


class StatCounterAggregator(val aggType: ErrorAggregate.Type)
    extends BaseGenericInternalRow with ClosedFormStats {

  @transient private val strataErrorStat = new StatCounterWithFullCount()

  var weightedCount: Double = 0
  var trueSum: Double = Double.NaN


  def getStrataErrorStat: StatCounterWithFullCount = this.strataErrorStat

  /** Evaluate once at each partition */
  def triggerSerialization() {
    this.store()
  }

  override def copy(): StatCounterAggregator = {
    val other = new StatCounterAggregator(this.aggType)
    super.copy(other)
    other
  }

  def store(): Unit = {
    if (!strataErrorStat.weight.isNaN) {
      if (this.trueSum.isNaN) {
        this.trueSum = 0
      }
      this.weightedCount += strataErrorStat.count * strataErrorStat.weight
      this.trueSum += strataErrorStat.cfMeanTotal * strataErrorStat.weight
      strataErrorStat.computeVariance(aggType)
      super.mergeDistinctCounter(strataErrorStat)
      strataErrorStat.weight = Double.NaN
    }
  }

  override def setNullAt(i: Int): Unit = {}

  override def update(i: Int, value: Any): Unit = {}
}
