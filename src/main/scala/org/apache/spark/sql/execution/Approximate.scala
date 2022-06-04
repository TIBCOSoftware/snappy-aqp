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

import org.apache.spark.sql.types.SQLUserDefinedType

/**
 * True count is > lower Bound & less than Max , with the given probability
 */
@SQLUserDefinedType(udt = classOf[ApproximateType])
class Approximate(val lowerBound: Long, val estimate: Long, val max: Long,
    val probabilityWithinBounds: Double) extends Comparable[Approximate]
    with Ordered[Approximate] with Serializable {

  def +(other: Approximate): Approximate = {
    require(this.probabilityWithinBounds == other.probabilityWithinBounds)
    new Approximate(this.lowerBound + other.lowerBound, this.estimate + other.estimate,
      this.estimate + other.estimate, this.probabilityWithinBounds)
  }

  def -(other: Approximate): Approximate = {
    require(this.probabilityWithinBounds == other.probabilityWithinBounds)
    new Approximate(this.lowerBound - other.lowerBound, this.estimate - other.estimate,
      this.estimate - other.estimate, this.probabilityWithinBounds)
  }

  override def compare(o: Approximate): Int = {
    if (this.estimate > o.estimate) {
      1
    } else if (this.estimate == o.estimate) {
      0
    } else {
      -1
    }
  }

  override def toString: String =
    "Estimate = " + this.estimate + ", Lower Bound = " + this.lowerBound +
        ", upper bound = " + this.max + ", confidence = " +
        this.probabilityWithinBounds
}

object Approximate {
  def zeroApproximate(confidence: Double): Approximate =
    new Approximate(0, 0, 0, confidence)
}
