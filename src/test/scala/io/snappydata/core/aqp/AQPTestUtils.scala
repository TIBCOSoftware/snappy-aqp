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
package io.snappydata.core.aqp

import org.scalatest._

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{SnappyHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.bootstrap.DeclarativeBootstrapAggregateFunction
import org.apache.spark.sql.execution.closedform.ClosedFormErrorEstimate
import org.apache.spark.sql.{DataFrame, Row}

object AQPTestUtils extends Matchers {

  def getError(plan: SparkPlan): Double = {
    plan.collectFirst {
      case sorted: SortAggregateExec => sorted
      case tung: SnappyHashAggregateExec => tung
    } match {
      case Some(x) =>
        var err: Double = -1
        x.expressions.foreach {
          y => val error = y.collectFirst {
            case a: DeclarativeBootstrapAggregateFunction => a.error
            case ClosedFormErrorEstimate(_, _, _, _, e, _, _) => e
          } match {
            case Some(z) => z
            case None => -1
          }
            if (error != -1) {
              err = error
            }
        }
        err
      case None => -1
    }
  }

  private def doPrint(str: String): Boolean = {
    // scalastyle:off println
    println(str)
    // scalastyle:on println
    false
  }

  def compareShowAndCollect[T](dfShow: DataFrame, arrayCollect: Array[Row], fn: Row => T): Unit =
    compareShowAndCollectImpl[T](dfShow, arrayCollect, fn, true, false)

  def compareShowAndCollectNoOrder[T](dfShow: DataFrame, arrayCollect: Array[Row],
      fn: Row => T): Unit = compareShowAndCollectImpl[T](dfShow, arrayCollect, fn, false, false)

  private def compareShowAndCollectImpl[T](dfShow: DataFrame, arrayCollect: Array[Row],
      fn: Row => T, checkOrder: Boolean, disableAssertion: Boolean): Unit = {
    val takeResult = dfShow.toDF().take(21)
    val data = takeResult.take(20)
    val showArray: Seq[T] = data.map(r => fn(r)).toSeq
    val collectArray: Seq[T] = arrayCollect.map(r => fn(r)).toSeq

    if (disableAssertion) {
      if (showArray.length != collectArray.length) {
        doPrint("compareShowAndCollect: Unequal sizes : " + showArray.length +
            " vs " + collectArray.length)
      } else {
        var anyMismatch: Boolean = false
        showArray.indices foreach { i =>
          if (checkOrder) {
            if (!compare[T](showArray(i), collectArray(i))) {
              doPrint(s"compareShowAndCollect: Unequal value at index $i : " +
                  showArray(i) + " vs " + collectArray(i))
              anyMismatch = true
            }
          } else {
            if (!(contains[T](collectArray, showArray(i)) &&
                contains[T](showArray, collectArray(i)))) {
              doPrint(s"compareShowAndCollect: Unequal value at index $i : " +
                  showArray(i) + " in " + collectArray.mkString(" , "))
              anyMismatch = true
            }
          }
        }
        if (!anyMismatch) {
          doPrint("compareShowAndCollect: Values from show and collect are of same size and order")
        }
      }
    } else {
      def debugPrint() = {
        doPrint("compareShowAndCollect Show:")
        showArray.foreach(v => doPrint(v.toString))
        doPrint("compareShowAndCollect Collect:")
        collectArray.foreach(v => doPrint(v.toString))
      }

      assertResult(true) {
        val result: Boolean = showArray.length == collectArray.length
        if (!result) debugPrint()
        result
      }

      showArray.indices foreach { i =>
        assertResult(true) {
          val result: Boolean = if (checkOrder) {
            compare[T](showArray(i), collectArray(i))
          } else {
            contains[T](collectArray, showArray(i)) &&
                contains[T](showArray, collectArray(i))
          }
          if (!result) debugPrint()
          result
        }
      }
    }
  }

  def compare[T](left: T, right: T): Boolean = {
    left match {
      case x: Double => (left.asInstanceOf[Double] -
          right.asInstanceOf[Double]).abs < 1
      case x: BigDecimal => (left.asInstanceOf[BigDecimal] -
          right.asInstanceOf[BigDecimal]).abs < 1
        // TODO: A defect that show values off by 1 in random repeat runs
      case x: Long => (left.asInstanceOf[Long]-
          right.asInstanceOf[Long]).abs < 2
      case _ => left == right
    }
  }

  def contains[T](arr: Seq[T], value: T): Boolean = arr.exists(e => compare[T](e, value))
}
