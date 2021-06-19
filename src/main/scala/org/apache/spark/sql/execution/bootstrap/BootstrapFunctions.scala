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

import scala.math.Fractional

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.closedform.ErrorAggregate
import org.apache.spark.sql.execution.common.HAC
import org.apache.spark.sql.types.{DoubleType, NumericType}

/**
 *
 * The functions used in this class are used by the the generated classes.
 * The function names should match exactly with the string fields.
 * Take care while changing function names.
 * Created by ashahid on 6/24/16.
 *
 */
object BootstrapFunctions {


  val arrayOpsFunction = "operateOnArrays"
  val compressorFunc = "compressMultiplicty"
  val evalApproxColumn = "evalApproxColumn"
  val multiplicityGroupingIndex = "getMultiplicityGroupingIndexesForBootstrap"


  val numeric = DoubleType match {
    case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
    case other => other.asInstanceOf[Numeric[Any]]
  }
  val divisor = 2d

  val divide: (Any, Any) => Any = DoubleType.fractional.asInstanceOf[Fractional[Any]].div



  val arrayOpsBehaviour: Array[(DoubleMutableRow, InternalRow, Int, Boolean, Boolean) =>
    DoubleMutableRow] = Array(
    (lhs, rhs, opType, isLHSNull, isRHSNull) => {
      if (isLHSNull || isRHSNull) {
        null
      } else {
        opType match {
          case ArrayOpsExpression.ADD =>
            val values = lhs.values
            for (i <- 0 until lhs.numFields) {
              values(i) += rhs.getDouble(i)
            }
            lhs

          case ArrayOpsExpression.DIVIDE =>

            for (i <- 0 until lhs.numFields) {
              val rhsValue = rhs.getDouble(i)
              val values = lhs.values
              if (rhsValue == 0) {
                values(i) = Double.NaN // null assignmnet
              } else {
                values(i) /=  rhsValue
                // lhs.divide(i, rhsValue)
              }
            }
            lhs
        }
      }
    }
    ,

    (lhs, rhs, opType, isLHSNull, isRHSNull) => {
      if (isRHSNull) {
        lhs
      } else {
        if (isLHSNull) {
          new DoubleMutableRow(Array.tabulate[Double](rhs.numFields)(i => rhs.getDouble(i)),
            rhs.numFields)
        } else {
          opType match {
            case ArrayOpsExpression.ADD =>
              val values = lhs.values
              for (i <- 0 until lhs.numFields) {
                values(i) += rhs.getDouble(i)
              }
              lhs
          }
        }
      }
    }

  )



  def operateOnArrays(array1: DoubleMutableRow, array2: InternalRow, opType: Int,
      behaviour: Int, numBootstrapTrials: Int): DoubleMutableRow = {


    val isLHSNull = array1 == null || array1.isNullAt(0) || array1.getDouble(0).isNaN
    val isRHSNull = array2 == null ||  array2.isNullAt(0) || array2.getDouble(0).isNaN

    arrayOpsBehaviour(behaviour)(array1, array2, opType, isLHSNull, isRHSNull)
  }


  def compressMultiplicty(a: ArrayData, startIndex: Int, endIndex: Int): Byte = {
    var value: Byte = 0x00;
    for (i <- startIndex to endIndex) {
      if (a.getByte(i) > 0) {
        value = (value | (1 << (i - startIndex))).asInstanceOf[Byte];
      }
    }
    return value;
  }

  def compressMultiplicty(inputByte: Byte, index: Int, isOn: Boolean): Byte = {
    {
      if (isOn) {
        (inputByte | (1 << index)).asInstanceOf[Byte];
      } else {
        inputByte
      }
    }
  }


  def evalApproxColumn(multiplictiesResultExpr: ByteMutableRow, expArray: InternalRow,
      lower: Double, upper: Double, baseAggregateType: ErrorAggregate.Type,
      error: Double, behavior: HAC.Type): InternalRow = {


    def include(mask: Byte, normalizedIndex: Int, actualValue: Double): Boolean =
      (mask & (1 << normalizedIndex)) != 0 && (baseAggregateType == ErrorAggregate.Count ||
        !actualValue.isNaN);



    val multiplicties = multiplictiesResultExpr.values

    val row = new GenericInternalRow(ApproxColumn.TOTAL_STRUCT_FIELDS)


    if (expArray != null && !expArray.isNullAt(0) && !expArray.getDouble(0).isNaN) {
      // val results = Array.tabulate[Double](expArray.numFields)(i => expArray.getDouble(i))
      val values = for (index <- 0 until expArray.numFields;  value = expArray.getDouble(index)
                        if (include(multiplicties(index / BootstrapMultiplicity.groupSize),
                          index % BootstrapMultiplicity.groupSize, value) )) yield {
        value
      }
      val sorted = values.sorted(numeric)


      if (values.length > 0 ) {
        val estimate = values(0)
        require(!estimate.isNaN)
        row(ApproxColumn.ORDINAL_ESTIMATE) = estimate

          val lb = sorted((values.length * lower).floor.toInt)
          val ub = sorted((values.length * upper).ceil.toInt - 1)
          require(!lb.isNaN)
          require(!ub.isNaN)
          row(ApproxColumn.ORDINAL_LB) = numeric.toDouble(lb)
          row(ApproxColumn.ORDINAL_UB) = numeric.toDouble(ub)
          val absError = numeric.toDouble(divide(numeric.minus(ub, lb), divisor))
          require(!absError.isNaN)
          row(ApproxColumn.ORDINAL_ABSERR) = absError
        val safeEstimate = if (estimate < -1 || estimate > 1) {
          estimate
        }
        else if (estimate >= 0) {
          estimate + 1
        } else {
          estimate -1
        }
          val relError = math.abs(numeric.toDouble(divide(numeric.minus(ub, lb),
            numeric.times(divisor, safeEstimate))))
          require(!relError.isNaN)
          row(ApproxColumn.ORDINAL_RELERR) = relError
          if (relError > error) {
            if (behavior == HAC.SPECIAL_SYMBOL) {
              row(ApproxColumn.ORDINAL_ESTIMATE) = null
              row(ApproxColumn.ORDINAL_LB) = null
              row(ApproxColumn.ORDINAL_UB) = null
              row(ApproxColumn.ORDINAL_ABSERR) = null
              row(ApproxColumn.ORDINAL_RELERR) = null
            }
          }

      } else {
        if (baseAggregateType == ErrorAggregate.Count) {
          row(ApproxColumn.ORDINAL_ESTIMATE) = 0d
        }
      }
    } else {
      if (baseAggregateType == ErrorAggregate.Count) {
        row(ApproxColumn.ORDINAL_ESTIMATE) = 0d
      }
    }
    row
  }


  def getMultiplicityGroupingIndexesForBootstrap(numBootstrapTrials: Int): Array[Array[Int]] = {
    val numGroups = BootstrapMultiplicity.calculateMultiplicityResultSize(numBootstrapTrials)
    val groupingInfo: Seq[(Int, Int)] = for (i <- 0 until numGroups) yield {
      i * BootstrapMultiplicity.groupSize -> (
          if ((numBootstrapTrials - (i + 1) * BootstrapMultiplicity.groupSize) > 0) {
            (i + 1) * BootstrapMultiplicity.groupSize - 1
          } else {
            numBootstrapTrials - 1
          }
          )
    }

    Array.tabulate[Int](numGroups, 2){(i, j) => val tuple = groupingInfo(i)
      if(j == 0) {
       tuple._1
      } else {
      tuple._2
      }
    }
  }

}

