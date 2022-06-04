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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.types.{DataType, DecimalType, DoubleType, NumericType}

case class BootstrapAddTrialsAsArray( actualColumn: Expression, buffer: Expression, numTrials: Int,
                                      opType: Int, isAQPDebug: Boolean,
                                      override val dataType: DataType) extends Expression {

  override def nullable: Boolean = true

  override def deterministic: Boolean = false

  override def children: Seq[Expression] = actualColumn :: buffer :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException()

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val bufferEval = buffer.genCode(ctx)
    val actualColumnEval = actualColumn.genCode(ctx)
    val const = ctx.freshName("constantDouble")
    val mutableRowClass = if (isAQPDebug) {
      classOf[DoubleMutableRowDebug].getName
    } else {
      classOf[DoubleMutableRow].getName
    }
    val holder = ctx.freshName("holderMutableRow")
    val functionClass = BootstrapFunctions.getClass.getName

    ctx.addMutableState(s"$mutableRowClass", holder,
      s"$holder = null;")

    val addValue = s"""
          if (${BootstrapMultiplicityAggregate.isPreviousAggregateBootstrapMultiplicty}) {
             $holder.add($const, ${BootstrapMultiplicityAggregate.columnBatchCount},
             ${BootstrapMultiplicityAggregate.cachedBootstrapCurrentSeed});
          } else {
            $holder.add($const, ${BootstrapMultiplicityAggregate.columnBatchCount} );
          }
       """

    val addNullValue = s"""
          if (${BootstrapMultiplicityAggregate.isPreviousAggregateBootstrapMultiplicty}) {
            $holder.addNull(${BootstrapMultiplicityAggregate.columnBatchCount},
                 ${BootstrapMultiplicityAggregate.cachedBootstrapCurrentSeed});
          } else {
            $holder.addNull(${BootstrapMultiplicityAggregate.columnBatchCount} );
          }
      """
    val processFirstAggHolder = s"""
       if (${BootstrapMultiplicityAggregate.isPreviousAggregateBootstrapMultiplicty}) {
         $holder.setStateArray(${BootstrapMultiplicityAggregate.cachedBootstrapCurrentState});
         ${BootstrapMultiplicityAggregate.storedBoostrapHolder}.setFirstAggregateMutableRow(
           $holder);
       }
     """
    val processHolderChange = s"""
      $holder = ($mutableRowClass)${bufferEval.value};
      $holder.setSeedArray(${BootstrapMultiplicityAggregate.cachedBootstrapSeedArray});
      $processFirstAggHolder
       """
    val codeNullCaseSum = s"""
             ${ev.isNull} = false;
             if(${bufferEval.isNull}) {
               ${bufferEval.value} =  new $mutableRowClass(null, $numTrials);
             }
             if($holder != ${bufferEval.value}) {
               $processHolderChange
             }
             $addNullValue
             ${ev.value} = ($mutableRowClass)${bufferEval.value};
         """


    val code = s"""
      boolean ${ev.isNull} = false;
      $mutableRowClass ${ev.value} = null ;
      ${actualColumnEval.code}
      ${bufferEval.code}
      if(${BootstrapMultiplicityAggregate.columnBatchCount} == 1 && ${holder} != null) {
        $holder.store();
      }
      if(!${actualColumnEval.isNull} || $opType == ${BootstrapAddTrialsAsArray.BootstrapCount}) {
         double $const = """ +
        (if (opType == BootstrapAddTrialsAsArray.BootstrapCount) {
          s"""  ${BootstrapMultiplicityAggregate.cachedWeightField}; """
        } else {
          s"""${actualColumnEval.value} * ${BootstrapMultiplicityAggregate.cachedWeightField};"""

        }) +
        s"""
        if (${bufferEval.isNull}) {
          ${bufferEval.value} =  new $mutableRowClass($numTrials);
        }

        ${ev.value} = ($mutableRowClass)${bufferEval.value};
        if($holder != ${bufferEval.value}) {
          $processHolderChange
        }
        $addValue
      } else {
        $codeNullCaseSum
      }
      ${BootstrapMultiplicityAggregate.isPreviousAggregateBootstrapMultiplicty} = false;
     """
    ev.copy(code = code)
  }
}

object BootstrapAddTrialsAsArray {
  val Default: Int = 0
  val BootstrapSum: Int = 1
  val BootstrapAverage_SUM: Int = 2
  val BootstrapCount: Int = 3

  def castToDouble(column: Expression): Expression = {
    val colDataType = column.dataType
    if (NumericType.acceptsType(colDataType)) {
      if (DecimalType.acceptsType(colDataType)) {
        Cast(column, DoubleType)
      } else {
        column
      }
    } else {
      Cast(column, DoubleType)
    }
  }

}
