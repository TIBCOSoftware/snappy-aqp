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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType}

case class BootstrapAddTrialsAvgCountPart( actualColumn: Expression, buffer: Expression,
    numTrials: Int, sumBuffer: Expression, override val dataType: DataType)
  extends Expression {

  override def nullable: Boolean = true
  override def children: Seq[Expression] = actualColumn ::
      buffer :: sumBuffer :: Nil
  override def deterministic: Boolean = false
  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException()

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val bufferEval = buffer.genCode(ctx)
    val sumBufferEval = sumBuffer.genCode(ctx)
    val actualColumnEval = actualColumn.genCode(ctx)
   // val const = ctx.freshName("constantDouble")
    val countHolder = ctx.freshName("holderMutableRowAvgCount")
    val sumHolder = ctx.freshName("holderMutableRowAvgSum")
    val holderChanged = ctx.freshName("holderChanged")

    val sumVal = ctx.freshName("sumValue")
    val countVal = ctx.freshName("countValue")

    val functionClass = BootstrapFunctions.getClass.getName
    val mutableRowClass = classOf[DoubleMutableRow].getName


    ctx.addMutableState(s"$mutableRowClass", countHolder,
      s"$countHolder = null;")

    ctx.addMutableState(s"$mutableRowClass", sumHolder,
      s"$sumHolder = null;")

    val processFirstAggHolder = s"""
      if( ${BootstrapMultiplicityAggregate.isPreviousAggregateBootstrapMultiplicty}) {
        $countHolder.setStateArray(${BootstrapMultiplicityAggregate.cachedBootstrapCurrentState});
        ${BootstrapMultiplicityAggregate.storedBoostrapHolder}.setFirstAggregateMutableRow(
        $countHolder);
      }
     """


    val processHolderChange = s"""
      $countHolder = ($mutableRowClass)${bufferEval.value};
      $sumHolder = ($mutableRowClass)${sumBufferEval.value};
      $countHolder.setSeedArray(${BootstrapMultiplicityAggregate.cachedBootstrapSeedArray});
      $sumHolder.setSeedArray(${BootstrapMultiplicityAggregate.cachedBootstrapSeedArray});
      $processFirstAggHolder
       """

    val addValue = s"""
       if( ${BootstrapMultiplicityAggregate.isPreviousAggregateBootstrapMultiplicty}) {
         $sumHolder.add($sumVal, ${BootstrapMultiplicityAggregate.columnBatchCount});
         $countHolder.add($countVal, ${BootstrapMultiplicityAggregate.columnBatchCount},
           ${BootstrapMultiplicityAggregate.cachedBootstrapCurrentSeed});
       }else {
         $sumHolder.add($sumVal, ${BootstrapMultiplicityAggregate.columnBatchCount} );
         $countHolder.add($countVal, ${BootstrapMultiplicityAggregate.columnBatchCount});
       }
       """


      val code =
        s"""
      boolean ${ev.isNull} = false;
      $mutableRowClass ${ev.value} = null;
      ${actualColumnEval.code}
      ${bufferEval.code}
      ${sumBufferEval.code}
      if(${BootstrapMultiplicityAggregate.columnBatchCount} == 1 && ${countHolder} != null) {
        $countHolder.store($sumHolder);
      }
      ${ev.value} = ($mutableRowClass)${bufferEval.value};


      if($countHolder != ${bufferEval.value}) {
         $processHolderChange
      }

      double $sumVal = 0d, $countVal = 0d;
      if (!${actualColumnEval.isNull} ) {
        $sumVal = ${actualColumnEval.value} * ${BootstrapMultiplicityAggregate.cachedWeightField};
        $countVal = ${BootstrapMultiplicityAggregate.cachedWeightField};
      }
      $addValue
      ${BootstrapMultiplicityAggregate.isPreviousAggregateBootstrapMultiplicty} = false;
      """
      ev.copy(code = code)

  }
}
