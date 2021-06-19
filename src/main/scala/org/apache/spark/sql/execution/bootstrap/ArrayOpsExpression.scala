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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

case class ArrayOpsExpression(array1: Expression, array2: Expression, opType: Int,
     behaviour : Int, numBootstrapTrials: Int, override val dataType: DataType) extends Expression {

  override def nullable: Boolean = true

  override def children: Seq[Expression] = array1 :: array2 :: Nil

  // Implement gen code in ApproxColumn
  override def eval(input: InternalRow): Any = {
    val a1 = array1.eval(input).asInstanceOf[DoubleMutableRow]
    val a2 = array2.eval(input).asInstanceOf[InternalRow]
    if (a1 != null && a2 != null) {
      BootstrapFunctions.operateOnArrays(a1, a2, opType, behaviour, numBootstrapTrials)
    } else {
      null
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val functionClass = BootstrapFunctions.getClass.getName
    val array1Eval = array1.genCode(ctx)
    val array2Eval = array2.genCode(ctx)
    val rowClass = classOf[DoubleMutableRow].getName
    val code = s"""
       boolean ${ev.isNull} = false;
       $rowClass ${ev.value} = null;
       """ +
        array1Eval.code +
        array2Eval.code +
        s"""

         ${ev.value} = $functionClass.MODULE$$.
            ${BootstrapFunctions.arrayOpsFunction}(($rowClass)${array1Eval.value},
        ${array2Eval.value},  $opType,   $behaviour, $numBootstrapTrials);

     """
    ev.copy(code = code)
  }
}

object ArrayOpsExpression {
  val ADD: Int = 0
  val DIVIDE: Int = 1

  val BEHAVIOUR_DEFAULT = 0
  val BEHAVIOUR_BOOTSTRAP_SUM = 1

}
