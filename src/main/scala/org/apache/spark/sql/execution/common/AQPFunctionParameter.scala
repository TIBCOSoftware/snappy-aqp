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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, ObjectType}

/**
 * Created by ashahid on 5/10/16.
 */
case class AQPFunctionParameter(val columns: Seq[Expression], val weight: MapColumnToWeight,
    actualAggregateDataType: DataType)
    extends Expression {
  override def foldable: Boolean = children.forall(_.foldable)

  override def children: Seq[Expression] = columns :+ weight

  override def dataType: DataType = columns.head.dataType // ArrayType(ObjectType(classOf[Any]))


  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    new GenericArrayData(children.map(_.eval(input)).toArray)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val arrayClass = classOf[GenericArrayData].getName
    val values = ctx.freshName("values")
   val code = s"""
           final boolean ${ev.isNull} = false;
           final Object[] $values = new Object[${children.size}];
         """ +
        children.zipWithIndex.map { case (e, i) =>
          val eval = e.genCode(ctx)
          eval.code +
              s"""
               if (${eval.isNull}) {
                 $values[$i] = null;
               } else {
                 $values[$i] = ${eval.value};
               }
              """
        }.mkString("\n") +
        s"final ArrayData ${ev.value} = new $arrayClass($values);"
    ev.copy(code = code, isNull = "false")
  }

  def headColumnDataType: DataType = columns.head.dataType

  override def prettyName: String = "AQPFunctionParameter"
}
