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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

case class StatCounterAggregatorCreator(aggType: ErrorAggregate.Type) extends LeafExpression {

  def nullable: Boolean = false

  def dataType: DataType = StatCounterUDT

  def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("eval not implemented")

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val instanceClass = classOf[StatCounterAggregator].getName
    val errorAggregateClass = ErrorAggregate.getClass.getName
    val aggTypeForJava = s"""$errorAggregateClass.MODULE$$.$aggType()"""
    // strip double quotes , we will add later with escape characters
    val code = s"""
      boolean ${ev.isNull} = false;
      $instanceClass ${ev.value} = new $instanceClass( $aggTypeForJava);
    """
    ev.copy(code = code)
  }
}
