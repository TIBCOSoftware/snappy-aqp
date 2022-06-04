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
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._


case class ByteStructCreator(numGroups: Int) extends LeafExpression {

  def nullable: Boolean = false

  override val  dataType: DataType = BootstrapStructType(numGroups, ByteType,
    BootstrapStructType.multiplicityField, false)

  def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("eval not implemented")

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val className = classOf[ByteMutableRow].getName
    val code = s"""
      boolean ${ev.isNull} = false;
      $className ${ev.value} = new $className($numGroups);
    """
    ev.copy(code = code)
  }
}



case class DoubleStructCreator(numTrials: Int, isAQPDebug: Boolean) extends LeafExpression {

  def nullable: Boolean = false

  override val  dataType: DataType = BootstrapStructType(numTrials, DoubleType,
    BootstrapStructType.trialField)

  def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("eval not implemented")

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val className = if (isAQPDebug) classOf[DoubleMutableRowDebug].getName
                    else classOf[DoubleMutableRow].getName

    val code = s"""
      boolean ${ev.isNull} = false;
      $className ${ev.value} = new $className($numTrials);
    """
    ev.copy(code = code)
  }
}


object BootstrapStructType {

  val trialField = "bootstrap_trial_"
  val multiplicityField = "bootstrap_multiplicity_group_"

  def apply(numbootstrapTrials: Int, dataType: DataType, fieldPrefix: String,
            nullable: Boolean = true ): StructType =
    StructType(Seq.tabulate[StructField](numbootstrapTrials) ( i =>
      StructField(fieldPrefix + i  , dataType, nullable)))
}
