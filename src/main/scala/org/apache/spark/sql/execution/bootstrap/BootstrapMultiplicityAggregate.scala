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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.aggregate.GroupAggregate
import org.apache.spark.sql.types._

case class BootstrapMultiplicityAggregate(weight: Expression,
    bootstrapMultiplicities: Expression, numBootstrapTrials: Int)
  extends DeclarativeAggregate with GroupAggregate {

  override def children: Seq[Expression] = Seq(weight)

  override def nullable: Boolean = false

  override def dataType: DataType = ByteType

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)

  // @transient lazy private val arraySize =
  //   BootstrapMultiplicity.calculateMultiplicityResultSize(numBootstrapTrials)

  @transient lazy val numGroups = BootstrapMultiplicity.
    calculateMultiplicityResultSize(numBootstrapTrials)
  @transient lazy val zero = Cast(Literal(0), ByteType)
  @transient lazy val zeroArray = ByteStructCreator(numGroups)


  lazy private val state = AttributeReference(BootstrapMultiplicityAggregate.partialAttributeName,
    zeroArray.dataType, nullable = false)()

  @transient override lazy val aggBufferAttributes = Seq(state)

  @transient override lazy val initialValues: Seq[Expression] = Seq(zeroArray)

  override lazy val aggBufferAttributesForGroup: Seq[AttributeReference] = {
   aggBufferAttributes.map(attr => attr.copy(nullable = false)(
      attr.exprId, attr.qualifier, attr.isGenerated))
  }

  @transient override lazy val initialValuesForGroup = initialValues


  @transient override lazy val updateExpressions: Seq[Expression] = {
    val precomputedExps = weight :: bootstrapMultiplicities :: state :: Nil
    Seq(UpdateMultiplicityExpression(precomputedExps, numBootstrapTrials,
       zeroArray.dataType))
  }


  @transient override lazy val mergeExpressions: Seq[Expression] =
    Seq(MergeMultiplicityExpression(state.left,
      state.right, numGroups, numBootstrapTrials, zeroArray.dataType))

  @transient override lazy val evaluateExpression: Expression =
    BootstrapResultEvalAndCache(state)

  override def toString: String = BootstrapMultiplicityAggregate.name
}

object BootstrapMultiplicityAggregate {

  val name = "BootstrapMultiplicityAgg"
  val partialAttributeName = "BS-Multiplicity-state"
  val cachedBootstrapResultField = "cached_bootstrap_result"
  val cachedBootstrapCurrentState = "cached_bootstrap_current_state"
  val cachedBootstrapCurrentSeed = "cached_bootstrap_current_seed"
  val cachedBootstrapSeedArray = "cached_bootstrap_seeds"
  val cachedWeightField = "weight"
  val columnBatchCount = "currentBatchCount"
  val storedBoostrapHolder = "storedBSHolder"
  val isPreviousAggregateBootstrapMultiplicty = "isPreviousAggregateBootstrapMultiplicty"

}


case class UpdateMultiplicityExpression(children: Seq[Expression],
    numBootstrapTrials: Int, override val dataType: DataType)  extends Expression {

  override def nullable: Boolean = false

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val state = children.last
    val weightExpression = children.head
    val bootstrapExpression = children(1)

    val stateClass = classOf[ByteMutableRow].getName
    val doubleMutableRowClass = classOf[DoubleMutableRow].getName
    val fullSatisfiedState = BootstrapMultiplicity.getFullySatisfiedCount(numBootstrapTrials)

    ctx.addMutableState("int", BootstrapMultiplicityAggregate
        .cachedBootstrapCurrentSeed, s"${BootstrapMultiplicityAggregate
        .cachedBootstrapCurrentSeed} = 0;")


    ctx.addMutableState("double", BootstrapMultiplicityAggregate
        .cachedWeightField, s"${BootstrapMultiplicityAggregate
        .cachedWeightField} = 0d;")

    ctx.addMutableState("byte[]", BootstrapMultiplicityAggregate
        .cachedBootstrapCurrentState, s"${BootstrapMultiplicityAggregate
        .cachedBootstrapCurrentState} = null;")

    ctx.addMutableState("int", BootstrapMultiplicityAggregate
      .columnBatchCount, s"${BootstrapMultiplicityAggregate
      .columnBatchCount} = 0;")

    ctx.addMutableState(stateClass, BootstrapMultiplicityAggregate
      .storedBoostrapHolder, s"${BootstrapMultiplicityAggregate
      .storedBoostrapHolder} = null;")

    ctx.addMutableState("int[]", BootstrapMultiplicityAggregate
      .cachedBootstrapSeedArray, s"${BootstrapMultiplicityAggregate
      .cachedBootstrapSeedArray} = new int[${DoubleMutableRow.cacheSize}];")


    val stateEval = state.genCode(ctx)
    val bootstrapEval = bootstrapExpression.genCode(ctx)
    val weightEval = weightExpression.genCode(ctx)
    val code = s"""
      boolean ${BootstrapMultiplicityAggregate.isPreviousAggregateBootstrapMultiplicty} = true;
      ${bootstrapEval.code}
      ${stateEval.code}
      $stateClass currentState = ($stateClass)${stateEval.value};

      ${BootstrapMultiplicityAggregate.cachedBootstrapCurrentSeed} =
         ${bootstrapEval.value};
      ${BootstrapMultiplicityAggregate.cachedBootstrapCurrentState} =  currentState.values();
     """ +
      s"""
       ${weightEval.code}
       ${BootstrapMultiplicityAggregate.cachedWeightField} = ${weightEval.value};

       if(${BootstrapMultiplicityAggregate.storedBoostrapHolder} != currentState) {
         ${BootstrapMultiplicityAggregate.storedBoostrapHolder} = currentState;
         ${BootstrapMultiplicityAggregate.columnBatchCount} = 1;
       }else {
         if (${BootstrapMultiplicityAggregate.columnBatchCount} == ${DoubleMutableRow.cacheSize}) {
           ${BootstrapMultiplicityAggregate.columnBatchCount} = 1;
         }else {
           ${BootstrapMultiplicityAggregate.columnBatchCount} += 1;
         }
       }


       boolean ${ev.isNull} = false;
       $stateClass ${ev.value} = currentState;
     """
    ev.copy(code = code)
  }

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("not expected to be invoked")
}

case class MergeMultiplicityExpression(leftArray: Expression,
    rightArray: Expression, numGroups: Int,
    numBootstrapTrials: Int, override val dataType: DataType) extends Expression {

  override def nullable: Boolean = false



  override def children: Seq[Expression] = leftArray :: rightArray :: Nil

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val leftArrayEval = leftArray.genCode(ctx)
    val rightArrayEval = rightArray.genCode(ctx)
    val stateClass = classOf[ByteMutableRow].getName
    val rowClass = classOf[InternalRow].getName
    val code = s"""
      ${leftArrayEval.code}
      ${rightArrayEval.code}
      boolean ${ev.isNull} = false;
      $stateClass leftArray =  ($stateClass)${leftArrayEval.value};
      $rowClass rightArray = ${rightArrayEval.value};
      byte[] leftVals = leftArray.values();
      int len = leftArray.numFields();

      for (int k = 0; k < len; ++k) {
        leftVals[k] = (byte) (leftVals[k] | rightArray.getByte(k));
      }
      $stateClass ${ev.value} =  leftArray;
    """
    ev.copy(code = code)
  }

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("not expected to be invoked")
}

case class BootstrapResultEvalAndCache(child: Expression)
    extends UnaryExpression {

  override def nullable: Boolean = false

  override def dataType: DataType = ByteType

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val resultClass = classOf[ByteMutableRow].getName
    ctx.addMutableState(resultClass, BootstrapMultiplicityAggregate
        .cachedBootstrapResultField, s"${BootstrapMultiplicityAggregate
        .cachedBootstrapResultField} = null;")
    val eval = child.genCode(ctx)
    val code = s"""
      ${eval.code}
      ${BootstrapMultiplicityAggregate.cachedBootstrapResultField} =
       ($resultClass) ${eval.value};
      boolean ${ev.isNull} = false;
      byte ${ev.value} = 0;
    """
    ev.copy(code = code)
  }

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("not expected to be invoked")
}
