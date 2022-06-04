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
package org.apache.spark.sql.execution.closedform

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.common.HAC
import org.apache.spark.sql.types.{DataType, Metadata}

case class ClosedFormColumnExtractor(child: Expression, name: String,
    confidence: Double, confFactor: Double,
    aggType: ErrorAggregate.Type, error: Double, dataType: DataType,
    behavior: HAC.Type, override val nullable: Boolean)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Option[String] = None)
    extends UnaryExpression with NamedExpression  {
  // Alias(Generator, xx) need to be transformed into Generate(generator, ...)
  override lazy val resolved = true

  override def eval(input: InternalRow): Any = {
    val errorStats = child.eval(input).asInstanceOf[ClosedFormStats]
    val retVal: Double = StatCounterUDTCF.finalizeEvaluation(errorStats,
      confidence, confFactor, aggType, error, behavior)
    if (retVal.isNaN) null else retVal
  }

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childEval = child.genCode(ctx)
    val statClass = classOf[ClosedFormStats].getName
    val statVar = ctx.freshName("errorStats")
    val returnValue = ctx.freshName("returnValue")
    val statCounterUDTF = StatCounterUDTCF.getClass.getName
    val behaviorString = HAC.getBehaviorAsString(behavior)
    val hacClass = HAC.getClass.getName
    val aggTypeStr = aggType.toString
    val aggTypeClass = ErrorAggregate.getClass.getName

    val code = childEval.code +
        s"""
       $statClass $statVar = ($statClass)${childEval.value};
       double $returnValue = $statCounterUDTF.MODULE$$.finalizeEvaluation($statVar,
       $confidence, $confFactor,$aggTypeClass.MODULE$$.withName("$aggTypeStr"), $error,
         $hacClass.MODULE$$.getBehavior("$behaviorString"));
       boolean ${ev.isNull} = Double.isNaN($returnValue);
       double ${ev.value} = $returnValue;
         """
    ev.copy(code = code)

  }

  override def metadata: Metadata = Metadata.empty

  override def toAttribute: Attribute =
    if (resolved) {
      AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)
    } else {
      UnresolvedAttribute(name)
    }

  override def toString: String = s"$child AS $name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId :: qualifier :: Nil
  }

  override def equals(other: Any): Boolean = other match {
    case a: Alias =>
      name == a.name && exprId == a.exprId && child == a.child

    case _ => false
  }

  /** Returns a copy of this expression with a new `exprId`. */
  override def newInstance(): NamedExpression = ClosedFormColumnExtractor(
    child, name, confidence, confFactor, aggType, error, dataType, behavior,
    nullable)( qualifier = qualifier)
}
