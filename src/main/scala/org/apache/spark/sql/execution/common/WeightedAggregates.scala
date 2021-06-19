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
package org.apache.spark.sql.execution.common

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Count, Sum}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{LogicalRDD, StratifiedSample}
import org.apache.spark.sql.internal.AnalyzerInvocation
import org.apache.spark.sql.sources.SamplingRelation
import org.apache.spark.sql.types._

case class WeightageRule(analyzerInvoc: AnalyzerInvocation) extends Rule[LogicalPlan] {
  // Transform the plan to changed the aggregates to weighted aggregates.
  // The hidden column is pulled from the StratifiedSample
  def apply(plan: LogicalPlan): LogicalPlan = if (analyzerInvoc.getCallStackCount > 0 ||
    !plan.resolved) {
    plan
  } else {
    val x = plan.transformUp {
      case p@PlaceHolderPlan(hidden, _, _, false) => p.copy(hiddenChild =
        applyWeightageRule(hidden), makeVisible = false)
    }
    val y = applyWeightageRule(x)
    y
  }

  def applyWeightageRule(plan: LogicalPlan): LogicalPlan = {
    val analysisType = plan.find {
      case _: ErrorAndConfidence => true
      // case _: SampleTableQuery => true
      case _ => false
    } match {
      case Some(x: ErrorAndConfidence) => x.analysisType
      // case Some(x: SampleTableQuery) => x.analysisType
      case _ => None
    }
    analysisType match {
      case Some(analysys) => plan transformUp {
        case aggr: Aggregate =>
          // By default allow all relation objects to be picked up for potential
          // candidates which can store sampled data.
          val sp = (aggr find {
            case a1: StratifiedSample => true
            case l@LogicalRelation(b: SamplingRelation, _, _) => true
            case _ => false
          })
          val samplingPlan = if (sp.isDefined) {
            sp
          } else {
            aggr find {
              case l@(LogicalRelation(_, _, _) | LogicalRDD(_, _, _, _))
                if l.output.exists(_.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME))
              => true
              case _ => false
            }
          }

          val hiddenCol = samplingPlan match {
            case Some(t: StratifiedSample) =>
              t.output.find(_.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME))
            // The aggregate is not on a StratifiedSample. No transformations needed.
            case Some(l@LogicalRelation(b: SamplingRelation, _, _)) =>
              val sqlContext = l.relation.sqlContext
              l.resolveQuoted(Utils.WEIGHTAGE_COLUMN_NAME,
                sqlContext.sessionState.analyzer.resolver)
            case Some(l: LogicalPlan) =>
              l.output.find(_.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME))
            case _ => None
          }

          hiddenCol match {
            case Some(col) =>
              val generatedRatioExpr = new MapColumnToWeight(col)
              aggr transformExpressions {
                // cheat code to run the query on sample table without applying weightages
                case alias@Alias(_, name)
                  if name.startsWith(Utils.SKIP_ANALYSIS_PREFIX) => alias
                // TODO: Extractors should be used to find the difference between the aggregate
                // and weighted aggregate functions instead of the unclean isInstance function
                case alias@Alias(e, name) =>
                  val expr = transformAggExprToWeighted(
                    e, generatedRatioExpr, analysys == AnalysisType.Closedform)
                  Alias(expr, name)(alias.exprId,
                    alias.qualifier, alias.explicitMetadata, alias.isGenerated)
              }
            case _ => aggr
          }
      }
      case None => plan
    }
  }

  private def checkArgsForWeight(exps: Seq[Expression]): Boolean =
    exps.exists {
      _.find {
        case _: MapColumnToWeight => true
        case _: AQPFunctionParameter => true
        case _ => false
      }.isDefined
    }

  def transformAggExprToWeighted(e: Expression,
      mapExpr: MapColumnToWeight, useClosedForm: Boolean): Expression = {
    val weightColumn = mapExpr
    /*
    if (useClosedForm) {
      mapExpr.convertToClosedFormUsable
    } else {
      mapExpr
    }
    */

    e transform {
      case aggr@Count(children) if !checkArgsForWeight(children) =>
        Count(AQPFunctionParameter(children, weightColumn,
          aggr.dataType))
      case aggr@Sum(arg) if !checkArgsForWeight(arg :: Nil) =>
        Sum(AQPFunctionParameter(arg :: Nil, weightColumn,
          aggr.dataType))
      case aggr@Average(arg) if !checkArgsForWeight(arg :: Nil) =>
        Average(AQPFunctionParameter(arg :: Nil, weightColumn,
          aggr.dataType))

      // TODO: This repetition is bad. Find a better way.
      case Add(left, right) =>
        Add(transformAggExprToWeighted(left, mapExpr, useClosedForm),
          transformAggExprToWeighted(right, mapExpr, useClosedForm))
      case Subtract(left, right) =>
        Subtract(transformAggExprToWeighted(left, mapExpr, useClosedForm),
          transformAggExprToWeighted(right, mapExpr, useClosedForm))
      case Divide(left, right) =>
        Divide(transformAggExprToWeighted(left, mapExpr, useClosedForm),
          transformAggExprToWeighted(right, mapExpr, useClosedForm))
      case Multiply(left, right) =>
        Multiply(transformAggExprToWeighted(left, mapExpr, useClosedForm),
          transformAggExprToWeighted(right, mapExpr, useClosedForm))
      case Remainder(left, right) =>
        Remainder(transformAggExprToWeighted(left, mapExpr, useClosedForm),
          transformAggExprToWeighted(right, mapExpr, useClosedForm))
      case Cast(left, dtype) =>
        Cast(transformAggExprToWeighted(left, mapExpr, useClosedForm), dtype)
      case Sqrt(left) =>
        Sqrt(transformAggExprToWeighted(left, mapExpr, useClosedForm))
      case Abs(left) =>
        Abs(transformAggExprToWeighted(left, mapExpr, useClosedForm))
      case UnaryMinus(left) =>
        UnaryMinus(transformAggExprToWeighted(left, mapExpr, useClosedForm))
    }
  }
}

case class CoalesceDisparateTypes(children: Seq[Expression])
    extends Unevaluable {

  override lazy val resolved = childrenResolved

  override def dataType: DataType = children.head.dataType

  override def nullable: Boolean = true

  // Coalesce is foldable if all children are foldable.
  override def foldable: Boolean = !children.exists(!_.foldable)

  override def toString: String =
    s"CoalesceDisparateTypes(${children.mkString(",")})"
}

/*
  final case class ClosedFormMapColumnToWeight(child: Expression) extends UnaryExpression {

  override def dataType: DataType = ObjectType(classOf[(Double, Long, Long, Long)])

  override def foldable: Boolean = child.foldable

  override def nullable: Boolean = true

  override def toString: String = s"ClosedFormMapColumnToWeight($child)"

  private[this] val boundReference = child match {
    case b: BoundReference => b
    case _ => null
  }

  override protected def doGenCode(ctx: CodegenContext,
    ev: ExprCode): String = {
    val eval = child.genCode(ctx)
    s"""
      ${eval.code}
      boolean ${ev.isNull} = false;
      scala.Tuple4 ${ev.value} = null;
      final long val;
      if (!${eval.isNull} && (val = ${eval.value}) != 0L) {
        final long left = (val >> 40) & 0xffffffffL;
        final long right = (val >> 8) & 0xffffffffL;
        ${ev.value} = (left != 0) ? ( new scala.Tuple4((double)right /
           (double)left, left, right, val)   )
        : new scala.Tuple4(1.0, 0 , 0 , 0);
      }
    """
  }

  override def eval(input: InternalRow): (Double, Long, Long, Long) = {
    MapColumnToWeight.detailedEval(input, boundReference, child)
  }
}
*/

final case class MapColumnToWeight(child: Expression)
    extends UnaryExpression {

  override def dataType: DataType = DoubleType

  override def foldable: Boolean = child.foldable

  override def nullable: Boolean = true

  override def toString: String = s"MapColumnToWeight($child)"

  def rawWeight: Attribute = this.child.asInstanceOf[NamedExpression].toAttribute

  private[this] val boundReference: BoundReference = child match {
    case b: BoundReference => b
    case _ => null
  }

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val value = ctx.freshName("val")
    val left = ctx.freshName("left")
    val right = ctx.freshName("right")
    val code =
      s"""
      ${eval.code}
      boolean ${ev.isNull} = false;
      double ${ev.value} = 1.0;
      final long $value;
      $value = ${eval.value};
      final long $left = ($value >> 40) & 0xffffffffL;
      final long $right = ($value >> 8) & 0xffffffffL;
      ${ev.value} = ($left != 0) ? ((double)$right / (double)$left) : 1.0;
    """
    ev.copy(code = code)
  }

  override def eval(input: InternalRow): Double = {
    MapColumnToWeight.detailedEval(input, boundReference, child)._1
  }

  // def convertToClosedFormUsable: ClosedFormMapColumnToWeight =
  //   ClosedFormMapColumnToWeight(child)
}

object MapColumnToWeight {

  def detailedEval(input: InternalRow, boundRef: BoundReference,
      child: Expression): (Double, Long, Long, Long) = {
    if (boundRef != null) {
      try {
        val value = input.getLong(boundRef.ordinal)
        if (value != 0 || !input.isNullAt(boundRef.ordinal)) {
          val left = (value >> 40) & 0xffffffffL
          val right = (value >> 8) & 0xffffffffL

          if (left != 0) (right.toDouble / left, left, right, value)
          else (1.0, left, right, value)
        }
        else {
          (1.0, 0, 0, 0)
        }
      } catch {
        case NonFatal(e) => (1.0, 0, 0, 0)
      }
    }
    else {
      val evalE = child.eval(input)
      if (evalE != null) {
        val value = evalE.asInstanceOf[Long]
        val left = (value >> 40) & 0xffffffffL
        val right = (value >> 8) & 0xffffffffL

        if (left != 0) (right.toDouble / left, left, right, value)
        else (1.0, left, right, value)
      } else {
        (1.0, 0, 0, 0)
      }
    }
  }
}
