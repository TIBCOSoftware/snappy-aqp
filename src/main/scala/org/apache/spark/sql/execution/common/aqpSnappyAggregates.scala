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

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.{SnappyHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.bootstrap.{ApproxColumn, ApproxColumnExtractor, DeclarativeBootstrapAggregateFunction}
import org.apache.spark.sql.execution.closedform.{ClosedFormColumnExtractor, ClosedFormErrorEstimate, ErrorAggregate}
import org.apache.spark.sql.execution.common.ResultsExpressionCreator.createColumnExtractorSubstituedExpression
import org.apache.spark.sql.types.DoubleType

final class SnappySortAggregate(
    _requiredChildDistributionExpressions: Option[Seq[Expression]],
    _groupingExpressions: Seq[NamedExpression],
    _aggregateExpressions: Seq[AggregateExpression],
    _aggregateAttributes: Seq[Attribute],
    _initialInputBufferOffset: Int,
    val baseResultExpressions: Seq[NamedExpression],
    dumbo: Boolean = true, _child: SparkPlan, val relativeErrorsExprIDs: Option[Seq[ExprId]])
    extends SortAggregateExec(_requiredChildDistributionExpressions,
      _groupingExpressions, _aggregateExpressions, _aggregateAttributes,
      _initialInputBufferOffset, Nil, _child) {


  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  override lazy val resultExpressions = ResultsExpressionCreator
      .createResultExpressions(aggregateAttributes, baseResultExpressions,
        aggregateExpressions, groupingExpressions, relativeErrorsExprIDs)

  override def productElement(n: Int): Any = n match {
    case 0 => _requiredChildDistributionExpressions
    case 1 => _groupingExpressions
    case 2 => _aggregateExpressions
    case 3 => _aggregateAttributes
    case 4 => _initialInputBufferOffset
    case 5 => baseResultExpressions
    case 6 => dumbo
    case 7 => _child
    case 8 => relativeErrorsExprIDs
  }

  override def productArity: Int = 9


}

final class SnappyHashAggregate(
    _requiredChildDistributionExpressions: Option[Seq[Expression]],
    _groupingExpressions: Seq[NamedExpression],
    _aggregateExpressions: Seq[AggregateExpression],
    _aggregateAttributes: Seq[Attribute],
    val baseResultExpressions: Seq[NamedExpression],
    dumbo: Boolean = true,
    _child: SparkPlan, _hasDistinct: Boolean,
    val relativeErrorsExprIDs: Option[Seq[ExprId]]) extends SnappyHashAggregateExec(
  _requiredChildDistributionExpressions,
  _groupingExpressions, _aggregateExpressions, _aggregateAttributes,
  Nil, _child, _hasDistinct) {

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */

  override lazy val resultExpressions = ResultsExpressionCreator.createResultExpressions(
    aggregateAttributes, baseResultExpressions, aggregateExpressions,
    groupingExpressions, relativeErrorsExprIDs)

  override def productElement(n: Int): Any = n match {
    case 0 => requiredChildDistributionExpressions
    case 1 => groupingExpressions
    case 2 => aggregateExpressions
    case 3 => aggregateAttributes
    case 4 => baseResultExpressions
    case 5 => dumbo
    case 6 => child
    case 7 => hasDistinct
    case 8 => relativeErrorsExprIDs
  }

  override def productArity: Int = 9

}

final class SnappyPartialHashAggregate(
    _requiredChildDistributionExpressions: Option[Seq[Expression]],
    _groupingExpressions: Seq[NamedExpression],
    _aggregateExpressions: Seq[AggregateExpression],
    _aggregateAttributes: Seq[Attribute],
    _baseResultExpressions: Seq[NamedExpression],
    dumbo: Boolean = true,
    _child: SparkPlan, _hasDistinct: Boolean) extends SnappyHashAggregateExec(
  _requiredChildDistributionExpressions,
  _groupingExpressions, _aggregateExpressions, _aggregateAttributes,
  Nil, _child, _hasDistinct) {

  @transient
  override lazy val resultExpressions: Seq[Attribute] = {
    this.groupingExpressions.map(_.toAttribute) ++
        this.aggregateExpressions.flatMap {
          _.collectFirst[DeclarativeAggregate] {
            case func: DeclarativeAggregate => func
          }.get.inputAggBufferAttributes
        }
  }

  /*
  private lazy val compressedNCA : Seq[(Attribute, Int)] = {
    val buffer = scala.collection.mutable.ArrayBuffer[(Attribute,  Int)]()
    var currentBaseAttribute: Attribute = null
    var count = 0
    for( attrib <- _nonCompleteAggregateAttributes) {
      if (currentBaseAttribute == null) {
        currentBaseAttribute = attrib
        count = 0
      } else if(currentBaseAttribute.name == attrib.name) {
        if (attrib.exprId.id == currentBaseAttribute.exprId.id + count + 1) {
          count = count + 1
        } else {
          buffer += currentBaseAttribute -> count
          currentBaseAttribute = attrib
          count = 0
        }
      } else {
        buffer += currentBaseAttribute -> count
        currentBaseAttribute = attrib
        count = 0
      }
    }
    buffer += currentBaseAttribute -> count
    buffer.toSeq
  }

   @transient override lazy val nonCompleteAggregateAttributes = {
    compressedNCA.flatMap {
      case(attrib, count) => Seq.tabulate[Attribute](count + 1)(i => {
        attrib.asInstanceOf[AttributeReference]. withExprId(ExprId(
          attrib.exprId.id + i, attrib.exprId.jvmId))
      })
    }
  }
  */

  @transient
  override val aggregateAttributes: Seq[AttributeReference] = aggregateExpressions.flatMap(
    _.aggregateFunction match {
      case da: DeclarativeAggregate => da.inputAggBufferAttributes
      case _ => Nil
    }
  )

  override def productElement(n: Int): Any = n match {
    case 0 => requiredChildDistributionExpressions
    case 1 => groupingExpressions
    case 2 => aggregateExpressions
    case 3 => aggregateAttributes
    case 4 => resultExpressions
    case 5 => dumbo
    case 6 => child
    case 7 => hasDistinct
  }

  override def productArity: Int = 8
}

final class SnappyPartialSortedAggregate(
    _requiredChildDistributionExpressions: Option[Seq[Expression]],
    _groupingExpressions: Seq[NamedExpression],
    _aggregateExpressions: Seq[AggregateExpression],
    _aggregateAttributes: Seq[Attribute],
    _initialInputBufferOffset: Int,
    _baseResultExpressions: Seq[NamedExpression],
    dumbo: Boolean = true,
    _child: SparkPlan) extends SortAggregateExec(
  _requiredChildDistributionExpressions, _groupingExpressions,
  _aggregateExpressions, _aggregateAttributes,
  _initialInputBufferOffset, Nil, _child) {

  @transient override lazy val resultExpressions: Seq[Attribute] = {
    this.groupingExpressions.map(_.toAttribute) ++
        this.aggregateExpressions.flatMap {
          _.collectFirst[DeclarativeAggregate] {
            case func: DeclarativeAggregate => func
          }.get.inputAggBufferAttributes
        }
  }

  @transient
  override val aggregateAttributes: Seq[AttributeReference] = aggregateExpressions.flatMap(
    _.aggregateFunction match {
      case da: DeclarativeAggregate => da.inputAggBufferAttributes
      case _ => Nil
    }
  )

  override def productElement(n: Int): Any = n match {
    case 0 => requiredChildDistributionExpressions
    case 1 => groupingExpressions
    case 2 => aggregateExpressions
    case 3 => aggregateAttributes
    case 4 => initialInputBufferOffset
    case 5 => resultExpressions
    case 6 => dumbo
    case 7 => child
  }

  override def productArity: Int = 8
}


object ResultsExpressionCreator {
  val hiddenColPrefix = "hidden_column_"

  def createResultExpressions(nonCompleteAggregateAttributes: Seq[Attribute],
      baseResultExpressions: Seq[NamedExpression],
      nonCompleteAggregateExpressions: Seq[AggregateExpression],
      groupingExpressions: Seq[NamedExpression],
      relativeErrorsExprIDs: Option[Seq[ExprId]]): Seq[NamedExpression] = {
    val mapping =
      scala.collection.mutable.HashMap[Int, Seq[(ErrorAggregate.Type, String,
          NamedExpression, Double, Double, Double, ExprId, Boolean, HAC.Type, NamedExpression)]]()
    val baseAggregates = scala.collection.mutable.ArrayBuffer[(Attribute,
        Double, Double, ErrorAggregate.Type, Double, Boolean, HAC.Type)]()

    val zippedNCA = nonCompleteAggregateAttributes.zipWithIndex
    var index = 0
    var baseIndex = 0
    val results = baseResultExpressions.flatMap { namedExp =>
      val newExp = namedExp
      if (newExp.name == "havingCondition") {
        val modifiedExp = zippedNCA.find {
          case (ar, _) => newExp.find {
            case named: NamedExpression => named.exprId == ar.exprId
            case _ => false
          } match {
            case Some(_) => true
            case None => false
          }
        } match {
          case Some((actualExp, idx)) => nonCompleteAggregateExpressions(idx) match {
            case AggregateExpression(errEst: ClosedFormErrorEstimate, _, _, _) =>
              newExp transformUp {
                case ne: NamedExpression if ne.exprId == actualExp.exprId =>
                  ClosedFormColumnExtractor(actualExp,
                    namedExp.name, errEst.confidence, errEst.confFactor,
                    errEst.aggregateType, errEst.error,
                    DoubleType, errEst._behavior, errEst.aggregateType != ErrorAggregate.Count)()
              }
            case AggregateExpression(bsFunc: DeclarativeBootstrapAggregateFunction,
            _, _, _) =>
              newExp transformUp {
                case ne: NamedExpression if ne.exprId == actualExp.exprId =>
                  ApproxColumnExtractor(actualExp, actualExp.name,
                    ApproxColumn.ORDINAL_ESTIMATE, DoubleType, bsFunc.nullable)()
                // actualExp.dataType.asInstanceOf[StructType]
                // (ApproxColumn.ORDINAL_ESTIMATE).dataType)()
              }
            case _ => newExp
          }
          case None => newExp
        }
        Seq(modifiedExp.asInstanceOf[NamedExpression])

      } else {
        // The starting aggregates may be the "having" ones so skip those
        index = index + 1
        baseIndex = baseIndex + 1

        // find the index in non complete aggregate attribs to which
        // this position of AttribRef refers t
        val usedNCA = zippedNCA.filter{
          case (ar, _) => newExp.find {
            case named: NamedExpression => named.exprId == ar.exprId
            case _ => false
          } match {
            case Some(_) => true
            case None => false
          }
        }

        if (usedNCA.isEmpty) {
          val exprToUse = newExp
          if (mapping.contains(index)) {
            var seq = new scala.collection.mutable.ArrayBuffer[NamedExpression]()
            seq += exprToUse
            while (mapping.get(index) match {
              case Some(elements) =>
                index = index + 1
                seq += createColumnExtractorSubstituedExpression(elements)
                true
              case None => false
            }) {}
            seq
          } else {
            Seq(exprToUse)
          }
        } else {
          var seq = new scala.collection.mutable.ArrayBuffer[NamedExpression]()
          seq += newExp
          usedNCA.foldLeft[scala.collection.mutable.ArrayBuffer[NamedExpression]](seq) {
            case (seq, (actualExp, idx)) =>
              nonCompleteAggregateExpressions(idx) match {
                case AggregateExpression(errEst: ErrorAggregateFunction, _, _, _) =>
                  val isClosedForm = errEst match {
                    case _: DeclarativeBootstrapAggregateFunction => false
                    case _ => true

                  }
                  errEst.errorEstimateProjs.foreach {
                    case (indx, aggType, projName, _, exprId, rootExp) =>
                      val seq = mapping.getOrElse(indx, Nil)

                      mapping.put(indx, (aggType, projName, actualExp,
                        errEst.confidence, errEst.confFactor, errEst.error,
                        exprId, isClosedForm, errEst.behavior, rootExp) +: seq)
                  }


                  val modifiedExp = seq(0) transformUp {
                    case ne: NamedExpression if ne.exprId == actualExp.exprId =>
                      if (AQPRules.requireHiddenRelativeErrors(errEst.behavior)) {
                        baseAggregates += ((actualExp, errEst.confidence, errEst.confFactor,
                          ErrorAggregate.getRelativeErrorTypeForBaseType(errEst.aggregateType),
                          errEst.error, isClosedForm, errEst.behavior))
                      }

                      if (isClosedForm) {
                        ClosedFormColumnExtractor(actualExp,
                          namedExp.name, errEst.confidence, errEst.confFactor,
                          errEst.aggregateType, errEst.error, DoubleType, errEst.behavior,
                          errEst.aggregateType != ErrorAggregate.Count)()

                      } else {
                        ApproxColumnExtractor(actualExp, actualExp.name,
                          ApproxColumn.ORDINAL_ESTIMATE, DoubleType, errEst.nullable)()
                      }
                  }

                  seq(0) = modifiedExp.asInstanceOf[NamedExpression]

                  while (mapping.get(index) match {
                    case Some(elements) =>
                      index = index + 1
                      seq += createColumnExtractorSubstituedExpression(elements)
                      true
                    case None => false

                  }) {}
                  seq

                case _ =>
                  if (mapping.contains(index)) {
                    while (mapping.get(index) match {
                      case Some(elements) =>
                        index = index + 1
                        seq += createColumnExtractorSubstituedExpression(elements)
                        true
                      case None => false
                    }) {}
                    seq
                  } else {
                    seq
                  }
              }

          }
        }
      }
    }
    // }
    results ++
        (if (baseAggregates.nonEmpty &&
          AQPRules.requireHiddenRelativeErrors(baseAggregates(0)._7)) {
          val exprIDs = relativeErrorsExprIDs.get
          baseAggregates.zipWithIndex.map {
            case ((actualExp, confi, confFact, aggType, error, isClosedForm,
            behavior), idx) => if (isClosedForm) {
              ClosedFormColumnExtractor(actualExp, hiddenColPrefix + idx, confi, confFact,
                aggType, error, DoubleType, behavior, true)(exprIDs(idx))
            } else {
              ApproxColumnExtractor(actualExp, hiddenColPrefix + idx,
                ApproxColumn.getOrdinal(aggType), DoubleType, true)(exprIDs(idx))
            }
          }
        } else {
          Nil
        }
            ) ++
        (if (baseAggregates.nonEmpty && AQPRules.requireHiddenGroupBy(baseAggregates(0)._7)) {
          groupingExpressions.map(_.toAttribute)
        } else {
          Nil
        }
            )
  }

  def errorEstimateAttributes(baseResultExpressions: Seq[NamedExpression],
      resultExpressions: Seq[NamedExpression], aggregateExpressions: Seq[AggregateExpression]):
  Seq[(NamedExpression, Int)] = {
    val errorEstimatesExprIDs = aggregateExpressions.flatMap(agg => agg.aggregateFunction match {
      case errAggFunc: ErrorAggregateFunction => errAggFunc.errorEstimateProjs.map(_._6.exprId)
      case _ => Nil
    })
    if (errorEstimatesExprIDs.isEmpty) {
      Nil
    } else {
      val numHavings = baseResultExpressions.count(_.name == "havingCondition")
      resultExpressions.zipWithIndex.collect {
        case (ne, index) if (errorEstimatesExprIDs.contains(ne.exprId)) =>
          (ne.toAttribute, index - numHavings)
      }
    }
  }

  def createColumnExtractorSubstituedExpression(elements: Seq[(ErrorAggregate.Type, String,
      NamedExpression, Double, Double, Double, ExprId, Boolean, HAC.Type, NamedExpression)]):
  NamedExpression = {
    elements.foldLeft[NamedExpression](null)((z, elem) => {
                  val (elemAggType, elemProjName, childX, elemConfi, elemConfact,
                  elemError, exprId, closedForm, elemBehavior, rootExpr) = elem
                   val root = if (z == null) {
                     rootExpr
                   } else {
                     z
                   }
                   root.transformUp{
                   case ar: AttributeReference if (ar.exprId == exprId) => if (closedForm) {
                        ClosedFormColumnExtractor(childX, elemProjName,
                          elemConfi, elemConfact, elemAggType, elemError,
                          DoubleType, elemBehavior, true)(exprId)
                      } else {
                        ApproxColumnExtractor(childX, elemProjName,
                          ApproxColumn.getOrdinal(elemAggType), DoubleType, true)(exprId)
                      }
                   }.asInstanceOf[NamedExpression]
                  })
  }
}
