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


import scala.collection.immutable.HashSet
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.classTag

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Final}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.bootstrap.PoissonCreator.PoissonType.PoissonType
import org.apache.spark.sql.execution.bootstrap._
import org.apache.spark.sql.execution.closedform.ErrorLimitExceededException
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SnappyHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.internal.SnappyAQPSessionState
import org.apache.spark.sql.sources.Masquerader
import org.apache.spark.sql.types.{ByteType, DoubleType}
import org.apache.spark.sql.{SQLContext, SnappySession, execution}
import io.snappydata.Property



object AQPRules {

  @volatile  private var testHookStoreAQPInfoOpt: Option[AQPInfoStoreTestHook] = None

  def setTestHookStoreAQPInfo(cb: AQPInfoStoreTestHook): Unit = {
    if (cb != null) {
      testHookStoreAQPInfoOpt = Some(cb)
    } else {
      testHookStoreAQPInfoOpt = None
    }
  }

  def getAQPInfo(plan: SparkPlan, position: Int): Option[AQPInfo] = {
    plan.sqlContext.sessionState match {
      case state: SnappyAQPSessionState =>
        val arr = state.contextFunctions.aqpInfo
        if (arr.length <= position) {
          None
        } else {
          Some(arr(position))
        }
      case _ => None
    }
  }

  def clearAQPInfo(sqlContext: SQLContext): Unit = {
    testHookStoreAQPInfoOpt.foreach(_.callbackBeforeClearing(
      sqlContext.sessionState.asInstanceOf[SnappyAQPSessionState].
      contextFunctions.aqpInfo.toArray))
    sqlContext.sessionState.asInstanceOf[SnappyAQPSessionState].
      contextFunctions.aqpInfo.clear()
    sqlContext.sessionState.asInstanceOf[SnappyAQPSessionState].
      contextFunctions.currentPlanAnalysis = None
  }



  def isBootStrapAnalysis(plan: SparkPlan, position: Int): Boolean = getAQPInfo(plan, position)
  match {
    case Some(AQPInfo(_, _, _, Some(AnalysisType.Bootstrap), _, _, _, _, _, _, _, _)) => true
    case _ => false
  }

  def isClosedForm(plan: SparkPlan, position: Int): Boolean = getAQPInfo(plan, position) match {
    case Some(AQPInfo(_, _, _, Some(AnalysisType.Closedform), _, _, _, _, _, _, _, _)) => true
    case _ => false
  }

  def getPoissonType(plan: SparkPlan, position: Int): PoissonType = getAQPInfo(plan, position)
  match {
    case Some(aqpInfo) => aqpInfo.poissonType
    case _ => null
  }

  def requireHiddenRelativeErrors(behaviour: HAC.Type): Boolean = behaviour != HAC.DO_NOTHING &&
    behaviour != HAC.SPECIAL_SYMBOL

  def requireHiddenGroupBy(behaviour: HAC.Type): Boolean = behaviour == HAC.PARTIAL_ROUTING
}

object HideSubqueryNodes extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case p: SampleTablePlan if (p != plan) => p.copy(makeVisible = false)
  }
}

object UnHideSubqueryNodes extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case p: SampleTablePlan => p.copy(makeVisible = true)
  }
}

private[sql] case class SampleTablePlan(error: Double, @transient confidence: Double,
                                        @transient sampleRatioAttribute: Masquerader,
                                        analysisType: Option[AnalysisType.Type],
                                        @transient bootstrapMultiplicities: Option[Attribute],
                                        child: SparkPlan, @transient bsMultiplictyAliasID: ExprId,
                                        @transient numBSTrials: Int,
                                        @transient behavior: HAC.Type, numBaseAggregates: Int,
                                        TODO_UNUSED: Boolean,
                                        hiddenCols: Option[Seq[NamedExpression]],
                                        logicalPlanHasSort: Boolean,
                                        realSortingOrder: Option[Seq[SortOrder]],
                                        @transient logicalPlanForQueryRouting: Option[LogicalPlan],
                                        val makeVisible: Boolean)
  extends SparkPlan with CodegenSupport {

  var allRows: String = _



  override def children: Seq[SparkPlan] = if (makeVisible) child :: Nil else Nil

  override def supportCodegen: Boolean =
    !(behavior == HAC.REROUTE_TO_BASE || fullRoutingInPlaceOfPartialRouting ||
      behavior == HAC.PARTIAL_ROUTING)


  override protected def doProduce(ctx: CodegenContext): String = {
    if (behavior == HAC.REROUTE_TO_BASE || fullRoutingInPlaceOfPartialRouting) {
      throw new UnsupportedOperationException("Not implemented")
    } else {
      child.asInstanceOf[CodegenSupport].produce(ctx, this)
    }

  }

  private def getModifiedSortingOrder : (Option[Seq[SortOrder]], Boolean) =
   {
    var isModified: Boolean = false
     (realSortingOrder.map(x => x.map(so =>
      so.transformUp {
        case ar: AttributeReference if (!child.output.contains(ar)) => {
          isModified = true
          child.collectFirst {
            case agg: SnappyHashAggregate => {
              val sortCol = agg.resultExpressions.find(_.exprId
                == ar
                .exprId)
              sortCol.map {
                case Alias(ar: AttributeReference, _) => agg.resultExpressions.find(_.exprId ==
                  ar.exprId).map(_.toAttribute).getOrElse(ar)
                case _ => ar
              }.getOrElse(ar)
            }
          }.getOrElse(ar)
      }
      }.asInstanceOf[SortOrder]
    )
    ), isModified)
  }

  // This method is suppose to be invoked only for cases where we know for sure there are hidden
  // relatiev error columns
  // added. This method has no meaning for case like local_omit & do nothing behaviour
  private def getGroupExpressionStartPosition: Int = child.output.length - hiddenColsSize +
    numBaseAggregates

  // This method is suppose to be invoked only for cases where we know for sure there are hidden
  // relatiev error columns
  // added. This method has no meaning for case like local_omit & do nothing behaviour
  private def getRelativeErrorPositions: Range = {
    val groupingExpressionSize = hiddenColsSize - numBaseAggregates
    val numFields = child.output.length
    (numFields - 1 - groupingExpressionSize) until (numFields - numBaseAggregates -
      1 - groupingExpressionSize) by -1
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = this.child.asInstanceOf[CodegenSupport].
    inputRDDs()

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {

    val childOutput = child.output
    val indexOfBSMultiplicity: Int = if (analysisType.map(_ == AnalysisType.Bootstrap).
      getOrElse(false)) {
      val output1 = childOutput
      output1.indexWhere(_.exprId == bsMultiplictyAliasID)
    } else {
      -1
    }


    try {
      val session: SnappySession = sqlContext.sparkSession.asInstanceOf[SnappySession]
      if (behavior == HAC.THROW_EXCEPTION || invalidCaseOfRouting) {
        val relativeErrorPositions: Range = getRelativeErrorPositions
        logWarning("For given behavior=" + HAC.getBehaviorAsString(behavior) +
          " logical plan is not cached. So exception will be thrown.")

        val netFields = childOutput.length - hiddenColsSize
        val projection = generateBSMultiplicityProjection(childOutput,
          netFields, indexOfBSMultiplicity).map(ExpressionCanonicalizer.execute(_))

        val exceptionClass = classOf[ErrorLimitExceededException].getName
        ctx.currentVars = input

        val resultVars = projection.map(_.genCode(ctx))
        relativeErrorPositions.map(i =>
          s"""
          if (${input(i).isNull}|| ${input(i).value} > $error|| Double.isNaN(${input(i).value})) {
            throw new $exceptionClass("At least one row in the " +
                "result has an error greater than specified limit, " +
                "try running the query with a more lenient policy");
          }
          """
        ).mkString("\n") +
          s"""
             ${consume(ctx, resultVars)}
             """
      } else if (behavior == HAC.REROUTE_TO_BASE || fullRoutingInPlaceOfPartialRouting) {
        throw new UnsupportedOperationException("SampleTablePlan: whole stage code gen not " +
          "supported for full reroute")

      } else if (behavior == HAC.PARTIAL_ROUTING) {
        throw new UnsupportedOperationException("SampleTablePlan: whole stage code gen not " +
          "supported for partial reroute")
      } else {
        if (indexOfBSMultiplicity == -1) {
          parent.doConsume(ctx, input, row)
        } else {
          val exprs = output.map(x =>
            ExpressionCanonicalizer.execute(BindReferences.bindReference(x, child.output)))
          ctx.currentVars = input
          val resultVars = exprs.map(_.genCode(ctx))
          // Evaluation of non-deterministic expressions can't be deferred.
          s"""
             ${consume(ctx, resultVars)}
     """.stripMargin
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  override def output: Seq[Attribute] = {
    val output1 = if (hiddenColsSize > 0) {
      child.output.dropRight(hiddenColsSize)
    } else {
      child.output
    }

    if (analysisType.orNull == AnalysisType.Bootstrap) {
      if (output1.exists(_.exprId == bsMultiplictyAliasID)) {
        output1.filter(_.exprId != bsMultiplictyAliasID)
      } else {
        output1
      }
    } else {
      output1
    }
  }

  def hiddenColsSize: Int = if (hiddenCols.isDefined) hiddenCols.get.size
  else 0

  def fullRoutingInPlaceOfPartialRouting: Boolean = behavior == HAC.PARTIAL_ROUTING &&
    hiddenColsSize == numBaseAggregates

  def invalidCaseOfRouting: Boolean = (behavior == HAC.PARTIAL_ROUTING ||
    behavior == HAC.REROUTE_TO_BASE) && logicalPlanForQueryRouting.isEmpty

  protected override def doExecute(): RDD[InternalRow] = {
    internalExecute(child.execute().map(_.copy()))
  }

  private def internalExecute(childRDD: RDD[InternalRow]): RDD[InternalRow] = {
    val indexOfBSMultiplicity: Int = if (analysisType.map(_ == AnalysisType.Bootstrap).
      getOrElse(false)) {
      val output1 = child.output
      output1.indexWhere(_.exprId == bsMultiplictyAliasID)
    } else {
      -1
    }

    try {
      val session: SnappySession = sqlContext.sparkSession.asInstanceOf[SnappySession]
      if (AQPRules.requireHiddenGroupBy(behavior) || AQPRules.
        requireHiddenRelativeErrors(behavior)) {

        val groupingExpressionStartPosition: Int = getGroupExpressionStartPosition
        val relativeErrorPositions: Range = getRelativeErrorPositions

        def errorExists(ir: InternalRow): Boolean = relativeErrorPositions.exists(i =>
          ir.isNullAt(i) || ir.getDouble(i) > error)

        def invalidRows(childRDD: RDD[InternalRow]): Array[Array[Any]] =
        {
          val groupingSize = hiddenColsSize - numBaseAggregates
          childRDD.collect({ case ir if errorExists(ir) =>
            (for (i <- 0 until groupingSize) yield {
              ir.get(groupingExpressionStartPosition + i,
                hiddenCols.get(i + numBaseAggregates).dataType).asInstanceOf[Any]
            }).toArray[Any]
          }).collect()
        }

        def validRowsRDD(childRDD: RDD[InternalRow],
                         output: Seq[Attribute]): RDD[InternalRow] = {
          val projection = generateBSMultiplicityProjection(output,
            output.length - hiddenColsSize,
            indexOfBSMultiplicity)
          childRDD.filter { ir =>
            !relativeErrorPositions.exists(i => ir.isNullAt(i) || ir.getDouble(i) > error)
          }.mapPartitionsInternal { itr =>
            val project = UnsafeProjection.create(projection)
            itr.map(project)
          }
        }

        def invalidGroupsKeyList(invalidGroupbyKeys: Array[Array[Any]]): Array[Set[Any]] =
          invalidGroupbyKeys.transpose[Any].map(_.toSet)



        if (behavior == HAC.THROW_EXCEPTION || invalidCaseOfRouting) {
          logWarning("For given behavior=" + HAC.getBehaviorAsString(behavior) +
            " logical plan is not cached. So exception will be thrown.")
          val projection = generateBSMultiplicityProjection(child.output,
            child.output.length - hiddenColsSize, indexOfBSMultiplicity)
          childRDD.mapPartitionsInternal { itr =>
            val project = UnsafeProjection.create(projection)
            itr.map { ir =>
              relativeErrorPositions.foreach(i =>
                if (ir.getDouble(i) > error) {
                  throw new ErrorLimitExceededException("At least one row in the " +
                    "result has an error greater than specified limit, " +
                    "try running the query with a more lenient policy")
                })
              project(ir)
            }
          }
        } else if (behavior == HAC.REROUTE_TO_BASE || fullRoutingInPlaceOfPartialRouting) {
          val fullRoutingNotNeeded = childRDD.collect({ case ir =>
            relativeErrorPositions.exists(i =>
              ir.isNullAt(i) || ir.getDouble(i) > error)
          }).filter(x => x).isEmpty()
          if (fullRoutingNotNeeded) {
            val projection = generateBSMultiplicityProjection(child.output,
              child.output.length - hiddenColsSize, indexOfBSMultiplicity)
            childRDD.mapPartitionsInternal { itr =>
              val project = UnsafeProjection.create(projection)
              itr.map(ir => project(ir))
            }
          } else {
            val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
            session.sessionState.executePlan(CatchErrorPlan.newPlan(session,
              logicalPlanForQueryRouting.get)).executedPlan.execute()
          }
        } else if (behavior == HAC.PARTIAL_ROUTING) {
          val updatedLogicalPlan: Option[LogicalPlan] =
            CatchErrorPlan.newPartialRoutingPlan(session,
              invalidGroupsKeyList(invalidRows(childRDD)), logicalPlanForQueryRouting.get)
          if (updatedLogicalPlan.isDefined) {
            val baseTablePlan = session.sessionState.executePlan(
              updatedLogicalPlan.get).executedPlan
            /* val hasSort =
              QueryRoutingRules.getPlanHasSort(CatchErrorPlan.getLogicalPlan(session).get) */
            if (realSortingOrder.isDefined) {
              val (correctedSortOrder, isRealSortModified) = getModifiedSortingOrder
              if (isRealSortModified) {

                def getHiddenCols(groupingExpressions: Seq[NamedExpression]):
                Seq[NamedExpression] = {

                  if (analysisType.map(_ == AnalysisType.Bootstrap).getOrElse(false)) {
                    var j = 0
                    Alias(Cast(Literal(1), ByteType), "dumb")() +: hiddenCols.map(cols =>
                      cols.map(col => if (col.name.startsWith(ResultsExpressionCreator.
                        hiddenColPrefix)) {
                      Alias(Cast(Literal(0), DoubleType), "dumb")()
                    } else {
                      val x = groupingExpressions(j)
                      j += 1
                      x
                    })).getOrElse(Nil)

                  } else {
                    var j = 0
                    hiddenCols.map(cols => cols.map(col => if (col.name.startsWith
                    (ResultsExpressionCreator.hiddenColPrefix)) {
                      Alias(Cast(Literal(0), DoubleType), "dumb")()
                    } else {
                      val x = groupingExpressions(j)
                      j += 1
                      x
                    })).getOrElse(Nil)
                  }

                }

                def getModifiedList(list: Seq[NamedExpression], hidden: Seq[NamedExpression]):
                Seq[NamedExpression] = {
                  if (analysisType.map(_ == AnalysisType.Bootstrap).getOrElse(false)) {
                    hidden(0) +: (list ++ hidden.drop(1))
                  } else {
                    list ++ hidden
                  }
                }

                var startChange = false
                var hidden: Seq[NamedExpression] = null
                val modifiedBaseTablePlan = baseTablePlan.transformUp {
                  case ProjectExec(prList, child) if (startChange) => ProjectExec(getModifiedList
                  (prList, hidden), child)
                  case top@TakeOrderedAndProjectExec(_, _, prList, _) if (startChange) => top.copy(
                    projectList = getModifiedList(prList, hidden))
                  case agg: HashAggregateExec if (agg.aggregateExpressions(0).mode == Final || agg
                    .aggregateExpressions(0).mode == Complete) => {
                    startChange = true
                    val _hidden = getHiddenCols(agg.groupingExpressions)
                    val newResultsList = getModifiedList(agg.resultExpressions, _hidden)
                    hidden = _hidden.map(_.toAttribute)
                    agg.copy(resultExpressions = newResultsList)
                  }
                  case agg: SortAggregateExec if (agg.aggregateExpressions(0).mode == Final || agg
                    .aggregateExpressions(0).mode == Complete) => {
                    startChange = true
                    val _hidden = getHiddenCols(agg.groupingExpressions)
                    val newResultsList = getModifiedList(agg.resultExpressions, _hidden)
                    hidden = _hidden.map(_.toAttribute)
                    agg.copy(__resultExpressions = newResultsList)
                  }
                  case agg: SnappyHashAggregateExec if (agg.aggregateExpressions(0).mode == Final
                    || agg.aggregateExpressions(0).mode == Complete) => {
                    startChange = true
                    val _hidden = getHiddenCols(agg.groupingExpressions)
                    val newResultsList = getModifiedList(agg.resultExpressions, _hidden)
                    hidden = _hidden.map(_.toAttribute)
                    agg.copy(__resultExpressions = newResultsList)
                  }

                }
                val baseTableRDD = modifiedBaseTablePlan.execute()
                val combinedRDD = childRDD.map(_.copy()) ++ baseTableRDD.map(_.copy())
                val ord = new InterpretedOrdering(correctedSortOrder.get, child.output)
                val orderedRDD = combinedRDD.sortBy(x => x)(ord, classTag[InternalRow])
                validRowsRDD(orderedRDD, child.output)
              } else {
                val baseTableRDD = baseTablePlan.execute()
                val combinedRDD = validRowsRDD(childRDD, child.output).map(_.copy()) ++
                  baseTableRDD.map(_.copy())
                val ord = new InterpretedOrdering(realSortingOrder.get, output)
                combinedRDD.sortBy(x => x)(ord, classTag[InternalRow])
              }
            } else {
              val baseTableRDD = baseTablePlan.execute()
              // TODO: Asif : is the copy actually needed now? as the childRDD already has copy
              validRowsRDD(childRDD, child.output).map(_.copy()) ++
                baseTableRDD.map(_.copy())
            }
          } else {
            validRowsRDD(childRDD, child.output)
          }
        } else {
          throw new IllegalStateException("Undefined behaviour=" + behavior)
        }
      } else {
        if (indexOfBSMultiplicity == -1) {
          childRDD
        } else {
          val finalOutput = output
          val childOutput = child.output
          childRDD.mapPartitionsInternal(iter => {
            val project = UnsafeProjection.create(finalOutput, childOutput,
              subexpressionEliminationEnabled)
            iter.map { row => project(row) }
          }, true
          )
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  def generateBSMultiplicityProjection(output: Seq[Attribute],
                                       numFields: Int, bsMultiplictyIndex: Int):
  Seq[BoundReference] = {
    val goTill = if (numFields - 1 == bsMultiplictyIndex) {
      numFields - 1
    } else {
      numFields
    }
    for (ordinal <- 0 until goTill if (ordinal != bsMultiplictyIndex)) yield {
      val column = output(ordinal)
      BoundReference(ordinal, column.dataType, column.nullable)
    }
  }


}


object SampleTablePlan {
  def applyRecursivelyToInvisiblePlan(root: SparkPlan, transformer: (SparkPlan, Int) => SparkPlan):
  SparkPlan = {
    // The subquery nodes are invisible when in this rule, so start from top to bottom
    val positionCounter = new PositionCounter
    val newPlan = recurseDown(root, transformer, positionCounter)
    newPlan match {
      case stp: SampleTablePlan if (!stp.makeVisible) => stp.copy(makeVisible = true)
      case x => x
    }
  }

  private def recurseDown(node: SparkPlan, transformer: (SparkPlan, Int) => SparkPlan,
                          positionCounter: PositionCounter): SparkPlan =
    node.transformDown {
      case stp: SampleTablePlan => {
        val visibleStp = stp.copy(makeVisible = true)
        val newStp = transformer(visibleStp, positionCounter.getPosition).
          asInstanceOf[SampleTablePlan]
        newStp.copy(child = recurseDown(newStp.child, transformer, positionCounter),
          makeVisible = false)
      }
    }
}

final class PositionCounter {
  private var position: Int = -1

  def getPosition: Int = {
    position += 1
    position
  }
}

case class AQPInfo(error: Double, confidence: Double,
                   sampleRatioAttribute: Masquerader, analysisType: Option[AnalysisType.Type],
                   isAQPDebug: Boolean, isDebugFixedSeed: Boolean,
                   poissonType: PoissonType, bootstrapMultiplicities: Option[Attribute],
                   bsMultiplictyAliasID: ExprId, numBSTrials: Int, behavior: HAC.Type,
                   numBaseAggs: Int)

final object AnalysisType extends Enumeration {
  type Type = Value
  val Bootstrap = Value("Bootstrap")
  val Closedform = Value("Closedform")
  val ByPassErrorCalc = Value("ByPassErrorCalc")
  val Defer = Value("Defer")
}


object CatchErrorPlan {
  def newPlan(session: SnappySession, logicalPlanForQueryRouting: LogicalPlan): LogicalPlan =
    QueryRoutingRules.setBehaviorForcedRoute(logicalPlanForQueryRouting,
      session.sessionState.conf)

  def newPartialRoutingPlan(session: SnappySession,
                            params: Array[Set[Any]],
    logicalPlanForQueryRouting: LogicalPlan): Option[LogicalPlan] =
    QueryRoutingRules.setPartialRoutePlan(logicalPlanForQueryRouting, session.sqlContext.conf,
      params)
}
