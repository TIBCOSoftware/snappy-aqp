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

import scala.collection.mutable.ArrayBuffer
import scala.collection.{:+, mutable}
import scala.util.control.Breaks._

import io.snappydata.{Constant, Property}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{UnresolvedException, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter, logical => logic}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalysts.plans.logical.SnappyUnaryNode
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.aggregate.{SnappyHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.bootstrap._
import org.apache.spark.sql.execution.closedform.{ClosedFormErrorEstimate, ErrorAggregate, ErrorEstimateAttribute}
import org.apache.spark.sql.execution.columnar.impl.BaseColumnFormatRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.{SparkPlan => ExecSparkPlan, _}
import org.apache.spark.sql.hive.{SampledRelation, SnappyAQPSessionCatalog}
import org.apache.spark.sql.internal.{AnalyzerInvocation, SQLConf}
import org.apache.spark.sql.row.JDBCMutableRelation
import org.apache.spark.sql.sampling.ColumnFormatSamplingRelation
import org.apache.spark.sql.sources.{SampleTableQuery, SamplingRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst._

object HAC extends Enumeration {

  type Type = Value
  val DO_NOTHING = Value(0)
  val SPECIAL_SYMBOL = Value(1)
  val THROW_EXCEPTION = Value(2)
  val REROUTE_TO_BASE = Value(3)
  val PARTIAL_ROUTING = Value(4)

  override def toString(): String = {
    s" 1)DO_NOTHING 2)LOCAL_OMIT 3)STRICT 4)RUN_ON_FULL_TABLE 5)PARTIAL_RUN_ON_BASE_TABLE"
  }

  def getBehavior(expr: Expression): HAC.Type = {
    expr match {
      case lp: ParamLiteral => getBehavior(lp.valueString)
      case _ => getBehavior(expr.simpleString)
    }
  }


  def getBehavior(name: String): HAC.Type = {
    Utils.toUpperCase(name) match {
      case Constant.BEHAVIOR_DO_NOTHING => DO_NOTHING
      case Constant.BEHAVIOR_LOCAL_OMIT => SPECIAL_SYMBOL
      case Constant.BEHAVIOR_STRICT => THROW_EXCEPTION
      case Constant.BEHAVIOR_RUN_ON_FULL_TABLE => REROUTE_TO_BASE
      case Constant.DEFAULT_BEHAVIOR => getDefaultBehavior()
      case Constant.BEHAVIOR_PARTIAL_RUN_ON_BASE_TABLE => PARTIAL_ROUTING

      case x@_ => throw new UnsupportedOperationException(
        s"Please specify valid HAC from below:\n$HAC\nGiven: $x")
    }
  }

  def getBehaviorAsString(value: HAC.Type): String = {
    value match {
      case DO_NOTHING => Constant.BEHAVIOR_DO_NOTHING
      case SPECIAL_SYMBOL => Constant.BEHAVIOR_LOCAL_OMIT
      case THROW_EXCEPTION => Constant.BEHAVIOR_STRICT
      case REROUTE_TO_BASE => Constant.BEHAVIOR_RUN_ON_FULL_TABLE
      case PARTIAL_ROUTING => Constant.BEHAVIOR_PARTIAL_RUN_ON_BASE_TABLE
      case _ => "INVALID"
    }
  }

  def getDefaultBehavior(conf: SQLConf = null): HAC.Type = {
    if (System.getProperty(Constant.defaultBehaviorAsDO_NOTHING, "false").toBoolean) {
      DO_NOTHING
    }
    else if (conf != null) {
      try {
        HAC.getBehavior(Literal.create(Property.Behavior.getOption(conf).getOrElse(
          Constant.BEHAVIOR_RUN_ON_FULL_TABLE),
          StringType))
      } catch {
        case e: UnsupportedOperationException => Property.Behavior.set(conf,
          Constant.BEHAVIOR_RUN_ON_FULL_TABLE)
          throw e
      }
    } else REROUTE_TO_BASE
  }
}

case class ReplaceWithSampleTable(catalog: SnappyAQPSessionCatalog,
                                  analyzerInvocation: AnalyzerInvocation, numBootStrapTrials: Int,
                                  isAQPDebug: Boolean, isFixedSeed: Boolean,
                                  isDefaultClosedForm: Boolean, conf: SQLConf)
  extends Rule[LogicalPlan] {
  val subqueryChildHandler: (SubqueryAlias, LogicalPlan, Option[LogicalPlan]) => LogicalPlan =
    (parent: SubqueryAlias, child: LogicalPlan, aqpClause: Option[LogicalPlan]) => {
      child match {
        case _: LeafNode | _: SerializeFromObject | _: PlaceHolderPlan => parent
        case _ =>
          val childWithAqpClause = aqpClause.map(aqpC => {
            aqpC.transformUp {
              case _: DummyLeafNode => child
            }
          }).getOrElse(child)

          val (modChild, isDistributed, sampleTableFound, analysisType) =
          ReplaceWithSampleTable.handleUnprocessedPlan(childWithAqpClause,
            catalog, analyzerInvocation, numBootStrapTrials,
            isAQPDebug, isFixedSeed, isDefaultClosedForm, conf)
          if (sampleTableFound) {
            parent.copy(child = PlaceHolderPlan(modChild, false, isDistributed, false))
          } else if (!sampleTableFound && parent.resolved) {
            // remove aqp clause if added to the child
            val modModChild = aqpClause.map( _ => modChild.transformUp {
              case b: Behavior => b.child
              case e: Error => e.child
              case e: ErrorDefaults => e.child
            }).getOrElse(modChild)
            parent.copy(child = PlaceHolderPlan(modModChild, false, isDistributed, true))
          } else {
            throw new UnsupportedOperationException("Not supported")
          }
      }
    }


  def apply(plan: LogicalPlan): LogicalPlan = {

    val skipRule = plan.find {
      case _: ErrorAndConfidence => true
      case _: SampleTableQuery => true
      case _: DeserializeToObject => true
      case _: Aggregate => true
      case _ => false
    } match {
      case Some(x: ErrorAndConfidence) => {
      //  isAQPSql = true
        x.ruleNumProcessing == RuleNumbers.REPLACE_WITH_SAMPLE_TABLE
      }
      case Some(x: SampleTableQuery) => {
      //  isAQPSql = true
        x.ruleNumProcessing == RuleNumbers.REPLACE_WITH_SAMPLE_TABLE
      }
      case Some(_: DeserializeToObject) => {
      //  isAQPSql = false
        true
      }
      case Some(_: Aggregate) => {
        false
      }
      case _ => true
    }
    if (skipRule || analyzerInvocation.getCallStackCount > 0
      || ReplaceWithSampleTable.isQueryRoutingApplicable
      || !plan.resolved
    ) {
      plan
    } else {

      val suppliedAqpClauses = plan.collect {
        case x@(Error(_, _) | Behavior(_, _, _) |  ErrorDefaults(_)) => x
      }
     val aqpClausePlan = if (suppliedAqpClauses.isEmpty) {
       None
     } else {
       Some(suppliedAqpClauses.foldLeft[LogicalPlan](DummyLeafNode())(
         (rs, x) => x.mapChildren( _ => rs)))
     }

      plan find {
        case _: SampleTableQuery => true
        case _ => false
      } match {
        case Some(stq: SampleTableQuery) => {
          ReplaceWithSampleTable.sampleTableQueryHandler(plan, catalog,
            analyzerInvocation, numBootStrapTrials,
            isAQPDebug, isFixedSeed, isDefaultClosedForm, conf, stq)._1
        }
        case _ => {
          val x = plan.transformUp {
            case sqa: SubqueryAlias => subqueryChildHandler(sqa, sqa.child, aqpClausePlan)
          }
          val childWithAqpClause = aqpClausePlan.map(_.transformUp {
              case _: DummyLeafNode => x
            }
          ).getOrElse(x)

          val (modChild, isDistributed, sampleTableFound, analysisType) =
            ReplaceWithSampleTable.handleUnprocessedPlan(childWithAqpClause, catalog,
              analyzerInvocation, numBootStrapTrials,
              isAQPDebug, isFixedSeed, isDefaultClosedForm, conf)
          if (!sampleTableFound) {
            // remove aqp clause if added to the child
            aqpClausePlan.map(_ => modChild.transformUp {
              case b: Behavior => b.child
              case e: Error => e.child
              case e: ErrorDefaults => e.child
            }).getOrElse(modChild)
          } else {
            modChild
          }
        }
      }
    }
  }
}


object ReplaceWithSampleTable {

  val INIFINITE_ERROR_TOLERANCE: Double = Double.MaxValue
  val qcsPattern = """#[0-9]+""".r


  val sampleTableQueryHandler: (LogicalPlan, SnappyAQPSessionCatalog, AnalyzerInvocation, Int,
    Boolean, Boolean, Boolean, SQLConf, SampleTableQuery) => (LogicalPlan, Boolean, Boolean,
    Option[AnalysisType.Type]) =
    (subqueryChild: LogicalPlan, catalog: SnappyAQPSessionCatalog,
     analyzerInvocation: AnalyzerInvocation, numBootStrapTrials: Int,
     isAQPDebug: Boolean, isFixedSeed: Boolean, isDefaultClosedForm: Boolean,
     conf: SQLConf, stq: SampleTableQuery) => handleProcessedPlan(subqueryChild, catalog,
          analyzerInvocation, numBootStrapTrials,
          isAQPDebug, isFixedSeed, isDefaultClosedForm, conf, stq)


  // val maxErrorAllowed = conf.getConfString(Constant.maxErrorAllowed, "1").toDouble

  def handleUnprocessedPlan(originalplan: LogicalPlan, catalog: SnappyAQPSessionCatalog,
                            analyzerInvocation: AnalyzerInvocation, numBootStrapTrials: Int,
                            isAQPDebug: Boolean, isFixedSeed: Boolean, isDefaultClosedForm: Boolean,
                            conf: SQLConf):
  (LogicalPlan, Boolean, Boolean, Option[AnalysisType.Type]) = {
    val maxErrorAllowed = Property.MaxErrorAllowed.get(conf)
    var error: Double = -1d
    val joinTypes = ArrayBuffer[org.apache.spark.sql.catalyst.plans.JoinType]()
    val mapRelations = scala.collection.mutable.Map[String, Boolean]()
    var sampleTableIdentified: Option[String] = None
    val relationsProcessedInSubqueryNode = scala.collection.mutable.Set[Any]()

    var name: String = null
    var baseTableLP: LogicalPlan = null
    var currentAlias: Option[String] = None
    // var confidence: Double = -1
    var behavior: HAC.Type = HAC.getDefaultBehavior(conf)
    var behaviorSet: Boolean = false
    var confidence: Double = Property.Confidence.getOption(conf).getOrElse(-1.0d)

    val groupby_qcs = new mutable.ArrayBuffer[String]
    val query_qcs = new mutable.ArrayBuffer[String]
    var (analysisType, hasAvg, hasCount,
    hasSum, requiredQcs) = ReplaceWithSampleTable.getAnalysisType(originalplan, isDefaultClosedForm)

    if (!(hasAvg || hasCount || hasSum) && requiredQcs.size > 0
      && analysisType.map(_ != AnalysisType.Defer).getOrElse(false)
    ) {
      // case of only max so make analysis type as ByPassErrorCalc
      analysisType = Some(AnalysisType.ByPassErrorCalc)
    }

    var foundSampleQuery = false

    if (conf.contains(Property.Error.name)) {
      val values = getErrorDefaults(conf)
      error = values._1
      confidence = values._2
      behavior = values._3
    }

    val plan = if (analysisType.isDefined) {
      // Open up PlaceHolderPlans if they are plain projection based , like in case of views

      def openUpPlaceHolderPlanChecker(pln : LogicalPlan): Boolean = {
       pln.find(_ match {
          case Project(prjList, _) => !prjList.forall(ne => ne match {
            case _: Attribute => true
            case Alias(_: AggregateExpression, _) => false
            case Alias(_, _) => true
            case _ => false
          })
          case _: Error | _: Confidence | _: ErrorAndConfidence | _: ErrorDefaults | _: Filter |
                _: SubqueryAlias | _: LogicalRelation | _: PlaceHolderPlan | _: Join => false
          case _ => true
        }).isEmpty

      }

      originalplan transformDown {
        case pl@PlaceHolderPlan(child, _, distributed, true) =>
          if (openUpPlaceHolderPlanChecker(child)) {
          PlaceHolderPlan(child, true, distributed)
        } else {
          pl
        }
        case pl@PlaceHolderPlan(child, true, distributed, false) =>
          pl.copy(makeVisible = false)
      }

    } else {
      originalplan
    }

    def collectQCSFromGroupingExpression(groupingExpression: Seq[Expression]): Seq[String]
    = groupingExpression.map(x => Utils.toLowerCase(qcsPattern.replaceAllIn(x.toString, "")))


    val aggregateModifier: PartialFunction[LogicalPlan, LogicalPlan] = {
      case agg@Aggregate(groupingExpressions, aggregateExpressions, child) =>
        // markForRecursion = false
        val grpByQcs = collectQCSFromGroupingExpression(groupingExpressions)
        groupby_qcs ++= grpByQcs
        query_qcs ++= grpByQcs
        val filterCriteria: ((NamedExpression, Int)) => Boolean = {

          case (exprsn, _) => exprsn.find({
            case _: ErrorEstimateFunction => true
            case _ => false
          }) match {
            case Some(x) => true
            case None => false
          }
        }
        agg
    }

    def identifySampleTable(baseTable: String, baseTablePlan: LogicalPlan,
      requiredQcs: Seq[String]):
    Option[(LogicalPlan, String)] = {
      val baseTableAttribs = baseTablePlan.output
      val aliases = mutable.ArrayBuffer[Alias]()

      plan.foreach(pln => pln.expressions.foreach(exp => exp match {
        case al: Alias => aliases += al
        case _ =>
      }))

      def normalizeExpression(expression: Expression): Expression = {
        val attribRefs = expression.references.toSeq
        val startingAttribs = mutable.ArrayBuffer[Attribute](attribRefs: _*)
        for (al <- aliases; i <- 0 until startingAttribs.size) {
          if (startingAttribs(i).exprId == al.exprId) {
            val temp = al.child.references.toSeq
            if (temp.size == 1) {
              startingAttribs(i) = temp.head
            }
          }
        }
        for(i <- 0 until startingAttribs.size) {
          if (!baseTableAttribs.exists(_.exprId == startingAttribs(i).exprId)) {
            startingAttribs(i) = null
          }
        }
        expression.transform {
          case attr: Attribute => val i = attribRefs.indexWhere(_.exprId == attr.exprId)
            if (i > -1) {
              val normAttrib = startingAttribs(i)
              if (normAttrib != null) {
                normAttrib
              } else {
                attr
              }
            } else attr
        }
      }
      val qcsList = plan.collect {
        case x: Filter => x
      }.flatMap(filter => {
        filter.condition.collect {
          case comp: BinaryComparison =>
            Utils.toLowerCase(qcsPattern.replaceAllIn(
              normalizeExpression(comp.left).toString, "")) :: Nil
          case comp: BinaryExpression =>
            if (!comp.isInstanceOf[BinaryOperator]) {
              Utils.toLowerCase(qcsPattern.replaceAllIn(
                normalizeExpression(comp.left).toString, "")) :: Nil
            }
            else {
              Nil
            }
          case expr: UnaryExpression =>
            Utils.toLowerCase(qcsPattern.replaceAllIn(
              normalizeExpression(expr.child).toString, "")) :: Nil
          case _ => Nil
        }
      }).flatten

      query_qcs ++= qcsList

      val aqpTables = catalog.getSampleRelations(catalog.snappySession.tableIdentifier(baseTable))

      var aqp: Option[(LogicalPlan, String)] = None
      var superset, subset: Seq[(LogicalPlan, String)] = Seq[(LogicalPlan, String)]()
      var mismatch_cnt: Int = 0
      breakable {
        for ((lp, qName) <- aqpTables) {

          val table_qcs = lp match {
            case LogicalRelation(b: SamplingRelation, _, _) => b.qcs.map(Utils.toLowerCase)
            case ss: StratifiedSample => ss.qcs._2
            case _ => Utils.EMPTY_STRING_ARRAY
          }

          if (requiredQcs.isEmpty || requiredQcs.forall(table_qcs.contains(_))) {
            if (groupby_qcs.toSet.--(table_qcs).isEmpty
              && groupby_qcs.toSet.size == table_qcs.size) {
              aqp = Some(lp -> qName)
              break
            }
            else if ((query_qcs.toSet --table_qcs).isEmpty) {
              if (query_qcs.toSet.size == table_qcs.size) {
                aqp = Some(lp -> qName)
                break
              }
              else {
                superset = superset.+:(lp -> qName)
              }
            }
            else {
              if (mismatch_cnt == 0) {
                mismatch_cnt = query_qcs.toSet.--(table_qcs).size
                subset = subset.+:(lp -> qName)
              }
              else if (query_qcs.toSet.--(table_qcs).size < mismatch_cnt) {
                mismatch_cnt = query_qcs.toSet.--(table_qcs).size
                subset = Seq[(LogicalPlan, String)]()
                subset = subset.+:(lp -> qName)
              }
              else if (query_qcs.toSet.--(table_qcs).size == mismatch_cnt) {
                subset = subset.+:(lp -> qName)
              }
            }
          }
        }
      }
      aqp match {
        case Some(_) => aqp
        case None =>
          var index, loop = -1
          var size = 0.0

          if (superset.nonEmpty) {
            // Need to select the table based which has largest sample size
            for ((lp, _) <- superset) {
              loop += 1
              lp match {
                case LogicalRelation(sr: SamplingRelation, _, _) =>
                  val colSampl = sr.asInstanceOf[ColumnFormatSamplingRelation]
                  if (colSampl.options.fraction > size || colSampl.options.bypassSampling) {
                    index = loop
                    size = sr.asInstanceOf[ColumnFormatSamplingRelation].options.fraction
                  }
                case _ =>
              }
            }
            Some(superset(index))
          } else if (subset.nonEmpty) {
            // Need to select the table based which has largest sample size
            for ((lp, _) <- subset) {
              loop += 1
              lp match {
                case LogicalRelation(sr: SamplingRelation, _, _) =>
                  val colSampl = sr.asInstanceOf[ColumnFormatSamplingRelation]
                  if (colSampl.options.fraction > size || colSampl.options.bypassSampling) {
                    index = loop
                    size = sr.asInstanceOf[ColumnFormatSamplingRelation].options.fraction
                  }
                case _ =>

              }
            }

            Some(subset(index))
          } else {
            aqp

          }
      }
    }


    def shouldApplyReplaceWithSampleTable(tableName: String): Boolean = {
      if (sampleTableIdentified.isDefined) {
        mapRelations += (currentAlias.getOrElse(tableName) -> false)
        false
      } else {
        error != -1  || (catalog.getTempView(tableName) match {
          case Some(StratifiedSample(_, _, _)) => true
          case Some(LogicalRelation(_: SamplingRelation, _, _)) => true
          case _ => false
        })
      }
    }

    def applyReplaceWithSampleTable(lr: LogicalRelation, tableName: String,
                                    createSubquery: Boolean, subqueryName: String): LogicalPlan = {
      if (error == -1) {
        throw new UnsupportedOperationException(
          s"With error clause is mandatory")
        // error = ReplaceWithSampleTable.DEFAULT_ERROR
      }

      if (confidence == -1) {
        confidence = Constant.DEFAULT_CONFIDENCE
      }

      val aqp: Option[(LogicalPlan, String)] = identifySampleTable(tableName, lr, requiredQcs)

      aqp match {
        case Some((sample, name)) =>
          val baseSampleAttribs = sample match {
            case s: StratifiedSample => s.child.output
            case l: LogicalRelation => l.output
          }

          val baseMainAttribs = lr.output
          relationsProcessedInSubqueryNode += sample
          val node = if (baseSampleAttribs.corresponds(baseMainAttribs)
          (_.exprId == _.exprId)) {
            if (createSubquery) {
              SubqueryAlias(subqueryName, sample, None)
            } else {
              sample
            }
          } else {
            val expressions = baseMainAttribs.zipWithIndex.map {
              case (attribute, index) =>
                Alias(baseSampleAttribs(index).withNullability(attribute.nullable),
                  baseSampleAttribs(index).name)(attribute.exprId, attribute.qualifier,
                  Some(attribute.metadata))
            } :+ sample.output.last
            logic.Project(expressions, if (createSubquery) {
              SubqueryAlias(subqueryName, sample, None)
            } else {
              sample
            })
          }


          val ratioAttribute = sample.output.find(
            _.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME)
          ).getOrElse(throw new IllegalStateException(
            "Hidden column for ratio not found."))
          ReplaceWithSampleTable.markForRecursion()
          // ReplaceWithSampleTable.incrementNumTimesApplied()
          sampleTableIdentified = if (currentAlias.isDefined) currentAlias else Some(tableName)

          ErrorAndConfidence(error, confidence, node,
            MapColumnToWeight(ratioAttribute),
            if (analysisType.get == AnalysisType.Bootstrap) {
              Some(BootstrapMultiplicity.generateBootstrapColumnReference)
            } else {
              None
            }, isAQPDebug, isFixedSeed, analysisType,
            BootstrapMultiplicity.getBootstrapMultiplicityAliasID,
            numBootStrapTrials, behavior, sampleTableIdentified.get,
            RuleNumbers.REPLACE_WITH_SAMPLE_TABLE)

        case None =>
          val isDistributed = lr.relation match {
                case x: PartitionedDataSourceScan => x.isPartitioned
                case _ => true
              }
          mapRelations += (currentAlias.getOrElse(tableName) -> isDistributed)
          lr
      }
    }

    def createErrConfForSampleTableQuery(p: LogicalPlan, sampleAttributes: Seq[Attribute]):
    ErrorAndConfidence = {
      // ReplaceWithSampleTable.incrementNumTimesApplied()
      if (error == -1) {
        error = ReplaceWithSampleTable.INIFINITE_ERROR_TOLERANCE
        confidence = Constant.DEFAULT_CONFIDENCE
      } else {
        if (confidence == -1) {
          confidence = Constant.DEFAULT_CONFIDENCE
        }
      }


      val ratioAttribute = sampleAttributes.find(
        _.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME)
      ).getOrElse(throw new IllegalStateException(
        "Hidden column for ratio not found."))

      ErrorAndConfidence(error, confidence, p, MapColumnToWeight(ratioAttribute),
        if (analysisType.get == AnalysisType.Bootstrap) {
          Some(BootstrapMultiplicity.generateBootstrapColumnReference)
        } else {
          None
        }, isAQPDebug, isFixedSeed, analysisType,
        BootstrapMultiplicity.getBootstrapMultiplicityAliasID,
        numBootStrapTrials, behavior, currentAlias.getOrElse(null),
        RuleNumbers.REPLACE_WITH_SAMPLE_TABLE)


    }
    var isNonSampledRelationCase = false

    analysisType match {
      case Some((AnalysisType.Bootstrap | AnalysisType.Closedform |
                 AnalysisType.ByPassErrorCalc)) => val y = plan transformDown {
        aggregateModifier orElse {
          case x@ErrorDefaults(child) =>
            val values = getErrorDefaults(conf)
            error = values._1
            confidence = values._2
            behavior = values._3
            x
          case x@Error(expr, child) =>
            error = ReplaceWithSampleTable.convertToDouble(expr)
            if (error == 0 || error >= maxErrorAllowed) {
              throw new UnsupportedOperationException("" +
                "Please specify error within range of 0 to 1")
            } else if (error < 0) {
              error = -1d
            }
            x

          case x@Confidence(expr, child) =>
            confidence = ReplaceWithSampleTable.convertToDouble(expr)
            if (confidence <= 0 || confidence >= 1) {
              throw new UnsupportedOperationException("" +
                "Please specify confidence within range of 0 to 1")
            }
            x

          case x@Behavior(expr, child, _) =>
            behavior = HAC.getBehavior(expr)
            behaviorSet = true
            x

          case e@ErrorAndConfidence(err, confi, _, _, _, _, _, _, _, _, behav, name, _) =>
            sampleTableIdentified = Some(name)
            ReplaceWithSampleTable.markForRecursion()
            val (errorSetOnProp, confiSetOnProp, behavSetOnProp) = getErrorDefaults(conf)
            if (error != -1 && error != err && error != errorSetOnProp ) {
              val newConfi = if (confidence != -1 && confidence != confiSetOnProp) {
                confidence
              } else {
                confi
              }

              val newBehav: HAC.Type = if (behavior != HAC.getDefaultBehavior(conf)
              && behavior != behavSetOnProp) {
                behavior
              } else {
                behav
              }
              e.copy(error = error, confidence = newConfi, behavior = newBehav,
                ruleNumProcessing = RuleNumbers.REPLACE_WITH_SAMPLE_TABLE)
            } else {
              e.copy(ruleNumProcessing = RuleNumbers.REPLACE_WITH_SAMPLE_TABLE)
            }


          case s@SampleTableQuery(_, _, err, confi, _, _, _, _, analysisTypeX, _, _, behav, name,
          _, _, _, _) => sampleTableIdentified = Some(name)
            if (analysisTypeX != AnalysisType.Defer) {
              analysisType = analysisTypeX
            }
            foundSampleQuery = true
            ReplaceWithSampleTable.markForRecursion()
            if (error != -1 && error != err) {
              val newConfi = if (confidence != -1) {
                confidence
              } else {
                confi
              }
              val newBehav: HAC.Type =
                if (behavior != HAC.getDefaultBehavior(conf)) {
                  behavior
                } else {
                  behav
                }
              s.copy(error = error, confidence = newConfi, behavior = newBehav,
                ruleNumProcessing = RuleNumbers.REPLACE_WITH_SAMPLE_TABLE, makeVisible = false)
            } else {
              s.copy(ruleNumProcessing = RuleNumbers.REPLACE_WITH_SAMPLE_TABLE, makeVisible = false)
            }

          case j@Join(left, right, joinType, cond) =>
            joinTypes += joinType
            val newJoinNode = joinType match {
              case LeftOuter if !right.isInstanceOf[PlaceHolderPlan] =>
                Join(left, PlaceHolderPlan(right, false, false, false), joinType, cond)
              case RightOuter if !left.isInstanceOf[PlaceHolderPlan] =>
                Join(PlaceHolderPlan(left, false, false, false), right, joinType, cond)
              case FullOuter | LeftSemi | LeftAnti => PlaceHolderPlan(j, false, false,
                false)
              case _ => j
            }
            newJoinNode

          case lr@LogicalRelation(sr: SamplingRelation, _, _) if !relationsProcessedInSubqueryNode.
            contains(lr) =>
            if (!ReplaceWithSampleTable.isMarkedForRecursion && sampleTableIdentified.isEmpty) {

              ReplaceWithSampleTable.markForRecursion()
              currentAlias = Some(lr.schemaString)
              sampleTableIdentified = currentAlias
              mapRelations += (sampleTableIdentified.get -> true)
              createErrConfForSampleTableQuery(lr, lr.output)
            } else {
              if (sampleTableIdentified.isDefined && sampleTableIdentified.get != lr.schemaString &&
                  catalog.snappySession.tableIdentifier(sampleTableIdentified.get).unquotedString !=
                      sr.asInstanceOf[ColumnFormatSamplingRelation].baseTable.map(x =>
                        catalog.snappySession.tableIdentifier(x).unquotedString).orNull &&
                  catalog.snappySession.tableIdentifier(sampleTableIdentified.get).unquotedString !=
                      catalog.snappySession.tableIdentifier(sr.asInstanceOf[
                          ColumnFormatSamplingRelation].sampleTable).unquotedString

              ) {
                throw new UnsupportedOperationException("" +
                  "Join between two sample tables is not supported")
              }
              lr
            }
          case l: LogicalRDD if l.output.exists(
            _.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME)) =>
            if (ReplaceWithSampleTable.isMarkedForRecursion) {
              l
            } else {

              if (error == -1) {
                error = ReplaceWithSampleTable.INIFINITE_ERROR_TOLERANCE
                confidence = Constant.DEFAULT_CONFIDENCE
              } else {
                if (confidence == -1) {
                  confidence = Constant.DEFAULT_CONFIDENCE
                }
              }
              if (sampleTableIdentified.isDefined) {
                if (!relationsProcessedInSubqueryNode.contains(l)) {
                  val isDistributed = true
                  mapRelations += (l.schemaString -> isDistributed)
                }
                l
              } else {
                if (!relationsProcessedInSubqueryNode.contains(l)) {
                  currentAlias = Some(l.schemaString)
                  sampleTableIdentified = currentAlias
                }
                isNonSampledRelationCase = true
                mapRelations += (currentAlias.get -> true)
                ReplaceWithSampleTable.markForRecursion()
                    ErrorAndConfidence(error, confidence, l,
                      MapColumnToWeight(l.output.find(
                        _.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME)).get),
                      if (analysisType.get == AnalysisType.Bootstrap) {
                        Some(BootstrapMultiplicity.generateBootstrapColumnReference)
                      } else {
                        None
                      }, isAQPDebug, isFixedSeed, analysisType,
                      BootstrapMultiplicity.getBootstrapMultiplicityAliasID,
                      numBootStrapTrials, behavior, currentAlias.orNull)
              }
            }

          case lr@LogicalRelation(JDBCMutableRelation(_, tableName, _, _, _, _, _),
          _, _) if (error != -1 && !relationsProcessedInSubqueryNode.contains(lr) &&
             shouldApplyReplaceWithSampleTable(tableName)) =>
            applyReplaceWithSampleTable(lr, tableName, false, null)

          case lr@LogicalRelation(r: BaseColumnFormatRelation,
          _, _) if (error != -1 && !relationsProcessedInSubqueryNode.contains(lr) &&
            shouldApplyReplaceWithSampleTable(r.table)) =>
            applyReplaceWithSampleTable(lr, r.table, false, null)

          case l@(LogicalRDD(_, _, _, _) | LogicalRelation(_, _, _)) if error != -1 &&
            catalog.asInstanceOf[SnappyAQPSessionCatalog].getSamples(l).isEmpty &&
            !relationsProcessedInSubqueryNode.contains(l) => {
            val isDistributed = l match {
              case LogicalRelation(baseRelation, _, _) => baseRelation match {
                case x: PartitionedDataSourceScan => x.isPartitioned
                case _ => true
              }
              case _ => true
            }
            l match {
              case lr: LogicalRelation => lr.relation match {
                case _: ColumnFormatSamplingRelation =>
                case _ => mapRelations += (l.schemaString -> isDistributed)
              }

              case _ => mapRelations += (l.schemaString -> isDistributed)
            }

            l
          }


          case l: LogicalPlan if error != -1 &&
            catalog.asInstanceOf[SnappyAQPSessionCatalog].getSamples(l).nonEmpty =>
            if (ReplaceWithSampleTable.isMarkedForRecursion) {
              l
            } else {
              val samples = catalog.asInstanceOf[SnappyAQPSessionCatalog].getSamples(l)
              if (sampleTableIdentified.isDefined) {
                if (!relationsProcessedInSubqueryNode.contains(l)) {
                  val isDistributed = l match {
                    case LogicalRelation(baseRelation, _, _) => baseRelation match {
                      case x: PartitionedDataSourceScan => x.isPartitioned
                      case _ => true
                    }
                    case _ => true
                  }
                  mapRelations += (l.schemaString -> isDistributed)
                }
                l
              } else {
                if (!relationsProcessedInSubqueryNode.contains(l)) {
                  currentAlias = Some(l.schemaString)
                  sampleTableIdentified = currentAlias
                }
                mapRelations += (currentAlias.get -> true)
                var aqp: Option[LogicalPlan] = None
                var superset, subset: Seq[LogicalPlan] = Seq[LogicalPlan]()
                breakable {
                  for (lp <- samples) {

                    val table_qcs = lp match {
                      case ss: StratifiedSample => ss.qcs._2
                      case _ => Utils.EMPTY_STRING_ARRAY
                    }


                    if (query_qcs.toSet.--(table_qcs).isEmpty) {
                      if (query_qcs.size == table_qcs.size) {
                        // println("table where QCS(table) == QCS(query)")
                        aqp = Some(lp)
                        break

                      }
                      else {
                        // println("table where QCS(table) is superset of QCS(query)")
                        superset = superset :+ lp
                      }
                    }
                    else if (query_qcs.toSet.--(table_qcs).nonEmpty) {
                      // println("table where QCS(table) is subset of QCS(query)")
                      subset = subset :+ lp
                    }
                  }
                }
                if (aqp.isEmpty) {
                  if (superset.nonEmpty) {
                    // Need to select one of the table based on sample size
                    aqp = Some(superset.head)
                  } else if (subset.nonEmpty) {
                    aqp = Some(subset.head) //
                  }
                }

                // println("aqpTable" + aqp)
                val newPlan = aqp match {
                  case Some(samplePlan: StratifiedSample) =>
                    val baseSampleAttribs = samplePlan.child.output
                    val baseMainAttribs = l.output

                    val node = if (baseSampleAttribs.corresponds(baseMainAttribs)
                    (_.exprId == _.exprId)) {
                      samplePlan
                    } else {
                      val expressions = baseMainAttribs.zipWithIndex.map {
                        case (attribute, index) =>
                          Alias(baseSampleAttribs(index).withNullability(attribute.nullable),
                            baseSampleAttribs(index).name)(attribute.exprId, attribute.qualifier,
                            Some(attribute.metadata))
                      } :+ samplePlan.output.last
                      logic.Project(expressions, samplePlan)
                    }

                    val ratioAttribute = samplePlan.output.find(
                      _.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME)
                    ).getOrElse(throw new IllegalStateException(
                      "Hidden column for ratio not found."))
                    ReplaceWithSampleTable.markForRecursion()
                    //  ReplaceWithSampleTable.incrementNumTimesApplied()

                    ErrorAndConfidence(error, confidence, node,
                      MapColumnToWeight(ratioAttribute),
                      if (analysisType.get == AnalysisType.Bootstrap) {
                        Some(BootstrapMultiplicity.generateBootstrapColumnReference)
                      } else {
                        None
                      }, isAQPDebug, isFixedSeed, analysisType,
                      BootstrapMultiplicity.getBootstrapMultiplicityAliasID,
                      numBootStrapTrials, behavior, currentAlias.orNull)

                  case _ => l

                }
                newPlan
              }
            }


          case ss: StratifiedSample =>
            if (!ReplaceWithSampleTable.isMarkedForRecursion) {
              if (sampleTableIdentified.isEmpty) {
                ReplaceWithSampleTable.markForRecursion()
                currentAlias = Some(ss.child.schemaString)
                createErrConfForSampleTableQuery(ss, ss.output)
              } else if (sampleTableIdentified.get != ss.child.schemaString) {
                throw new UnsupportedOperationException("" +
                  "Join between two stratified samples is not supported")
              } else {
                ss
              }
            } else {
              ss
            }

          case pl@PlaceHolderPlan(child, _, distributed, allowOpeningOfPlaceHolderPlan) =>
            if (!allowOpeningOfPlaceHolderPlan) {
              mapRelations += ("PlaceHolder-" + child.toString -> distributed)
            }
            pl

          case p@SubqueryAlias(n, child, view) =>
            name = if (view.isEmpty) n else view.get.unquotedString
            baseTableLP = child
            currentAlias = Some(name)
            if (!ReplaceWithSampleTable.isMarkedForRecursion) {
              relationsProcessedInSubqueryNode += child
            }

            child match {
              case _: SerializeFromObject => p.copy(child = PlaceHolderPlan(child, false, true))
              case ss: StratifiedSample =>
                if (!ReplaceWithSampleTable.isMarkedForRecursion && sampleTableIdentified.isEmpty) {
                  ReplaceWithSampleTable.markForRecursion()
                  sampleTableIdentified = currentAlias
                  relationsProcessedInSubqueryNode += ss
                  mapRelations += (name -> true)
                  createErrConfForSampleTableQuery(p, ss.output)
                } else {
                  if (sampleTableIdentified.getOrElse(name) != name) {
                    throw new UnsupportedOperationException("" +
                      "Join between two sample tables is not supported")
                  }
                  relationsProcessedInSubqueryNode += ss
                  p
                }
              case lr@LogicalRelation(sr: SamplingRelation, _, _) =>
                if (!ReplaceWithSampleTable.isMarkedForRecursion && sampleTableIdentified.isEmpty) {
                  ReplaceWithSampleTable.markForRecursion()
                  sampleTableIdentified = currentAlias
                  relationsProcessedInSubqueryNode += lr
                  mapRelations += (name -> true)
                  createErrConfForSampleTableQuery(p, lr.output)
                } else {
                  if (sampleTableIdentified.isDefined && sampleTableIdentified.get != name &&
                      catalog.snappySession.tableIdentifier(sampleTableIdentified.get)
                          .unquotedString != sr.asInstanceOf[ColumnFormatSamplingRelation]
                          .baseTable.map(x => catalog.snappySession.tableIdentifier(x)
                          .unquotedString).orNull && catalog.snappySession.tableIdentifier(
                    sampleTableIdentified.get).unquotedString !=
                      catalog.snappySession.tableIdentifier(
                        sr.asInstanceOf[ColumnFormatSamplingRelation].sampleTable).unquotedString
                  ) {
                    throw new UnsupportedOperationException("" +
                      "Join between two sample tables is not supported")
                  }
                  relationsProcessedInSubqueryNode += lr
                  p
                }
              case lr@LogicalRelation(cfr, _, _) if sampleTableIdentified.isDefined &&
                sampleTableIdentified.get != name => {

                mapRelations += (name -> (cfr match {
                  case x: PartitionedDataSourceScan => x.isPartitioned
                  case _ => false
                }))
                p
              }
              case lr@LogicalRelation(JDBCMutableRelation(_, tableName, _, _, _, _, _),
              _, _) if shouldApplyReplaceWithSampleTable(tableName) =>
                applyReplaceWithSampleTable(lr, tableName, true, name)


              case lr@LogicalRelation(r: BaseColumnFormatRelation,
              _, _) if shouldApplyReplaceWithSampleTable(r.table) =>
                applyReplaceWithSampleTable(lr, r.table, true, name)

              case lr@LogicalRelation(_, _, Some(tableIdentifier))
                if shouldApplyReplaceWithSampleTable(tableIdentifier.qualifiedName)
              => applyReplaceWithSampleTable(lr, tableIdentifier.qualifiedName, true, name)

             case lr: LogicalRelation  if lr.outputSet.exists(_.name.equalsIgnoreCase(Utils
                .WEIGHTAGE_COLUMN_NAME)) && !ReplaceWithSampleTable.isMarkedForRecursion &&
                sampleTableIdentified.isEmpty => ReplaceWithSampleTable.markForRecursion()
                sampleTableIdentified = currentAlias
                relationsProcessedInSubqueryNode += lr
                mapRelations += (name -> true)
                isNonSampledRelationCase = true
                createErrConfForSampleTableQuery(p, lr.output)

              case pl@PlaceHolderPlan(child, _, distributed, allowOpeningOfPlaceHolderPlan) =>
                if (!allowOpeningOfPlaceHolderPlan) {
                  mapRelations += ("PlaceHolder-" + child.toString -> distributed)
                }
                p

             case _ if error != -1 || (catalog.getTempView(name) match {
                case Some(StratifiedSample(_, _, _)) => true
                case Some(LogicalRelation(_: SamplingRelation, _, _)) => true
                case _ => false
              }) =>

                if (error == -1) {
                  throw new UnsupportedOperationException(s"With error clause is mandatory")
                  // error = ReplaceWithSampleTable.DEFAULT_ERROR
                }

                /* if (approxSet) { // Approx related values may have set in snc
                  confidence = conf.getConfString(Constant.confidence,
                    ReplaceWithSampleTable.DEFAULT_CONFIDENCE.toString).toDouble
                  behavior = HAC.getBehavior(Literal.create(conf.getConfString(Constant.behavior,
                    HAC.getBehaviorAsString(HAC.DO_NOTHING)), StringType))
                } */

                if (confidence == -1) {
                  confidence = Constant.DEFAULT_CONFIDENCE
                }


                val aqp: Option[(LogicalPlan, String)] = if (sampleTableIdentified.isEmpty) {
                  identifySampleTable(name, baseTableLP, requiredQcs)
                } else {
                  None
                }


                val newPlan = aqp match {
                  case Some((sample, name2)) =>
                    val baseSampleAttribs = sample match {
                      case s: StratifiedSample => s.child.output
                      case l: LogicalRelation => l.output
                    }

                    val baseMainAttribs = p.output
                    relationsProcessedInSubqueryNode += sample
                    val node = if (baseSampleAttribs.corresponds(baseMainAttribs)
                    (_.exprId == _.exprId)) {
                      SubqueryAlias(name2, sample, None)
                    } else {
                      val expressions = baseMainAttribs.zipWithIndex.map {
                        case (attribute, index) =>
                          Alias(baseSampleAttribs(index).withNullability(attribute.nullable),
                            baseSampleAttribs(index).name)(attribute.exprId, attribute.qualifier,
                            Some(attribute.metadata))
                      } :+ sample.output.last
                      logic.Project(expressions, SubqueryAlias(name2, sample, None))
                    }


                    val ratioAttribute = sample.output.find(
                      _.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME)
                    ).getOrElse(throw new IllegalStateException(
                      "Hidden column for ratio not found."))
                    ReplaceWithSampleTable.markForRecursion()
                    // ReplaceWithSampleTable.incrementNumTimesApplied()
                    mapRelations += (name -> true)

                    sampleTableIdentified = currentAlias
                    ErrorAndConfidence(error, confidence, node,
                      MapColumnToWeight(ratioAttribute),
                      if (analysisType.get == AnalysisType.Bootstrap) {
                        Some(BootstrapMultiplicity.generateBootstrapColumnReference)
                      } else {
                        None
                      }, isAQPDebug, isFixedSeed, analysisType,
                      BootstrapMultiplicity.getBootstrapMultiplicityAliasID,
                      numBootStrapTrials, behavior, name, RuleNumbers.REPLACE_WITH_SAMPLE_TABLE)

                  case None =>
                    val isDistributed = child match {
                      case LogicalRelation(cfr, _, _) => cfr match {
                        case x: PartitionedDataSourceScan => x.isPartitioned
                        case _ => true

                      }
                      case _ => false
                    }
                    mapRelations += (name -> isDistributed)
                    p
                }
                newPlan
              case LogicalRelation(cfr, _, _) =>
                mapRelations += (name -> (cfr match {
                  case x: PartitionedDataSourceScan => x.isPartitioned
                  case _ => true
                }))
                p
              case _ => p
            }
        }
      }


        val forceAnalysisToBootstrap = (isNonSampledRelationCase &&
          analysisType.get == AnalysisType.Closedform) || ( analysisType.isDefined &&
          analysisType.get == AnalysisType.Closedform &&
          sampleTableIdentified.isDefined &&
          (joinTypes.nonEmpty && (joinTypes.exists(_ != Inner) ||
            mapRelations.exists { case (name,
            distributed) => distributed &&
              name != sampleTableIdentified.get
            } ||
            hasCount || hasAvg
            )))

        val isDistributed = mapRelations.exists { case (name,
        distributed) => distributed
        } || sampleTableIdentified.isDefined
        var sampleTableFound = false
        val temp = if (sampleTableIdentified.isDefined || analysisType.isEmpty) {
          y.transformUp {
            case Error(_, child) => child
            case Confidence(_, child) => child
            case Behavior(_, child, _) => child
            case j@Join(left, right: ErrorAndConfidence, joinType, cond) =>
              right.copy(child = j.copy(right = right.child))
            case j@Join(left: ErrorAndConfidence, right, joinType, cond) =>
              left.copy(child = j.copy(left = left.child))
            case pl@PlaceHolderPlan(child, true, _, true) if sampleTableFound => child
            case er: ErrorAndConfidence if (forceAnalysisToBootstrap &&
              er.analysisType.map(_ != AnalysisType.Bootstrap).getOrElse(true)) =>
              sampleTableFound = true
              er.copy(analysisType = Some(AnalysisType.Bootstrap), bootstrapMultiplicities =
                Some(BootstrapMultiplicity.generateBootstrapColumnReference),
                ruleNumProcessing = RuleNumbers.REPLACE_WITH_SAMPLE_TABLE)
            case s: SampleTableQuery if (forceAnalysisToBootstrap) =>
              sampleTableFound = true
              s.copy(analysisType = Some(AnalysisType.Bootstrap), bootstrapMultiplicities =
                Some(BootstrapMultiplicity.generateBootstrapColumnReference),
                ruleNumProcessing = RuleNumbers.REPLACE_WITH_SAMPLE_TABLE)
            case ErrorDefaults(child) => child
            case x: UnaryNode => x.child match {
              case e: ErrorAndConfidence =>
                sampleTableFound = true
                val z = e.child
                e.copy(child = x.mapChildren(_ => z))
              case stq: SampleTableQuery =>
                sampleTableFound = true
                val z = stq.child
                stq.copy(child = x.mapChildren(_ => z))
              case _ => x
            }
            case x: SnappyUnaryNode => x.child match {
              case e: ErrorAndConfidence =>
                sampleTableFound = true
                val z = e.child
                e.copy(child = x.mapChildren(_ => z))
              case stq: SampleTableQuery =>
                sampleTableFound = true
                val z = stq.child
                stq.copy(child = x.mapChildren(_ => z))
              case _ => x
            }
          }
        } else {
          y
        }

        if (foundSampleQuery) {
          (ReplaceWithSampleTable.moveUpSampleTableQuery(temp), isDistributed,
            sampleTableFound, analysisType)
        } else {
          (temp, isDistributed, sampleTableFound, analysisType)
        }

      case _ => (plan, false, false, analysisType)
    }
  }

  // Invoke this method only when you are sure that
  // 1) conf contains error property
  // or 2) with error clause is present

  def getErrorDefaults(conf: SQLConf): (Double, Double, HAC.Type) = {
    val maxErrorAllowed = Property.MaxErrorAllowed.get(conf)
    var error = Property.Error.get(conf)
    // error < 0 means do not use aqp....
    if (error == 0 || error >= maxErrorAllowed) {
      Property.Error.set(conf, Constant.DEFAULT_ERROR)
      throw new UnsupportedOperationException("Please specify error within range of 0 to 1")
    } else if (error < 0) {
      error = -1d;
    }
    val confidence = Property.Confidence.getOption(conf).getOrElse(Constant.DEFAULT_CONFIDENCE)
    if (confidence <= 0 || confidence >= 1) {
      Property.Confidence.set(conf, Constant.DEFAULT_CONFIDENCE)
      throw new UnsupportedOperationException("" +
        "Please specify confidence within range of 0 to 1")
    }

    val behavior = HAC.getBehavior(Literal.create( Property.Behavior.getOption(conf).
      getOrElse(HAC.getBehaviorAsString(HAC.DO_NOTHING)), StringType))
    (error, confidence, behavior)
  }

  def handleProcessedPlan(plan: LogicalPlan, catalog: SnappyAQPSessionCatalog,
                          analyzerInvocation: AnalyzerInvocation, numBootStrapTrials: Int,
                          isAQPDebug: Boolean, isFixedSeed: Boolean, isDefaultClosedForm: Boolean,
                          conf: SQLConf, stq: SampleTableQuery):
  (LogicalPlan, Boolean, Boolean, Option[AnalysisType.Type]) = {
    val maxErrorAllowed = Property.MaxErrorAllowed.get(conf)
    var error: Double = -1
    var isDistributed = false
    var behavior: HAC.Type = HAC.getDefaultBehavior(conf)
    var behaviorSet: Boolean = false
    var confidence: Double = Property.Confidence.getOption(conf).getOrElse(-1)
    var foundNodestoRemove = false
    val temp = plan transformDown {
      case x@ErrorDefaults(child) =>
        val values = getErrorDefaults(conf)
        error = values._1
        confidence = values._2
        behavior = values._3
        foundNodestoRemove = true
        x

      case x@Error(expr, child) =>
        error = ReplaceWithSampleTable.convertToDouble(expr)
        if (error == 0 || error >= maxErrorAllowed) {
          throw new UnsupportedOperationException("Please specify error within range of 0 to 1")
        } else if (error < 0) {
          error = -1d;
        }
        foundNodestoRemove = true
        x

      case x@Confidence(expr, child) =>
        confidence = ReplaceWithSampleTable.convertToDouble(expr)
        if (confidence <= 0 || confidence >= 1) {
          throw new UnsupportedOperationException("Please specify confidence within " +
            "range of 0 to 1")
        }
        foundNodestoRemove = true
        x

      case x@Behavior(expr, child, _) =>
        behavior = HAC.getBehavior(expr)
        behaviorSet = true
        foundNodestoRemove = true
        x

      case s@SampleTableQuery(_, _, err, confi, _, _, _, _, analysisTypeX, _, _, behav, name, _, _,
       isDistributedX, _) =>
        isDistributed = isDistributedX
        if (error != -1 && error != err) {
          val newConfi = if (confidence != -1) {
            confidence
          } else {
            confi
          }

          val newBehav: HAC.Type = if (behavior != HAC.getDefaultBehavior(conf)) {
            behavior
          } else {
            behav
          }
          s.copy(error = error, confidence = newConfi, behavior = newBehav,
            ruleNumProcessing = RuleNumbers.REPLACE_WITH_SAMPLE_TABLE, makeVisible = true)
        } else {
          s.copy(ruleNumProcessing = RuleNumbers.REPLACE_WITH_SAMPLE_TABLE, makeVisible = true)
        }
    }

    (ReplaceWithSampleTable.moveUpSampleTableQuery(if (foundNodestoRemove) {
      temp.transformUp {
        case x: ErrorDefaults => x.child
        case x: Error => x.child
        case x: Confidence => x.child
        case x: Behavior => x.child
      }
    } else {
      temp
    }), isDistributed, true, stq.analysisType)
  }

  protected val ruleState = new ThreadLocal[(Boolean, Int, Boolean)]() {
    override protected def initialValue: (Boolean, Int, Boolean) = (false, 0, false)
  }

  def reset(): Unit = {
    ruleState.set((false, 0, false))
  }

  def isMarkedForRecursion: Boolean = ruleState.get._1

  def isQueryRoutingApplicable: Boolean = ruleState.get._3


  def markForRecursion(): Unit = {
    val (markedForRecursion, num, queryRoutingApplied) = ruleState.get
    if (!markedForRecursion) {
      ruleState.set((true, num, queryRoutingApplied))
    }
  }

  def markQueryRoutingApplicable(): Unit = {
    val (markedForRecursion, num, queryRoutingApplied) = ruleState.get
    if (!queryRoutingApplied) {
      ruleState.set((markedForRecursion, num, true))
    }
  }

  def getAnalysisType(plan: LogicalPlan, isDefaultClosedForm: Boolean): (Option[AnalysisType.Type],
    Boolean, Boolean, Boolean, Seq[String]) = {

    def checkFoldable(expr: Expression): Boolean =
     expr.transformUp {
        case tl: TokenizedLiteral => TokenizedLiteralWrapper(tl)
      }.foldable


    val (agg, hasAnyAggregate) =
      plan.collectFirst { case x: org.apache.spark.sql.catalyst.plans.logical.Aggregate => x }
      match {
        case Some(x) => (x, true)
        case None => (null, false)
      }
    var hasCount, hasAvg, hasSum = false
    if (hasAnyAggregate) {
      val hasUnresolvedAgg =
        agg.aggregateExpressions.find(p => p.children.exists(e => e.collectFirst({
          case x: UnresolvedFunction => x
        }) match {
          case Some(_) => true
          case None => false
        })) match {
          case Some(_) => true
          case None => false
        }
      if (!hasUnresolvedAgg) {
        val aggFuncs = plan.find {
          case _: org.apache.spark.sql.catalyst.plans.logical.Aggregate => true
          case _ => false
        } match {
          case Some(x: Aggregate) => {
            x.aggregateExpressions.map {
              _.children.map {
                _.collectFirst[AggregateFunction] {
                  case AggregateExpression(y, _, _, _) => y
                }.getOrElse(null)
              }
            }.flatten
          }
          case _ => Nil
        }
        val byPassErrorCalc = aggFuncs.exists {
          case _: AQPAverage => true
          case _: AQPCount => true
          case _: AQPSum => true
          case _ => false
        }

        val requiredQcs = aggFuncs.flatMap(func => func match {
          case Max(_: Literal | _: TokenizedLiteral) => Seq.empty
          case Min(_: Literal | _: TokenizedLiteral) => Seq.empty
          case Max(x) if checkFoldable(x) => Seq.empty
          case Min(x) if checkFoldable(x) => Seq.empty
          case Max(child) => Seq(util.toPrettySQL(child).toLowerCase)
          case Min(child) => Seq(util.toPrettySQL(child).toLowerCase)
          case _ => Seq.empty
        })
        hasAvg = aggFuncs.exists {
          case Average(_) => true
          case _ => false

        }

        hasCount = aggFuncs.exists {
          case Count(_) => true
          case _ => false
        }


        hasSum = aggFuncs.exists {
          case Sum(_) => true
          case _ => false
        }


        if(byPassErrorCalc) {
          (Some(AnalysisType.ByPassErrorCalc), false, false, false, requiredQcs)
        }
        else if (isDefaultClosedForm) {

          if (!hasAvg) {
            (Some(AnalysisType.Closedform), hasAvg, hasCount, hasSum, requiredQcs)

          } else {
            plan.find {
              case f: org.apache.spark.sql.catalyst.plans.logical.Filter =>
                // check if the filter exists below the aggregate or above the aggregate
                // if it exists above the aggregate , it is a having condition filter
                f.find(_ match {
                  case _: Aggregate => true
                  case _ => false
                }).isEmpty || f.condition.children.exists(p =>
                  p.collectFirst {
                    case ar: AttributeReference => ar
                  } match {
                    case Some(ar) =>
                      // if the attribute reference exists in grouping expression, then having gets
                      // transformed into avg with where clause
                      agg.groupingExpressions.exists(exp => exp.find {
                        case anAr: AttributeReference if anAr.exprId == ar.exprId => true
                        case _ => false
                      }.isDefined)
                    case None => false
                  }
                )
              case _ => false
            } match {
              case Some(_) => (Some(AnalysisType.Bootstrap), hasAvg, hasCount, hasSum, requiredQcs)
              case None => (Some(AnalysisType.Closedform), hasAvg, hasCount, hasSum, requiredQcs)
            }
          }

        }
        else {
          (Some(AnalysisType.Bootstrap), hasAvg, hasCount, hasSum, requiredQcs)
        }
      } else {
        (Some(AnalysisType.Defer), hasAvg, hasCount, hasSum, Seq.empty)
      }
    } else {
      (None, hasAvg, hasCount, hasSum, Seq.empty)
    }
  }

  private def convertToDouble(expr: Expression): Double = expr.dataType match {
    case _: IntegralType => expr.eval().asInstanceOf[Int]
    case _: DoubleType => expr.eval().asInstanceOf[Double]
    case _: FloatType => expr.eval().asInstanceOf[Float]
    case _: DecimalType => expr.eval().asInstanceOf[Decimal].toDouble
  }

  private def moveUpSampleTableQuery(plan: LogicalPlan): LogicalPlan = plan match {
    case s: SampleTableQuery => s
    case _ => {
      // we need to esnure that sample table query is
      // only above the first encountered aggregate node
      // check bug AQP-77 where a count type aggregate on
      // dataframe is invoked on an already processed aggregate
      var sq: SampleTableQuery = null
      var numAggSeen = 0
      val newPlan = plan.transformUp {
        case lp: LogicalPlan if numAggSeen <= 1 =>
          lp match {
            case _: Aggregate => numAggSeen += 1
            case _ =>
          }
          if (numAggSeen > 1) {
            val temp = lp.asInstanceOf[Aggregate].child
            val newChild = if (sq != null) {
              sq.copy(child = temp, makeVisible = false)
            } else {
              temp
            }
            lp.asInstanceOf[Aggregate].copy(child = newChild)
          } else {
            lp match {
              case x: SampleTableQuery => sq = x
                x.child
              case _ => lp
            }
            /* lp.children.find {
              case y: SampleTableQuery => true
              case _ => false
            } match {
              case Some(z: SampleTableQuery) => sq = z
                val newChildren = lp.children.map(ch => if (ch == sq) {
                  sq.child
                } else {
                  ch
                })
                lp.withNewChildren(newChildren)
              case None => lp
            } */
          }
      }

      if (numAggSeen <= 1) {
        sq.copy(child = newPlan, makeVisible = false)
      } else {
        newPlan
      }
    }
  }

}


@transient
case class GetErrorBounds(analyzerInvoc: AnalyzerInvocation)
  extends Rule[LogicalPlan] {
  val attributeTransformer: (LogicalPlan, String) => Option[Expression] = (toCheck: LogicalPlan,
                   errorName: String) => {
    val errorAggregateFunctions = toCheck.collect {
      case a: Aggregate => a.aggregateExpressions.flatMap {
        ne: NamedExpression => ne.collect {
          case eaf: ErrorAggregateFunction => (eaf, ne.name, ne.exprId)
        }
      }
    }.flatten
    val realExprIDs = for ((cfe, name, aggExprID) <- errorAggregateFunctions;
                           exprIDs = cfe.errorEstimateProjs.collect({
                             case (_, aggType, urfName, errFunctionName, exprID, rootEpr)
                               if (errorName.equalsIgnoreCase(urfName) || errorName.
                                 equalsIgnoreCase(errFunctionName)) => rootEpr
                           }); if exprIDs.nonEmpty) yield {
      (exprIDs.head, name, aggExprID)
    }
    if (realExprIDs.isEmpty) {
      None
    } else {
      val isNullable = true
      val(rootNamedExp, name, exprId) = realExprIDs.head
      Some(ErrorEstimateAttribute(name, DoubleType, isNullable, rootNamedExp.metadata, exprId,
        rootNamedExp.exprId)(rootNamedExp.qualifier))
    }
  }

  def errorClauseTransformer(toCheck: LogicalPlan): PartialFunction[Expression, Expression] = {
    case errorFunc: ErrorEstimateFunction => val fullUrf = errorFunc.prettyName + "(" +
      errorFunc.getParamName + ")"
      attributeTransformer(toCheck, fullUrf).getOrElse(throw new IllegalStateException(
        "Malformed query with error estimate in having clause not mapping with projection"))
    case attr: Attribute => attributeTransformer(toCheck, attr.name).getOrElse(attr)
  }

  def getErrorTransformerForHavingOrderByClause: PartialFunction[LogicalPlan, LogicalPlan] = {
    case filter: org.apache.spark.sql.catalyst.plans.logical.Filter =>
      filter.transformExpressionsUp { errorClauseTransformer(filter) }
    case  sort: Sort => sort.transformExpressionsUp(errorClauseTransformer(sort))
  }


  def getAggregateTransformer(confidence: Double, error: Double,
            analysisType: Option[AnalysisType.Type]): PartialFunction[LogicalPlan, LogicalPlan] = {
    case prj@logic.Project(pl, child) =>

      val output = child.output
      val newList1 = pl.filter(ne => {
        ne match {
          case ar: AttributeReference => output.exists(_.exprId == ar.exprId)
          case _ => true
        }
      })

      val newList2 = if (output.head.name == BootstrapMultiplicity.aggregateName &&
        newList1.head.name != BootstrapMultiplicity.aggregateName) {
        output.head +: newList1

      } else {
        newList1
      }
      prj.copy(projectList = newList2)

    case a: Aggregate =>
      val agg = a.aggregateExpressions.zipWithIndex
      val numHaving = a.aggregateExpressions.count(_.name == "havingCondition")
      val positionsToRemove = scala.collection.mutable.HashSet[Int]()

      val errorFunctions = agg.flatMap {
        case (ne, index) =>
          val collectedErrorFuncs = ne.collect {
            case x: ErrorEstimateFunction =>
              positionsToRemove.add(index)
              x
          }.map(_ -> (NamedExpression.newExprId, index - numHaving)).toMap
          // Replace the error estimate function in the
          // total expression with a mapping attribute reference
          val newRootExp = ne.transformUp {
            case exp: ErrorEstimateFunction =>
              AttributeReference(GetErrorBounds.AttributeNameForErrorEstimate,
                exp.dataType)(collectedErrorFuncs.get(exp).map(_._1).get)
          }.asInstanceOf[NamedExpression]
          collectedErrorFuncs.map { case (errFunc, (exprId, index)) => (errFunc, newRootExp, exprId,
            index)
          }

      }


      def copyErrorEstimates(aggregate: Aggregate, allClosedForm: Seq[ErrorAggregateFunction]):
    Aggregate = {
        val grouped = allClosedForm.groupBy(x => (x.children, x.aggregateType))
        val mapOldFuncToNewFuncIdentityMap = new  java.util.IdentityHashMap[ErrorAggregateFunction,
          ErrorAggregateFunction]()
        grouped.values.filter(_.size > 1).foreach { functions =>
          val distinctErrorEst = functions.flatMap(_.errorEstimateProjs).distinct
          if (!distinctErrorEst.isEmpty) {
           functions.foreach(old => {
              val newFunc = old match {
                case x: BootstrapSum => x.copy(_errorEstimateProjs = distinctErrorEst)
                case x: BootstrapCount => x.copy(_errorEstimateProjs = distinctErrorEst)
                case x: BootstrapAverage => x.copy(_errorEstimateProjs = distinctErrorEst)
                case x: ClosedFormErrorEstimate => x.copy(_errorEstimateProjs = distinctErrorEst)
              }
             mapOldFuncToNewFuncIdentityMap.put(old, newFunc)
            })

          }
        }
        aggregate.transformExpressionsUp {
          case old: ErrorAggregateFunction if (mapOldFuncToNewFuncIdentityMap.containsKey(old)) =>
            mapOldFuncToNewFuncIdentityMap.get(old)
        }
      }

      if (errorFunctions.isEmpty) {
        var foundOneNonEmptyError: Boolean = false
        val allClosedForm = a.aggregateExpressions.flatMap {
          _.collect { case x: ErrorAggregateFunction =>
            if (!foundOneNonEmptyError && x.errorEstimateProjs != Nil &&
              x.errorEstimateProjs.nonEmpty) {
              foundOneNonEmptyError = true
            }
            x
          }
        }
        if (foundOneNonEmptyError) {
          copyErrorEstimates(a, allClosedForm)
        } else {
          a
        }

      } else {
        val mapping = scala.collection.mutable.HashMap[NamedExpression, Seq[(Int,
          ErrorAggregate.Type, String, String, ExprId, NamedExpression)]]()

        errorFunctions.foreach {
          case (errorEstimateFunc, rootExp, virtulExprID, index) =>
            val errFuncNameStr = errorEstimateFunc.prettyName + "(" +
              errorEstimateFunc.getParamName + ")"
            agg.find {
              case (al: Alias, _) =>
                al.name.equalsIgnoreCase(errorEstimateFunc.getParamName)
              case _ => false
            } match {
              case Some((al@Alias(ErrorAggregateNumericConverter(AggregateExpression(
              aggFunc: ErrorAggregateFunction, _, _, _), _), _), _)) =>

                val aggType = ErrorAggregate.withName(aggFunc.aggregateType.toString
                  + "_" + errorEstimateFunc.prettyName.split("_").head)
                val projName = rootExp match {
                  case alias: Alias => alias.name
                  case _ => errFuncNameStr
                }
                val seq = mapping.getOrElse(al, Nil)
                mapping.put(al, (index, aggType, projName, errFuncNameStr,
                  virtulExprID, rootExp) +: seq)
              case Some((al@Alias(AggregateExpression(
               ((Max(_) | Min(_))), _, _, _), _), _)) => // do nothing
              case _ => throw new UnresolvedException(rootExp,
                errorEstimateFunc.prettyName)
            }
        }

        val modifiedExpressions = agg.collect {
          case (ne, indx) if !positionsToRemove.contains(indx) =>
            mapping.get(ne) match {
              case Some(seq) => ne.transformUp {
                case closedFormEst: ClosedFormErrorEstimate =>
                  closedFormEst.copy(_errorEstimateProjs = seq)
                case bootstrapAgg: DeclarativeBootstrapAggregateFunction =>
                  bootstrapAgg match {
                    case a: BootstrapAverage => a.copy(_errorEstimateProjs = seq)
                    case a: BootstrapSum => a.copy(_errorEstimateProjs = seq)
                    case a: BootstrapCount => a.copy(_errorEstimateProjs = seq)
                  }
              }.asInstanceOf[NamedExpression]
              case None => ne
            }
          case (ne, indx) if positionsToRemove.contains(indx) &&
            !mapping.values.exists( _.exists(_._1 == indx)) =>
            val errorEstimateName = errorFunctions.find(_._4 == indx).get._1.prettyName
            (if (ErrorAggregate.isRelativeErrorAggType(errorEstimateName)) {
              Alias (Literal(0d), ne.name)(agg(indx)._1.exprId)
            } else if (ErrorAggregate.isAbsoluteErrorAggType(errorEstimateName)) {
              Alias (Literal(0d), ne.name)(agg(indx)._1.exprId)
            } else if (ErrorAggregate.isLowerAggType(errorEstimateName)) {
              Alias (Literal(null), ne.name)(agg(indx)._1.exprId)
            } else {
              Alias (Literal(null), ne.name)(agg(indx)._1.exprId)
            }).asInstanceOf[NamedExpression]
        }
        // Collect all closed form error estimates & copy error estimates in
        // equivalent aggregate function.
        // as we do not know which will get eliminated in optimzation phase
        var foundOneNonEmptyError: Boolean = false
        val allClosedForm = modifiedExpressions.flatMap {
          _.collect { case x: ErrorAggregateFunction =>
            if (!foundOneNonEmptyError && x.errorEstimateProjs != Nil &&
              x.errorEstimateProjs.nonEmpty) {
              foundOneNonEmptyError = true
            }
            x
          }
        }
        val newAgg = a.copy(aggregateExpressions = modifiedExpressions)
        if(foundOneNonEmptyError) {
          copyErrorEstimates(newAgg, allClosedForm)
        } else {
          newAgg
        }
      }
    case err: ErrorAndConfidence => err.copy(ruleNumProcessing = RuleNumbers.GET_ERROR_BOUNDS)
    case sq: SampleTableQuery => sq.copy(ruleNumProcessing = RuleNumbers.GET_ERROR_BOUNDS)
  }

  def apply(plan: LogicalPlan): LogicalPlan =
    if (analyzerInvoc.getCallStackCount > 0 || !plan.resolved) {
      plan
    } else {
      PlaceHolderPlan.applyRecursivelyToInvisiblePlaceHolderPlan(plan, applyGetErrorBounds)
    }

  def applyGetErrorBounds(plan: LogicalPlan): LogicalPlan = {
    plan.collectFirst[Any] {
      case a: ErrorAndConfidence => a
      case b: SampleTableQuery => b
    } match {

      case Some(errAndConf: ErrorAndConfidence) if (errAndConf.ruleNumProcessing !=
        RuleNumbers.GET_ERROR_BOUNDS) =>
        plan transformUp {
          getAggregateTransformer(errAndConf.confidence, errAndConf.error,
            errAndConf.analysisType) orElse getErrorTransformerForHavingOrderByClause

        }
      case Some(s: SampleTableQuery) if (s.makeVisible && s.ruleNumProcessing !=
        RuleNumbers.GET_ERROR_BOUNDS) =>
        plan transformUp {
          getAggregateTransformer(s.confidence, s.error, s.analysisType) orElse
            getErrorTransformerForHavingOrderByClause
        }
      case _ => plan
    }
  }
}

object GetErrorBounds {
  val AttributeNameForErrorEstimate = "Virtual Error Attrbute"
}

@transient
case class MoveUpBootstrapReferencerConditionally(analyzerInvoc: AnalyzerInvocation)
  extends Rule[LogicalPlan] {


  def apply(plan: LogicalPlan): LogicalPlan =
    if (analyzerInvoc.getCallStackCount > 0 || !plan.resolved) {
      plan
    } else {
      PlaceHolderPlan.applyRecursivelyToInvisiblePlaceHolderPlan(plan, applyBootstrapReferencerRule)
    }

  def applyBootstrapReferencerRule(plan: LogicalPlan): LogicalPlan = {
    plan.collectFirst[Any] {
      case a: ErrorAndConfidence => a
      case s: SampleTableQuery if (s.makeVisible) => s
    } match {
      case Some(err: ErrorAndConfidence) => if (err.analysisType == AnalysisType.Bootstrap &&
        err.ruleNumProcessing != RuleNumbers.MOVE_UP_BOOTSTRAP) {
        plan transform MoveUpBootstrapReferencerConditionally.transformer
      } else {
        plan
      }
      case Some(sq: SampleTableQuery) => if (sq.analysisType == AnalysisType.Bootstrap &&
        sq.ruleNumProcessing != RuleNumbers.MOVE_UP_BOOTSTRAP) {
        plan transform MoveUpBootstrapReferencerConditionally.transformer
      } else {
        plan
      }
      case _ => plan
    }
  }
}

object MoveUpBootstrapReferencerConditionally {
  val transformer: PartialFunction[LogicalPlan, LogicalPlan] = {
    case filter@logic.Filter(cond, br@BootstrapReferencer(child, bsAliasID, bsAtt)) =>
      BootstrapReferencer(logic.Filter(cond, child), bsAliasID, bsAtt)
    case sort@logic.Sort(order, global, br@BootstrapReferencer(child, bsAliasID, bsAtt)) =>
      BootstrapReferencer(logic.Sort(order, global, child), bsAliasID, bsAtt)
    case err: ErrorAndConfidence => err.copy(ruleNumProcessing = RuleNumbers.MOVE_UP_BOOTSTRAP)
    case sq: SampleTableQuery => sq.copy(ruleNumProcessing = RuleNumbers.MOVE_UP_BOOTSTRAP)
  }
}


@transient
case class ErrorEstimateRule(analyzerInvoc: AnalyzerInvocation)
  extends Rule[LogicalPlan] {

  def getClosedFormAggregateFunctionTransformer(confidence: Double, error: Double,
                                   sampleRatioAttribute: MapColumnToWeight, behavior: HAC.Type)
  : PartialFunction[LogicalPlan, LogicalPlan] = {
    case a: org.apache.spark.sql.catalyst.plans.logical.Aggregate =>
      a transformExpressionsUp {
        case aggExp@AggregateExpression(Average(AQPFunctionParameter(columns,
        mcw: MapColumnToWeight, actualAggDataType)), _, _, _) =>
          ErrorAggregateNumericConverter(aggExp.copy(aggregateFunction =
            ClosedFormErrorEstimate(columns.head, mcw.rawWeight, confidence,
              ErrorAggregate.Avg, error, Nil, behavior)), actualAggDataType)

        case aggExp@AggregateExpression(Sum(AQPFunctionParameter(columns,
        mcw: MapColumnToWeight, actualAggDataType)), _, _, _) =>
          ErrorAggregateNumericConverter(aggExp.copy(aggregateFunction =
            ClosedFormErrorEstimate(columns.head, mcw.rawWeight, confidence,
              ErrorAggregate.Sum, error, Nil, behavior)), actualAggDataType)

        case aggExp@AggregateExpression(Count(Seq(AQPFunctionParameter(columns,
        mcw: MapColumnToWeight, actualAggDataType))), _, _, _) =>
          ErrorAggregateNumericConverter(aggExp.copy(aggregateFunction =
            ClosedFormErrorEstimate(columns.head, mcw.rawWeight, confidence,
              ErrorAggregate.Count, error, Nil, behavior)), actualAggDataType)

        case aggExp@AggregateExpression(Count(Seq(cfp@AQPFunctionParameter(
        _, _, actualAggType))), _, _, _) =>
          ErrorAggregateNumericConverter(aggExp.copy(aggregateFunction =
            ClosedFormErrorEstimate(cfp.columns.head,
              cfp.weight.rawWeight, confidence, ErrorAggregate.Count,
              error, Nil, behavior)), actualAggType)

        case cfe@ClosedFormErrorEstimate(_, _, _, _, err, _, _) =>
          if (error != err) {
            cfe.copy(_confidence = confidence, _error = error,
              _behavior = behavior)
          } else {
            cfe
          }
      }
  }

  def getBootstrapAggregateFunctionTransformer(confidence: Double, error: Double,
                   sampleRatioAttribute: MapColumnToWeight, bootstrapMultiplicities: Attribute,
                   isAQPDebug: Boolean, isFixedSeed: Boolean, bsAliasID: ExprId,
                   numBootstrapTrials: Int, behavior: HAC.Type)
  : PartialFunction[LogicalPlan, LogicalPlan] = {

    case a: org.apache.spark.sql.catalyst.plans.logical.Aggregate
    =>
      var weightCol: MapColumnToWeight = null

      var index: Int = 0

      val createBSAgg = a.aggregateExpressions.find(_.exprId == bsAliasID) match {
        case Some(_) => false
        case None => true
      }
     val modifiedAgg = a transformExpressionsUp {

        case aggExp@AggregateExpression(Average(AQPFunctionParameter(
        Seq(actualColumn), weight, actualAggType)), _, _, _) =>
          if (weightCol == null) {
            weightCol = weight
          }

          val bootstrapFunction = BootstrapAverage(actualColumn, numBootstrapTrials,
            confidence, error, Nil, isAQPDebug, behavior)

          index += 1
          createNewExpression(aggExp, bootstrapFunction, isAQPDebug, actualAggType)

        case aggExp@AggregateExpression(sum@Sum(AQPFunctionParameter(
        Seq(actualColumn), weight, actualAggType)), _, _, _) =>
          if (weightCol == null) {
            weightCol = weight
          }

          val bootstrapFunction = BootstrapSum(actualColumn, numBootstrapTrials,
            confidence, error, Nil, isAQPDebug, behavior)
          index += 1
          createNewExpression(aggExp, bootstrapFunction, isAQPDebug, actualAggType)

        case aggExp@AggregateExpression(Count(Seq() :+ AQPFunctionParameter(columns,
        weight, actualAggType)), _, _, _) =>
          if (weightCol == null) {
            weightCol = weight
          }
          val allNotNull = columns.forall(!_.nullable)

          val bootstrapFunction = BootstrapCount(columns, numBootstrapTrials,
              confidence, error, Nil, isAQPDebug, behavior)
          index += 1
          createNewExpression(aggExp, bootstrapFunction, isAQPDebug, actualAggType)

        case bs: DeclarativeBootstrapAggregateFunction =>
          index += 1
          if (error != bs.error) {
            bs match {
              case a: BootstrapAverage =>
                a.copy(_confidence = confidence, _error = error, _behavior = behavior)
              case a: BootstrapSum =>
                a.copy(_confidence = confidence, _error = error, _behavior = behavior)
              case a: BootstrapCount =>
                a.copy(_confidence = confidence, _error = error, _behavior = behavior)
            }
          } else {
            bs
          }
      }

      val newAggregate = modifiedAgg

      if (createBSAgg && index > 0) {
        val modBsMultiplicityFunc =
          BootstrapMultiplicityAggregate(weightCol, bootstrapMultiplicities,
            numBootstrapTrials)
        BootstrapReferencer(newAggregate.copy(aggregateExpressions = Alias(modBsMultiplicityFunc.
          toAggregateExpression(false), BootstrapMultiplicity.aggregateName)(bsAliasID) +:
          newAggregate.aggregateExpressions), bsAliasID, bootstrapMultiplicities)

      } else if (index > 0) {
        var bsIndex = 0
        val aggExps = newAggregate.aggregateExpressions
        val bsAgg = aggExps.zipWithIndex.find {
          case (ne, i) => if (ne.exprId == bsAliasID) {
            bsIndex = i
            true
          } else {
            false
          }
        }.get._1
        if (bsIndex > 0) {
          newAggregate.copy(aggregateExpressions = bsAgg +: (
            aggExps.slice(0, bsIndex) ++
              (if (bsIndex + 1 < aggExps.length) {
                aggExps.slice(bsIndex + 1, aggExps.length)
              } else {
                Nil
              })))
        } else {
          newAggregate
        }

      } else {
        newAggregate
      }
  }

  def createNewExpression(aggExpOld: AggregateExpression,
                          aggFunc: AggregateFunction, isAQPDebug: Boolean,
                          actualAggType: DataType): Expression = {
    val newAggExp = aggExpOld.copy(aggregateFunction = aggFunc)
    if (isAQPDebug) {
      newAggExp
    } else {
      ErrorAggregateNumericConverter(newAggExp, actualAggType)
    }
  }


  def apply(plan: LogicalPlan): LogicalPlan =
    if (analyzerInvoc.getCallStackCount > 0 || !plan.resolved) {
      plan
    } else {
      PlaceHolderPlan.applyRecursivelyToInvisiblePlaceHolderPlan(plan, applyErrorEstimateRule)
    }


  def applyErrorEstimateRule(plan: LogicalPlan): LogicalPlan = {
    plan.collectFirst[Any] {
      case a: ErrorAndConfidence => a
      case s: SampleTableQuery if (s.makeVisible) => s
    } match {

      case Some(ErrorAndConfidence(-1, _, _, _, _, _, _, _, _, _, _, _, _)) => plan.transformUp {
        case err: ErrorAndConfidence => err.copy(
          ruleNumProcessing = RuleNumbers.ERROR_ESTIMATE_RULE)
      }
      case Some(ErrorAndConfidence(_, _, _, _, _, _, _, None, _, _, _, _, _)) => plan.transformUp {
        case err: ErrorAndConfidence => err.copy(
          ruleNumProcessing = RuleNumbers.ERROR_ESTIMATE_RULE)
      }

      case Some(SampleTableQuery(_, _, -1, _, _, _, _, _, _, _, _, _, _, _, _, _, _)) =>
        plan.transformUp {
        case stq: SampleTableQuery => stq.copy(ruleNumProcessing = RuleNumbers.ERROR_ESTIMATE_RULE)
      }
      case Some(SampleTableQuery(_, _, _, _, _, _, _, _, None, _, _, _, _, _, _, _, _)) =>
        plan.transformUp {
        case stq: SampleTableQuery => stq.copy(ruleNumProcessing = RuleNumbers.ERROR_ESTIMATE_RULE)
      }

      case Some(errAndConf: ErrorAndConfidence) if (errAndConf.ruleNumProcessing !=
        RuleNumbers.ERROR_ESTIMATE_RULE) =>
        plan.transformUp {
          if (errAndConf.analysisType.get == AnalysisType.Closedform) {
            getClosedFormAggregateFunctionTransformer(errAndConf.confidence,
              errAndConf.error, errAndConf.sampleRatioAttribute,
              errAndConf.behavior)

          } else if (errAndConf.analysisType.get == AnalysisType.Bootstrap) {
            getBootstrapAggregateFunctionTransformer(errAndConf.confidence,
              errAndConf.error, errAndConf.sampleRatioAttribute,
              errAndConf.bootstrapMultiplicities.get, errAndConf.isAQPDebug,
              errAndConf.isFixedSeed, errAndConf.bsMultiplictyAliasID,
              errAndConf.numBootstrapTrials, errAndConf.behavior)
          } else {
             PartialFunction.empty
          }.orElse(
            {
              case errAndConf: ErrorAndConfidence => errAndConf.copy(ruleNumProcessing =
                RuleNumbers.ERROR_ESTIMATE_RULE)
            }
          )
        }

      case Some(s: SampleTableQuery) if (s.ruleNumProcessing != RuleNumbers.ERROR_ESTIMATE_RULE) =>
        plan.transformUp {
          if (s.analysisType.get == AnalysisType.Closedform) {
            getClosedFormAggregateFunctionTransformer(s.confidence, s.error,
              s.sampleRatioAttribute.sampleRatioAttribute, s.behavior)
          } else if (s.analysisType.get == AnalysisType.Bootstrap){
            getBootstrapAggregateFunctionTransformer(s.confidence, s.error,
              s.sampleRatioAttribute.sampleRatioAttribute,
              s.bootstrapMultiplicities.get, s.isAQPDebug, s.isFixedSeed,
              s.bsMultiplictyAliasID, s.numBSTrials, s.behavior)
          } else {
             PartialFunction.empty
          }.orElse(
            {
              case sq: SampleTableQuery => sq.copy(ruleNumProcessing =
                RuleNumbers.ERROR_ESTIMATE_RULE)
            }
          )
        }
      case _ => plan
    }
  }
}


case class Error(errorExpr: Expression, child: LogicalPlan)
  extends logic.UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class Confidence(confidenceExpr: Expression, child: LogicalPlan)
  extends logic.UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class Behavior(behaviorExpr: Expression, child: LogicalPlan,
                    forcedRoute: Boolean = false) extends logic.UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class ErrorDefaults(child: LogicalPlan) extends logic.UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class ErrorAndConfidence(error: Double, confidence: Double,
                              child: LogicalPlan, sampleRatioAttribute: MapColumnToWeight,
                              bootstrapMultiplicities: Option[Attribute], isAQPDebug: Boolean,
                              isFixedSeed: Boolean, analysisType: Option[AnalysisType.Type],
                              bsMultiplictyAliasID: ExprId, numBootstrapTrials: Int,
                              behavior: HAC.Type, sampleTable: String, ruleNumProcessing: Int = -1)
  extends logic.UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def producedAttributes: AttributeSet = bootstrapMultiplicities.map(AttributeSet(_)).
    getOrElse(AttributeSet.empty)
}

case class ErrorAggregateNumericConverter(child: Expression,
                                          masqueradingType: DataType) extends UnaryExpression {
  override def dataType: DataType = masqueradingType

  override def nullable: Boolean = child.nullable

  override def toString: String = s"ErrorAggregateNumericConverter($child)"

  override def eval(input: InternalRow): Any =
    Cast(child, masqueradingType).eval(input)

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval = Cast(child, masqueradingType).genCode(ctx)
    val code =
      s"""
      ${eval.code}
      boolean ${ev.isNull} = ${eval.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${eval.value};
    """
    ev.copy(code = code)
  }
}

object ImplementSnappyAggregate extends Rule[ExecSparkPlan] {
  val initialState = -1
  val partState = 0
  val shuffleState = 1

  override def apply(plan: ExecSparkPlan): ExecSparkPlan =
    SampleTablePlan.applyRecursivelyToInvisiblePlan(plan, applyRule)

  def applyRule(plan: ExecSparkPlan, position: Int): ExecSparkPlan =
    if ((AQPRules.isClosedForm(plan, position) || AQPRules.isBootStrapAnalysis(plan, position))
      && !AQPRules.getAQPInfo(plan, position).get.isAQPDebug) {
      var foundErrorEstimates = false
      var errorEstimateAttributes: Seq[(NamedExpression, Int)] = null
      var readyForAggregateChangeState: Int = plan.collectFirst {
        case x: Exchange => x
      }.map(_ => initialState).getOrElse(partState)

      val behavior = AQPRules.getAQPInfo(plan, position).get.behavior
      val numBaseAggs = AQPRules.getAQPInfo(plan, position).get.numBaseAggs

      var hiddenCols: Option[Seq[NamedExpression]] = None
      var getSortingOrder: Option[Seq[SortOrder]] = None



      plan.transformUp {
        case ex: Exchange if (readyForAggregateChangeState == partState) =>
          readyForAggregateChangeState = shuffleState
          ex
        case sortedAgg@SortAggregateExec(requiredChildDistributionExpressions,
        groupingExpressions, aggregateExpressions, aggregateAttributes,
        initialInputBufferOffset, resultExpressions, child) =>
          val needsAnalysis = if (AQPRules.isClosedForm(plan, position)) {
            sortedAgg.aggregateExpressions.exists(aggExp =>
              aggExp.aggregateFunction match {
                case _: ClosedFormErrorEstimate => true
                case _ => false
              })

          } else if (AQPRules.isBootStrapAnalysis(plan, position)) {
            aggregateAttributes.head.name == BootstrapMultiplicityAggregate.name ||
            aggregateAttributes.last.name == BootstrapMultiplicityAggregate.name ||
            aggregateAttributes.head.name == BootstrapMultiplicityAggregate.partialAttributeName ||
            aggregateAttributes.last.name == BootstrapMultiplicityAggregate.partialAttributeName

          } else {
            false
          }
          if (readyForAggregateChangeState == shuffleState ||
            readyForAggregateChangeState == partState) {
            if (needsAnalysis) {
              val (newAggs, newAttribs) = (aggregateExpressions, aggregateAttributes)

              val relativeErrorExprIDs = if (AQPRules.requireHiddenRelativeErrors(behavior)) {
                Some(Seq.fill[ExprId](numBaseAggs)(NamedExpression.newExprId))
              } else {
                None
              }
              val retVal = new SnappySortAggregate(
                requiredChildDistributionExpressions, groupingExpressions,
                newAggs, newAttribs, initialInputBufferOffset,
                resultExpressions, true, child, relativeErrorExprIDs)
              errorEstimateAttributes = ResultsExpressionCreator.errorEstimateAttributes(
                retVal.baseResultExpressions, retVal.resultExpressions, aggregateExpressions)
              foundErrorEstimates = errorEstimateAttributes.nonEmpty

              val numHiddenCols = (if (AQPRules.requireHiddenRelativeErrors(behavior)) numBaseAggs
              else 0) +
                (if (AQPRules.requireHiddenGroupBy(behavior)) groupingExpressions.length else 0)

              if (numHiddenCols > 0) {
                val len = retVal.resultExpressions.length
                hiddenCols = Some(retVal.resultExpressions.slice(len - numHiddenCols, len).
                  map(_.toAttribute))
              }
              retVal
            } else {
              sortedAgg
            }
          } else {

            if (needsAnalysis) {
              val (newAggs, newAttribs) = (aggregateExpressions, aggregateAttributes)

              readyForAggregateChangeState = partState
              new SnappyPartialSortedAggregate(
                requiredChildDistributionExpressions, groupingExpressions,
                newAggs, newAttribs, initialInputBufferOffset,
                resultExpressions, true, child)
            } else {
              sortedAgg
            }
          }
        case hashAgg@SnappyHashAggregateExec(requiredChildDistributionExpressions,
        groupingExpressions, aggregateExpressions, aggregateAttributes,
        resultExpressions, child, hasDistinct) =>
          val needsAnalysis = if (AQPRules.isClosedForm(plan, position)) {
            hashAgg.aggregateExpressions.find(aggExp =>
              aggExp.aggregateFunction match {
                case _: ClosedFormErrorEstimate => true
                case _ => false
              }).exists(_ => true)
          } else if (AQPRules.isBootStrapAnalysis(plan, position)) {
            aggregateAttributes.head.name == BootstrapMultiplicityAggregate.name ||
            aggregateAttributes.last.name == BootstrapMultiplicityAggregate.name ||
            aggregateAttributes.head.name == BootstrapMultiplicityAggregate.partialAttributeName ||
            aggregateAttributes.last.name == BootstrapMultiplicityAggregate.partialAttributeName
          } else {
            false
          }
          if (readyForAggregateChangeState == shuffleState ||
            readyForAggregateChangeState == partState) {
            if (needsAnalysis) {
              val (newAggs, newAttribs) = (aggregateExpressions, aggregateAttributes)

              val relativeErrorExprIDs = if (AQPRules.requireHiddenRelativeErrors(behavior)) {
                Some(Seq.fill[ExprId](numBaseAggs)(NamedExpression.newExprId))
              } else {
                None
              }
              val retVal = new SnappyHashAggregate(
                requiredChildDistributionExpressions, groupingExpressions,
                newAggs, newAttribs, resultExpressions, true, child, hasDistinct,
                relativeErrorExprIDs)
              errorEstimateAttributes = ResultsExpressionCreator.errorEstimateAttributes(
                retVal.baseResultExpressions, retVal.resultExpressions, aggregateExpressions)
              foundErrorEstimates = errorEstimateAttributes.nonEmpty

              val numHiddenCols = (if (AQPRules.requireHiddenRelativeErrors(behavior)) numBaseAggs
              else 0) +
                (if (AQPRules.requireHiddenGroupBy(behavior)) groupingExpressions.length else 0)

              if (numHiddenCols > 0) {
                val len = retVal.resultExpressions.length
                hiddenCols = Some(retVal.resultExpressions.slice(len - numHiddenCols, len).
                  map(_.toAttribute))
              }
              retVal
            } else {
              hashAgg
            }
          } else {

            if (needsAnalysis) {
              val (newAggs, newAttribs) = (aggregateExpressions, aggregateAttributes)

              readyForAggregateChangeState = partState
              new SnappyPartialHashAggregate(
                requiredChildDistributionExpressions, groupingExpressions,
                newAggs, newAttribs, resultExpressions, true, child, hasDistinct)
            } else {
              hashAgg
            }
          }

        case ProjectExec(list, child) if (foundErrorEstimates || hiddenCols.isDefined) =>
          val newList = mergeErrorAttributesAndGroupingCols(list, foundErrorEstimates,
            errorEstimateAttributes, hiddenCols)

          ProjectExec(newList, child)

        case p@TakeOrderedAndProjectExec(limit, sortOrder, projectList, child)
          if foundErrorEstimates || hiddenCols.isDefined =>

          getSortingOrder = Some(sortOrder)

          val newList = mergeErrorAttributesAndGroupingCols(projectList, foundErrorEstimates,
            errorEstimateAttributes, hiddenCols)
          TakeOrderedAndProjectExec(limit, sortOrder, newList, child)

        case SampledRelation(child) => child

        case sortPlan@SortExec(sortOrder, _, _, _) if hiddenCols.isDefined =>
          getSortingOrder = Some(sortOrder)
          sortPlan

        case sort: SnappySortExec if hiddenCols.isDefined =>
          getSortingOrder = Some(sort.sortPlan.sortOrder)
          sort

        case s: SampleTablePlan if hiddenCols.isDefined => s.copy(
          hiddenCols = hiddenCols, realSortingOrder = getSortingOrder)

      }
    } else {
      plan
    }

  def mergeErrorAttributesAndGroupingCols(list: Seq[NamedExpression], foundErrorEstimates: Boolean,
                                          errorEstimateAttributes: Seq[(NamedExpression, Int)],
                                          groupingExpression: Option[Seq[NamedExpression]]):
  Seq[NamedExpression] = {
    val part1 = if (foundErrorEstimates) {
      mergeAttributes(list, errorEstimateAttributes)
    } else {
      list
    }
    if (groupingExpression.isDefined) {
      part1 ++ groupingExpression.map(_.map(_.toAttribute)).get
    } else {
      part1
    }
  }

  def mergeAttributes(list: Seq[NamedExpression],
                      otherList: Seq[(NamedExpression, Int)]): Seq[NamedExpression] = {
    var i = 0
    var j = 0
    val y = for (x <- list) yield {
      var loopCondition = j < otherList.length && i == otherList(j)._2 - 1
      if (loopCondition) {
        i = i + 1
        x +: (for (k <- otherList.slice(j, otherList.length) if loopCondition) yield {
          j = j + 1
          i = i + 1
          loopCondition = j < otherList.length && i == otherList(j)._2
          k._1
        })

      } else {
        i = i + 1
        Seq(x)
      }
    }
    y.flatten
  }
}


object CleanupErrorEstimateAttribute extends Rule[ExecSparkPlan] {

  override def apply(plan: ExecSparkPlan): ExecSparkPlan = SampleTablePlan.
    applyRecursivelyToInvisiblePlan(plan, applyRule)

  def applyRule(plan: ExecSparkPlan, position: Int): ExecSparkPlan =
    if (AQPRules.isClosedForm(plan, position) || AQPRules.isBootStrapAnalysis(plan, position)) {
      plan.transformUp {
        case q: ExecSparkPlan => q.transformExpressionsUp {
          case eea@ErrorEstimateAttribute(name, dataType, nullable, metadata, _, realExprId) =>
            AttributeReference(name, dataType, nullable, metadata)(realExprId, eea.qualifier,
              eea.isGenerated)
        }
      }
    } else {
      plan
    }

}


case class ByPassErrorCalculationsConditionally(analyzerInvoc: AnalyzerInvocation,
  isAQPDebug: Boolean) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = if (analyzerInvoc.
    getCallStackCount > 0 || !plan.resolved || isAQPDebug) {
    plan
  } else {
    PlaceHolderPlan.applyRecursivelyToInvisiblePlaceHolderPlan(plan,
      applyByPassErrorCalculationsConditionally)
  }

  def applyByPassErrorCalculationsConditionally(plan: LogicalPlan): LogicalPlan = {
    plan.collectFirst {
      case a: ErrorAndConfidence if (a.analysisType.isDefined &&
        a.analysisType != AnalysisType.Defer && a.sampleRatioAttribute != null
        && a.behavior == HAC.DO_NOTHING) => (a.sampleRatioAttribute, a.analysisType,
        a.ruleNumProcessing)
      case b: SampleTableQuery if (b.makeVisible && b.analysisType.isDefined
        && b.analysisType != AnalysisType.Defer && b.sampleRatioAttribute != null
        && b.behavior == HAC.DO_NOTHING) => (b.sampleRatioAttribute.sampleRatioAttribute,
        b.analysisType, b.ruleNumProcessing)
    } match {
      case Some((weight, analysis, ruleNum)) if (ruleNum != RuleNumbers.ByPassErrorCalc) =>
        var shouldByPassErrorCalc = false
        var processedAnalysis = false
        val mod1 = plan transformUp {
          case agg: Aggregate =>
            shouldByPassErrorCalc = !agg.aggregateExpressions.exists(ne => ne.find {
              case errorAgg: ErrorAggregateFunction if (errorAgg.errorEstimateProjs != null &&
                !errorAgg.errorEstimateProjs.isEmpty) => true
              case _ => false
            }.isDefined)

            if (shouldByPassErrorCalc) {
              var foundAnyErrorAggregates = false
              val newAgg = agg transformExpressionsUp {
                case aggExp@AggregateExpression(errorAgg: ErrorAggregateFunction, _, _, _) => {
                  foundAnyErrorAggregates = true
                  aggExp.copy(aggregateFunction = errorAgg.convertToBypassErrorCalcFunction(weight))
                }
                case ErrorAggregateNumericConverter(child, actualDataType) =>
                  Cast(child, actualDataType)
              }
              if(foundAnyErrorAggregates) {
                if (analysis.get == AnalysisType.Bootstrap) {
                  newAgg.copy(aggregateExpressions = newAgg.aggregateExpressions.drop(1))
                } else {
                  newAgg
                }
              } else {
                shouldByPassErrorCalc = false
                agg
              }
            } else {
              agg
            }
          case BootstrapReferencer(child, _, _) if (shouldByPassErrorCalc) => child
          case s: SampleTableQuery if (s.makeVisible && shouldByPassErrorCalc) => {
            processedAnalysis = true
            s.copy(analysisType = Some(AnalysisType.ByPassErrorCalc))
          }
          case err: ErrorAndConfidence  if (shouldByPassErrorCalc) => {
            processedAnalysis = true
            err.copy(analysisType = Some(AnalysisType.ByPassErrorCalc))
          }
        }

        if(shouldByPassErrorCalc && !processedAnalysis) {
          mod1 transformUp {
            case s: SampleTableQuery => s.copy(analysisType = Some(AnalysisType.ByPassErrorCalc))
            case err: ErrorAndConfidence => err.copy(
              analysisType = Some(AnalysisType.ByPassErrorCalc))
          }
        } else if (shouldByPassErrorCalc) {
          mod1
        } else {
          plan
        }


      case _ => plan
    }
  }
}


case class EnsureSampleWeightageColumn(analyzerInvoc: AnalyzerInvocation)
  extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = if (analyzerInvoc.getCallStackCount > 0) {
    plan
  } else {
    PlaceHolderPlan.applyRecursivelyToInvisiblePlaceHolderPlan(plan, applySampleWeightageRule)
  }

  def applySampleWeightageRule(plan: LogicalPlan): LogicalPlan = {
    val (weightageAttribute, ruleNumProcessing) = plan.collectFirst[Any] {
      case a: ErrorAndConfidence => a
      case s: SampleTableQuery if (s.makeVisible) => s
    } match {
      case Some(err: ErrorAndConfidence) =>
        err.sampleRatioAttribute.rawWeight -> err.ruleNumProcessing
      case Some(s: SampleTableQuery) =>
        s.sampleRatioAttribute.sampleRatioAttribute.rawWeight -> s.ruleNumProcessing
      case _ => (null, -1)

    }
    if (weightageAttribute != null && ruleNumProcessing != RuleNumbers.ENSURE_SAMPLE_COLUMN) {
      var reachedAggregate = false
      plan transformUp {
        case logic.Project(projectList, child) if !(reachedAggregate ||
          projectList.exists(_.exprId == weightageAttribute.exprId)) &&
          child.output.exists(_.exprId == weightageAttribute.exprId) =>
          logic.Project(projectList :+ weightageAttribute, child)

        case gen@logic.Generate(_, _, _, _, out, child) if !(reachedAggregate ||
          out.exists(_.exprId == weightageAttribute.exprId)) &&
          child.output.exists(_.exprId == weightageAttribute.exprId) =>
          gen.copy(generatorOutput = out :+
            weightageAttribute.asInstanceOf[Attribute])

        case win@logic.Window(pl, _, _, child) if !(reachedAggregate
          || pl.exists(_.exprId == weightageAttribute.exprId)) &&
          child.output.exists(_.exprId == weightageAttribute.exprId) =>
          win.copy(windowExpressions = pl :+
            weightageAttribute.asInstanceOf[Attribute])

        case expand@logic.Expand(projections, output, child) if !(reachedAggregate
          || output.exists(_.exprId == weightageAttribute.exprId)) &&
          child.output.exists(_.exprId == weightageAttribute.exprId) =>
          expand.copy(output = output :+ weightageAttribute
            .asInstanceOf[Attribute], projections = projections.map(
            _ :+ weightageAttribute))

        case agg: Aggregate =>
          reachedAggregate = true
          agg
        case err: ErrorAndConfidence =>
          err.copy(ruleNumProcessing = RuleNumbers.ENSURE_SAMPLE_COLUMN)
        case sq: SampleTableQuery => sq.copy(ruleNumProcessing = RuleNumbers.ENSURE_SAMPLE_COLUMN)

      }
    } else {
      plan
    }
  }
}

case class PlaceHolderPlan(hiddenChild: LogicalPlan, makeVisible: Boolean,
  isDistributed: Boolean, allowOpeningOfPlaceHolderPlan: Boolean = true)
  extends SnappyUnaryNode {
  val x = 1
  override lazy val resolved = this.hiddenChild.resolved

  override def child: LogicalPlan = if (makeVisible) this.hiddenChild else null

  override def output: Seq[Attribute] = this.hiddenChild.output

  override def statistics: Statistics = hiddenChild.statistics
}

object PlaceHolderPlan {
  // Start from top to bottom
  def applyRecursivelyToInvisiblePlaceHolderPlan(root: LogicalPlan,
                                                 transformer: LogicalPlan => LogicalPlan):
  LogicalPlan = {
    val preparedRoot = root match {
      case stq: SampleTableQuery if (!stq.makeVisible) => stq.copy(makeVisible = true)
      case x => x
    }
    if (preparedRoot.resolved) {
      // The subquery nodes are invisible when in this rule, so start from top to bottom
      val newPlan = recurseDownPlaceHolder(transformer(preparedRoot), transformer)
      newPlan match {
        case stq: SampleTableQuery if (!stq.makeVisible) => stq.copy(makeVisible = true)
        case x => x
      }
    } else {
      root
    }
  }

  private def recurseDownPlaceHolder(node: LogicalPlan, transformer: LogicalPlan => LogicalPlan):
  LogicalPlan = {
    node.transformDown {
      case p@PlaceHolderPlan(hidden, _, _, _)  if (hidden.resolved) => p.copy(hiddenChild =
        recurseDownPlaceHolder(transformer(hidden), transformer) , makeVisible = false)
      case stq: SampleTableQuery if (stq.child.resolved) => val newStq = transformer(stq.copy
      (makeVisible = true)). asInstanceOf[SampleTableQuery]
      newStq.copy(child = recurseDownPlaceHolder(transformer(newStq.child), transformer),
        makeVisible = false)

    }
  }
}

object MakeSubqueryNodesVisible extends Rule[LogicalPlan] {

  def apply(root: LogicalPlan): LogicalPlan = {
    val preparedRoot = root match {
      case stq: SampleTableQuery if (!stq.makeVisible) => stq.copy(makeVisible = true)
      case x => x
    }
    preparedRoot.transformDown {
      case p: PlaceHolderPlan if !p.hiddenChild.isInstanceOf[SerializeFromObject] =>
        p.copy(makeVisible = true)
      case stq: SampleTableQuery => stq.copy(makeVisible = true)
    }
  }
}

object MakeSubqueryNodesInvisible extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan.transformUp {
      case p: PlaceHolderPlan => p.copy(makeVisible = false)
      case stq: SampleTableQuery => stq.copy(makeVisible = false)
    }
    newPlan match {
      case stq: SampleTableQuery if (!stq.makeVisible) => stq.copy(makeVisible = true)
      case x => x
    }
  }
}

object RuleNumbers {
  val REPLACE_WITH_SAMPLE_TABLE = 1
  val ERROR_ESTIMATE_RULE = 3
  val MOVE_UP_BOOTSTRAP = 4
  val GET_ERROR_BOUNDS = 5
  val ByPassErrorCalc = 6
  val ENSURE_SAMPLE_COLUMN = 7
}

case class TokenizedLiteralWrapper(child: TokenizedLiteral) extends
  UnaryExpression with Unevaluable  {
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable
  override def foldable: Boolean = true
}

case class DummyLeafNode() extends LeafNode {
  override def output: Seq[Attribute] = Seq.empty
}
