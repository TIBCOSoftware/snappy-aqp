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
package org.apache.spark.sql.internal

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

import io.snappydata.Property

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.bootstrap._
import org.apache.spark.sql.execution.command.{CreateDataSourceTableCommand, CreateSampleTableCommand, ExecutedCommandExec}
import org.apache.spark.sql.execution.common._
import org.apache.spark.sql.execution.datasources.{AnalyzeCreateTable, CreateTable, DataSourceAnalysis, FindDataSourceTable, ResolveDataSource}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.hive.{HiveConditionalRule, IdentifySampledRelation, OptimizeSortAndFilePlans, SnappyAQPSessionCatalog, SnappyAnalyzer, SnappySessionState}
import org.apache.spark.sql.sampling.{ColumnFormatSamplingRelation, QcsLogicalPlan, QcsSparkPlan}
import org.apache.spark.sql.sources.{ResolveQueryHints, SampleTableQuery}

final class SnappyAQPSessionState(_snappySession: SnappySession)
    extends SnappySessionState(_snappySession) {

  self =>

  override lazy val catalog: SnappyAQPSessionCatalog = {
    new SnappyAQPSessionCatalog(
      snappySession.sharedState.getExternalCatalogInstance(snappySession),
      snappySession,
      snappySession.sharedState.globalTempViewManager,
      functionResourceLoader,
      functionRegistry,
      conf,
      newHadoopConf())
  }

  override val sampleSnappyCase: PartialFunction[LogicalPlan, Seq[SparkPlan]] = {
    case s@StratifiedSample(_, child, _) =>
      s.getExecution(PlanLater(child)) :: Nil
    case SampleTableQuery(_, child, error, confidence,
    sampleRatio, bootstrapMultiplicities, _, _, analysisType, bsAliasID,
    numBSTrials, behavior, _, _, _, _, logicalPlanForQueryRouting) =>
      val numAggregates = child.find {
        case _: Aggregate => true
        case _ => false
      }.map(_.asInstanceOf[Aggregate].aggregateExpressions.count { ne =>
        ne.find {
          case _: ErrorAggregateFunction => true
          case _ => false
        }.exists(_ => true)
      }).getOrElse(0)

      val isAQPDebug = Property.AqpDebug.get(this.conf)
      val isDebugFixedSeed = Property.AqpDebugFixedSeed.get(this.conf)

      val poissonType = if (analysisType.map(_ == AnalysisType.Bootstrap).getOrElse(false)) {
        val opt = Property.AQPDebugPoissonType.getOption(this
          .conf)
        val poissonStr = opt.getOrElse(PoissonCreator.PoissonType.Real.toString)
        PoissonCreator.PoissonType.withName( poissonStr )
      } else {
        null
      }
      this.contextFunctions.currentPlanAnalysis = analysisType
      this.contextFunctions.aqpInfo += (AQPInfo(error, confidence, sampleRatio,
        analysisType, isAQPDebug, isDebugFixedSeed, poissonType,
        bootstrapMultiplicities, bsAliasID, numBSTrials, behavior, numAggregates))

      SampleTablePlan(error, confidence, sampleRatio,
        analysisType, bootstrapMultiplicities, PlanLater(child), bsAliasID,
        numBSTrials, behavior, numAggregates, TODO_UNUSED = false, None,
        logicalPlanHasSort = QueryRoutingRules.getPlanHasSort(child), None,
        logicalPlanForQueryRouting, makeVisible = true) :: Nil
    case BootstrapReferencer(child, _, _) => PlanLater(child) :: Nil
    case x: QcsLogicalPlan => QcsSparkPlan(x.output) :: Nil
    case MarkerForCreateTableAsSelect(child) => PlanLater(child) :: Nil
    case BypassRowLevelSecurity(child) => PlanLater(child) :: Nil
    case CreateTable(tableDesc, mode, None) if tableDesc.provider.
      map(_.equalsIgnoreCase(SnappyContext.SAMPLE_SOURCE)).getOrElse(false) =>
      val cmd = CreateSampleTableCommand(tableDesc, mode == SaveMode.Ignore)
      ExecutedCommandExec(cmd) :: Nil
    case _ => Nil
  }

  @transient
  override val contextFunctions: SnappyContextAQPFunctions =
    new SnappyContextAQPFunctions

  override lazy val analyzer: Analyzer =
    new AQPQueryAnalyzer(this) {

      // we pass wrapper catalog to make sure LogicalRelation
      // is passed in PreWriteCheck
      override val extendedCheckRules = Seq(
        ConditionalPreWriteCheck(datasources.PreWriteCheck(conf, wrapperCatalog)), PrePutCheck)

      private val replaceWithSampleTableRule = ReplaceWithSampleTable(
        catalog, this, numBootstrapTrials, isAQPDebug,
        isDebugFixedSeed, closedFormEstimates, conf)

      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        MakeSubqueryNodesVisible ::
            ResolveErrorEstimateFunctions ::
            new HiveConditionalRule(_.catalog.ParquetConversions, self) ::
            new HiveConditionalRule(_.catalog.OrcConversions, self) ::
            AnalyzeCreateTable(snappySession) ::
            new PreprocessTable(self) ::
            QueryRoutingRules(this) ::
            replaceWithSampleTableRule ::
            PostReplaceSampleTableQueryRoutingRules(this) ::
            MakeSubqueryNodesVisible ::
            ResolveAliasInGroupBy ::
            new FindDataSourceTable(snappySession) ::
            DataSourceAnalysis(conf) ::
            AnalyzeMutableOperations(snappySession, this) ::
            ResolveQueryHints(snappySession) ::
            RowLevelSecurity ::
            ExternalRelationLimitFetch ::
            WeightageRule(this) ::
            ErrorEstimateRule(this) ::
            MoveUpBootstrapReferencerConditionally(this) ::
            GetErrorBounds(this) ::
            ByPassErrorCalculationsConditionally(this, Property.AqpDebug.get(conf)) ::
            EnsureSampleWeightageColumn(this) ::
            MakeSubqueryNodesVisible ::
            (if (conf.runSQLonFile) new ResolveDataSource(snappySession) :: Nil else Nil)
    }


  override protected[sql] def queryPreparations(
      topLevel: Boolean): Seq[Rule[SparkPlan]] = Seq[Rule[SparkPlan]](
    python.ExtractPythonUDFs,
    TokenizeSubqueries(snappySession),
    EnsureRequirements(conf),
    OptimizeSortAndFilePlans(snappySession),
    CollapseCollocatedPlans(snappySession),
  //  CollectAnalysisInfo,
    HideSubqueryNodes,
    IdentifySampledRelation,
    PushUpSeed,
    PropagateBootstrapColumns,
    ImplementSnappyAggregate,
    PruneProjects,
    CleanupErrorEstimateAttribute,
    CleanupBootstrapAnalysisExpressions,
    UnHideSubqueryNodes,
    CollapseCodegenStages(conf),
    InsertCachedPlanFallback(snappySession, topLevel).asInstanceOf[Rule[SparkPlan]],
    ReuseExchange(conf))

  /*
  override protected def newQueryExecution(plan: LogicalPlan): QueryExecution = {
    new QueryExecution(snappySession, plan) {
      override protected def preparations: Seq[Rule[SparkPlan]] = queryPreparations

      val batches = Seq(
        Batch("Add exchange", Once, EnsureRequirements(session.sessionState.conf)),
        Batch("Collect information on type of analysis", Once, CollectAnalysisInfo),
        Batch("Identify Sampled Relations for bootstrap debug debug", Once,
          DebugFixedSeedIdentifySampledRelation
        ),
        Batch("Identify Sampled Relations", Once,
          IdentifySampledRelation),

        Batch("Bootstrap", Once,
          // PushUpResample,
          PushUpSeed,
          // ImplementResample,
          // DebugPoissonImplementResample,
          // BootstrapColumnGenerator,
          PropagateBootstrapColumns
        ),

        Batch("Change Final Aggregate to SnappyAggregate", Once, ImplementSnappyAggregate),
        Batch("Prune Projects", Once, PruneProjects),
        Batch("Convert ErrorEstimateAttribute to real attributes", Once,
          CleanupErrorEstimateAttribute),
        Batch("Cleanup Bootstrap Columns", Once, bootstrap.CleanupBootstrapAnalysisExpressions)
        // Batch("Remove bootstrap multiplicity columns", Once, RemoveBootstrapMultiplicity)
      )

      // TODO: Comment out this function if assertion is not needed
      override def execute(executePlan: org.apache.spark.sql.execution.SparkPlan):
      org.apache.spark.sql.execution.SparkPlan = {
        val topPlan = super.execute(executePlan)
        if (AQPRules.isClosedForm(topPlan) || AQPRules.isBootStrapAnalysis(topPlan)) {
          assert(topPlan match {
            case _: SampleTablePlan => true
            case x: GlobalLimitExec => x.child match {
              case _: SampleTablePlan => true
              case _ => false
            }
            case _ => false
          }, topPlan.getClass.getSimpleName)
        }
        topPlan
      }
    }
  }
  */


  private val closedFormEstimates = Property.ClosedFormEstimates.get(conf)
  private val numBootstrapTrials = Property.NumBootStrapTrials.get(conf)
  val isAQPDebug: Boolean = Property.AqpDebug.get(conf)
  val isDebugFixedSeed: Boolean = Property.AqpDebugFixedSeed.get(conf)

  ClusterUtils.checkClusterRestrictions(snappySession.sparkContext)
}

trait AnalyzerInvocation {

  protected val executeInvocationCount: ThreadLocal[Int] = new ThreadLocal[Int]() {
    override protected def initialValue: Int = 0
  }

  def getCallStackCount: Int = {
    this.executeInvocationCount.get - 1
  }

  def sqlConf: SQLConf
}

class AQPQueryAnalyzer(sessionState: SnappyAQPSessionState)
    extends SnappyAnalyzer(sessionState) with AnalyzerInvocation {

  override def execute(logical: LogicalPlan): LogicalPlan = {
    val invocationCount = executeInvocationCount.get

    if (invocationCount == 0) {
      ReplaceWithSampleTable.reset()
    }
    val temp1 = invocationCount + 1
    executeInvocationCount.set(temp1)
    try {
      Try(super.execute(logical)) match {
        case Success(plan) =>
          val temp2 = executeInvocationCount.get - 1

          executeInvocationCount.set(temp2)
          if (temp2 == 0) {
            SnappyQueryExecution.processPlan(plan, sessionState, logical)
          } else {
            plan
          }
        case Failure(f) =>
          val temp = executeInvocationCount.get - 1
          executeInvocationCount.set(temp)
          throw f
      }
    } finally {
      if (executeInvocationCount.get == 0) {
        sessionState.contextFunctions.setQueryExecutor(None)
      }
    }
  }

  object ResolveErrorEstimateFunctions extends Rule[LogicalPlan] {

    val functionTransformer: ErrorEstimateFunction =>
        org.apache.spark.sql.catalyst.expressions.Expression =
      (func: ErrorEstimateFunction) => {
        val newChildren = func.children.map {
          _.transformUp {
            case att: Attribute => Literal(att.name)
          }
        }
        func.makeCopy(newChildren.toArray)
      }

    def apply(plan: LogicalPlan): LogicalPlan =
      if (getCallStackCount > 1) {
        plan
      } else {
        plan resolveOperators {

          case q: LogicalPlan =>
            q transformExpressionsUp {
              case u@UnresolvedFunction(funcId, children, _) =>
                withPosition(u) {
                  sessionState.catalog.lookupFunction(
                    funcId, children) match {
                    case x: ErrorEstimateFunction => functionTransformer(x)
                    case _ => u
                  }
                }
              case UnresolvedAlias(child: ErrorEstimateFunction, _) =>

                /* TODO: hemant:  resolve alias has changed when the alias function is given
                  * Does this code need an update? */
                Alias(functionTransformer(child),
                  child.prettyName + "(" + child.getParamName + ")")()

              case al@Alias(func: ErrorEstimateFunction, name) =>
                val newChild = functionTransformer(func)
                al.copy(child = newChild, name = name)(exprId = al.exprId,
                  qualifier = al.qualifier,
                  explicitMetadata = al.explicitMetadata,
                  isGenerated = al.isGenerated)
            }
        }
      }
  }

  def sqlConf: SQLConf = sessionState.conf
}
