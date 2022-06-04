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

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer

import io.snappydata.{Constant, Property}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{InSet, Literal, NamedExpression, TokenizedLiteral}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.bootstrap.BootstrapMultiplicityAggregate
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.{AQPQueryAnalyzer, AnalyzerInvocation, SQLConf}
import org.apache.spark.sql.sampling.ColumnFormatSamplingRelation
import org.apache.spark.sql.types.StringType
import scala.util.Try


// TODO: Handle case of multiple sample table queries in a subquery system
@transient
case class QueryRoutingRules(analyzerInvoc: AnalyzerInvocation)
    extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = if (analyzerInvoc.getCallStackCount > 0
      || !plan.resolved) {
    plan
  } else {
    if (QueryRoutingRules.getBehaviorForcedRoute(plan, analyzerInvoc.sqlConf) ||
        nonSupportedExpressions(plan)) {
      ReplaceWithSampleTable.markQueryRoutingApplicable()
    }
    plan
  }

  protected def nonSupportedAggregates(agg: NamedExpression): Boolean = {
    agg.collect {
      case agg: AggregateExpression =>
        agg.aggregateFunction match {
          case sum: Sum => None
          case avg: Average => None
          case error: ErrorAggregateFunction => None
          case bootAgg: BootstrapMultiplicityAggregate => None
          case _: Max => None
          case _: Min => None
          case count: Count => if (agg.isDistinct) {
            Option(count)
          } else {
            val ifNodeExists = count.find {
              case _: org.apache.spark.sql.catalyst.expressions.If => true
              case _ => false
            }.isDefined
            if (ifNodeExists) {
              Option(count)
            } else {
              None
            }
          }
          case othersNonSupported => Option(othersNonSupported)
        }
    }.exists {
      case Some(_) => true
      case None => false
    }
  }

  protected def nonSupportedExpressions(plan: LogicalPlan): Boolean = {
    val allAggregates = plan.collect {
      case x: org.apache.spark.sql.catalyst.plans.logical.Aggregate => x
    }
    allAggregates.isEmpty ||
        allAggregates.exists(_.aggregateExpressions.exists(nonSupportedAggregates))
  }

  // TODO: AQP-238
  def notSupportingHavingForPartialRouting(plan: LogicalPlan) : Boolean = {
    analyzerInvoc match {
      case x: AQPQueryAnalyzer =>
        QueryRoutingRules.getBehaviorPartialRouting(plan, x.sqlConf) &&
        QueryRoutingRules.hasHavingClause(plan)
      case _ => false
    }
  }
}

object QueryRoutingRules extends Logging {
  val criteria: PartialFunction[LogicalPlan, Boolean] = {
    case (_: Error | _: Confidence | _: Behavior | _: ErrorDefaults) => true
    case PlaceHolderPlan(hidden, _, _, _) => hidden.find(criteria).map(_ => true).getOrElse(false)
    case _ => false
  }
  val transformer: PartialFunction[LogicalPlan, LogicalPlan] = {
    case Error(_, child) => child
    case Confidence(_, child) => child
    case Behavior(_, child, _) => child
    case ErrorDefaults(child) => child
    case p@PlaceHolderPlan(hidden, _, _, _) => p.copy(hiddenChild = hidden.transformUp(transformer))

  }

  def removeErrorAndConfidence(plan: LogicalPlan): (Boolean, LogicalPlan) = {
    plan find {
      criteria
    } match {
      case Some(_) => (true, plan.transformUp {
        transformer
      })
      case None => (false, plan)
    }
  }

  // TODO: Should we check for sample table? Its must be costly to lookup in catalog.
  // TODO: Handle case of multiple sample table queries
  def shouldRouteToBaseTable(plan: LogicalPlan, catalog: SessionCatalog,
                             conf: SQLConf): Boolean = {
    var isError = false
    var gotSampledTable = false
    var isForcedRoute = false
    var gotBehavior = None: Option[HAC.Type]
    if (conf.contains(Property.Error.name)) {
      val (error, _, _) = ReplaceWithSampleTable.getErrorDefaults(conf)
      if (error  != -1d) {
        isError = true
      }
    }
    plan foreach {
      case x@Behavior(expr, _, forcedRoute) =>
        isForcedRoute = forcedRoute
        gotBehavior = Some(HAC.getBehavior(expr))
      case x@(Error(_, _) | ErrorDefaults(_)) => isError = true
      case u: UnresolvedRelation =>
        Try {
          gotSampledTable = gotSampledTable ||
            (catalog.lookupRelation(u.tableIdentifier, u.alias) find {
              case _: LogicalRelation => true
              case _ => false
            } match {
              case Some(LogicalRelation(r: ColumnFormatSamplingRelation,
              _, _)) => true
              case _ => false
            })
        }.getOrElse(gotSampledTable)
      case _ =>
    }

    val isRouteToBase = !gotSampledTable && (gotBehavior match {
      case Some(b) => !isForcedRoute && (b == HAC.REROUTE_TO_BASE || b == HAC.PARTIAL_ROUTING)
      case None => isError && (HAC.getDefaultBehavior(conf) == HAC.REROUTE_TO_BASE ||
          HAC.getDefaultBehavior(conf) == HAC.PARTIAL_ROUTING)
    })


    if (isRouteToBase) {
      // TODO: Vivek, Remove this
      logInfo(s"Plan with HAC reroute: \n" + plan)
    }

    isRouteToBase
  }


  def getPlanHasSort(plan: LogicalPlan): Boolean = {
    plan find {
      case x: Sort => true
      case _ => false
    } match {
      case Some(_) => true
      case None => false
    }
  }


  def getBehaviorForcedRoute(plan: LogicalPlan, conf: SQLConf): Boolean = {
    var isForcedRoute = false
    plan find {
      case x@Behavior(expr, _, forcedRoute) =>
        val behavior = HAC.getBehavior(expr)
        if (behavior == HAC.REROUTE_TO_BASE || behavior == HAC.PARTIAL_ROUTING) {
          isForcedRoute = forcedRoute
        }
        true
      case _ => false
    }

    isForcedRoute
  }

  def setBehaviorForcedRoute(plan: LogicalPlan, conf: SQLConf): LogicalPlan = {
    var planTransformed: Boolean = false
    val newPlan = plan.transformDown {
      case x@Behavior(expr, _, _) =>
        val behavior = HAC.getBehavior(expr)
        // Failure means wrong query has been selected for reroute
        assert(behavior == HAC.REROUTE_TO_BASE || behavior == HAC.PARTIAL_ROUTING, behavior)
        planTransformed = true
        x.copy(forcedRoute = true)
    }

    if (planTransformed) {
      // TODO: Vivek, Remove this
      logInfo(s"setBehaviorForcedRoute: Updated Behavior Node Plan: \n" + newPlan)
      newPlan
    } else {
      val behaviorString = if (conf.contains(Property.Error.name) ) {
        val (error, _, behavior) = ReplaceWithSampleTable.getErrorDefaults(conf)
        val isForcedRoute = error != -1d && (behavior == HAC.REROUTE_TO_BASE
          || behavior == HAC.PARTIAL_ROUTING)
        if (isForcedRoute) {
          HAC.getBehaviorAsString(behavior)
        } else {
          Constant.DEFAULT_BEHAVIOR
        }
      } else {
        Constant.DEFAULT_BEHAVIOR
      }
      val newNodeAtTop = Behavior(Literal.create(behaviorString, StringType),
        plan, forcedRoute = true)
      // TODO: Vivek, Remove this
      logInfo(s"setBehaviorForcedRoute: Updated Behavior Node Plan: \n" + newNodeAtTop)
      newNodeAtTop
    }
  }

  def setPartialRoutePlan(plan: LogicalPlan, conf: SQLConf,
                          params: Array[Set[Any]]): Option[LogicalPlan] = {
    var stopPlanChange = false
    var behaviorNodeFound = false


    val newPlan = plan.transformDown {
      case x@Behavior(expr, _, forcedRoute) if !stopPlanChange || forcedRoute =>
        val behavior = HAC.getBehavior(expr)
        // Failure means wrong query has been selected for reroute
        assert(behavior == HAC.PARTIAL_ROUTING, behavior)
        behaviorNodeFound = true
        x.copy(forcedRoute = true)

      case x@ErrorDefaults(child) =>
        if (HAC.getDefaultBehavior(conf) == HAC.PARTIAL_ROUTING && !behaviorNodeFound) {
          behaviorNodeFound = true
          Behavior(Literal.create(HAC.getBehaviorAsString(HAC.PARTIAL_ROUTING), StringType),
            child, forcedRoute = true)
        } else {
          x
        }

      case x@Error(_, _) =>
        if (HAC.getDefaultBehavior(conf) == HAC.PARTIAL_ROUTING && !behaviorNodeFound) {
          behaviorNodeFound = true
          Behavior(Literal.create(HAC.getBehaviorAsString(HAC.PARTIAL_ROUTING), StringType),
            plan, forcedRoute = true)
        } else {
          x
        }

      case x@Aggregate(groupingExpressions, aggregateExpressions, child) if !stopPlanChange =>
        assert(params.isEmpty || params.size == groupingExpressions.size, "groupingExpressions" +
          ".size=" +
            groupingExpressions.size + " params.size=" + params.size)

        if (!params.isEmpty) {

         /*
          assert(selectedParams.size == selectedGroups.size,
            "selected groupingExpressions.size=" + selectedGroups.size +
                " selected params.size=" + selectedParams.size)
                */
          var newChild = Filter(InSet(groupingExpressions.head, params.head), child)
          (1 until groupingExpressions.size).foreach(i =>
            newChild = Filter(InSet(groupingExpressions(i), params(i)), newChild))
          x.copy(child = newChild)
        } else {
          stopPlanChange = true
          x
        }
    }

   val finalPlan = if (!behaviorNodeFound && conf.contains(Property.Error.name) &&
      HAC.getDefaultBehavior(conf) == HAC.PARTIAL_ROUTING) {
      Behavior(Literal.create(HAC.getBehaviorAsString(HAC.PARTIAL_ROUTING), StringType),
        newPlan, forcedRoute = true)
    } else {
     newPlan
   }

    // TODO: Vivek, Remove this
    logInfo(s"setPartialRoutePlan: Updated Behavior Node Plan: \n" +
        (if (stopPlanChange) "None" else finalPlan))

    if (stopPlanChange) None else Some(finalPlan)
  }

  def hasHavingClause(plan: LogicalPlan): Boolean = {
    plan find {
      case x@Filter(havingCondition, aggregate@Aggregate(_, _, _)) => true
      case _ => false
    } match {
      case Some(_) => true
      case None => false
    }
  }

  def getBehaviorPartialRouting(plan: LogicalPlan, conf: SQLConf): Boolean = {
    var behavior : Option[HAC.Type] = None
    plan find {
      case x@Behavior(expr, _, _) =>
        behavior = Some(HAC.getBehavior(expr))
        true
      case _ => false
    }

    if (behavior.isDefined) {
      behavior.get == HAC.PARTIAL_ROUTING
    } else {
      HAC.getDefaultBehavior(conf) == HAC.PARTIAL_ROUTING
    }
  }

}


@transient
case class PostReplaceSampleTableQueryRoutingRules(
  analyzerInvoc: AnalyzerInvocation) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan =
    if (plan.resolved &&
        analyzerInvoc.getCallStackCount == 0
    // && !ReplaceWithSampleTable.isQueryRoutingApplicable
    ) {
      val (planTransformed1, newPlan) = QueryRoutingRules.removeErrorAndConfidence(plan)
      if (planTransformed1) {
        logInfo(
          s"""Applied on
              | Old-Plan: $plan
              | New-Plan: """.stripMargin + newPlan)
      }

      val (planTransformed2, newPlan1) = replaceNonSupportedErrorEstimates(newPlan)
      if (planTransformed1 || planTransformed2) {
        newPlan1
      } else {
        plan
      }

    } else {
      plan
    }


  protected def replaceNonSupportedErrorEstimate(aggExp:
    org.apache.spark.sql.catalyst.expressions.Expression): Boolean = {
    aggExp.collect {
      case ae: AbsoluteError => ae.setDoEval(true)
      case re: RelativeError => re.setDoEval(true)
      case lb: LowerBound => lb.setDoEval(true)
      case ub: UpperBound => ub.setDoEval(true)
    }.nonEmpty
  }

  protected def replaceNonSupportedErrorEstimates(plan: LogicalPlan): (Boolean, LogicalPlan) = {
    plan find {
      case _: org.apache.spark.sql.catalyst.plans.logical.Aggregate => true
      case _ => false
    } match {
      case Some(_) => {
        var foundErrorEst = false
        val modPlan = plan.transformDown {
          case x: org.apache.spark.sql.catalyst.plans.logical.Aggregate =>
            x.aggregateExpressions.foreach(y => {
              val ests = replaceNonSupportedErrorEstimate(y)
              foundErrorEst = foundErrorEst || ests
            }
            )
            x
          case x: org.apache.spark.sql.catalyst.plans.logical.Filter =>
            val ests = replaceNonSupportedErrorEstimate(x.condition)
            foundErrorEst = foundErrorEst || ests
            x
        }
        (foundErrorEst, modPlan)
      }
      case None => (false, plan)
    }
  }
}
