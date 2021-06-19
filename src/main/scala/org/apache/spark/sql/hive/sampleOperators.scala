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
package org.apache.spark.sql.hive

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.bootstrap._
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.common.{AQPRules, AnalysisType, SampleTablePlan}
import org.apache.spark.sql.hive.execution.HiveTableScanExec

object IdentifySampledRelation extends Rule[SparkPlan] {

  val seedName = "__seed__"

  override def apply(plan: SparkPlan): SparkPlan = SampleTablePlan.
    applyRecursivelyToInvisiblePlan(plan, applyRule)

  def applyRule(plan: SparkPlan, position: Int): SparkPlan = {
    AQPRules.getAQPInfo(plan, position) match {
      case Some(aqpInfo) =>
        if (aqpInfo.analysisType.contains(AnalysisType.Bootstrap)) {
          apply(plan, aqpInfo.isDebugFixedSeed, aqpInfo.numBSTrials,
            aqpInfo.bootstrapMultiplicities.get, aqpInfo.poissonType)
        } else {
          plan
        }
      case None => plan
    }
  }

  def apply(plan: SparkPlan, useDebugFixedSeed: Boolean,
      numBootstrapTrials: Int, bootstrapAttr: Attribute, poissonType: PoissonCreator.PoissonType
    .Value): SparkPlan
  = {
    var doneBaseTranformation = false
    plan.transformUp {
      case scan@HiveTableScanExec(_, _, _) => SampledRelation(withSeed(scan,
        useDebugFixedSeed, numBootstrapTrials, bootstrapAttr, poissonType))
      /* case scan@PhysicalRDD(output,_, _, _, _) if( !doneBaseTranformation &&
           output.exists(_.name == Utils.WEIGHTAGE_COLUMN_NAME)) => doneBaseTranformation = true
         SampledRelation(withSeed(scan, useDebugFixedSeed))
      */

      /* case scan@PhysicalRDD(output,_, _, _, _) if( !doneBaseTranformation &&
           output.exists(_.name == Utils.WEIGHTAGE_COLUMN_NAME))
           => doneBaseTranformation = true
         SampledRelation(withSeed(scan, useDebugFixedSeed))
      */
      /* TODO: hemant check with asif if removing ConvertToUnsafe is ok here
      case scan@ConvertToUnsafe(PhysicalRDD(output, _, nodeName, _, _))
       if (!doneBaseTranformation &&
       (output.exists(_.name == Utils.WEIGHTAGE_COLUMN_NAME)
        || nodeName.indexOf("ColumnFormatSamplingRelation") != -1)) => doneBaseTranformation = true
        */
      case scan: DataSourceScanExec
        if !doneBaseTranformation &&
            (scan.relation.schema.exists(_.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME))
                || scan.nodeName.indexOf("ColumnFormatSamplingRelation") != -1) =>
        doneBaseTranformation = true
        SampledRelation(withSeed(scan, useDebugFixedSeed, numBootstrapTrials, bootstrapAttr,
          poissonType))
      case x: SparkPlan if !doneBaseTranformation && x.children.exists {
        case scan: DataSourceScanExec =>
          (scan.relation.schema.exists(_.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME))
              || scan.nodeName.indexOf("ColumnFormatSamplingRelation") != -1)
        case _ => false
      } => doneBaseTranformation = true
        x.transformUp {
          case scan: DataSourceScanExec
            if (scan.relation.schema.exists(_.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME))
                || scan.nodeName.indexOf("ColumnFormatSamplingRelation") != -1) =>
            SampledRelation(withSeed(scan, useDebugFixedSeed, numBootstrapTrials,
              bootstrapAttr, poissonType))
        }

      case scan@InMemoryTableScanExec(_, _, InMemoryRelation(output, _, _, _, _, _))
        if output.exists(_.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME)) =>
        doneBaseTranformation = true
        SampledRelation(withSeed(scan, useDebugFixedSeed, numBootstrapTrials,
          bootstrapAttr, poissonType))

      case ProjectExec(prList, child) if child.output.exists(_.name.equalsIgnoreCase(
        Utils.WEIGHTAGE_COLUMN_NAME)) && !doneBaseTranformation =>
        doneBaseTranformation = true
        SampledRelation(withSeedForDataSourceAPI(prList, child,
          useDebugFixedSeed, numBootstrapTrials, bootstrapAttr, poissonType))

      /*
      case ConvertToSafe(sampled@SampledRelation(Project(projectList, child))) =>
        SampledRelation(Project(projectList, ConvertToSafe(child)))
      */
    }
  }

  private[this] def withSeedForDataSourceAPI(prList: Seq[NamedExpression], child: SparkPlan,
      useDebugFixedSeed: Boolean, numBootstrapTrials: Int, bootstrapAttr: Attribute,
                                             poissonType: PoissonCreator.PoissonType.Value) = {
    ProjectExec(prList :+ TaggedAlias(Bootstrap,
      poissonType match {
        case PoissonCreator.PoissonType.Real => SnappyPoissonSeed(useDebugFixedSeed)
        case PoissonCreator.PoissonType.DbgIndpPredictable => DebugFixedSeed()
      }
     , seedName)(bootstrapAttr.exprId), child)
  }

  private[this] def withSeed(plan: SparkPlan, useDebugFixedSeed: Boolean, numBootstrapTrials: Int,
      bootstrapAttr: Attribute, poissonType: PoissonCreator.PoissonType.Value) =
    ProjectExec(plan.output :+ TaggedAlias(Bootstrap, poissonType match {
      case PoissonCreator.PoissonType.Real => SnappyPoissonSeed(useDebugFixedSeed)
      case PoissonCreator.PoissonType.DbgIndpPredictable => DebugFixedSeed()
    }, seedName)(bootstrapAttr.exprId), plan)

}

/*
object DebugFixedSeedIdentifySampledRelation
    extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = SampleTablePlan.
    applyRecursivelyToInvisiblePlan(plan, applyRule)

  def applyRule(plan: SparkPlan, position: Int): SparkPlan = {
    AQPRules.getAQPInfo(plan, position) match {
      case Some(aqpInfo) => if (aqpInfo.analysisType.get == AnalysisType.Bootstrap) {
        if (aqpInfo.isDebugFixedSeed) {
          IdentifySampledRelation.apply(plan, useDebugFixedSeed = true,
            aqpInfo.numBSTrials, aqpInfo.bootstrapMultiplicities.get)
        } else {
          plan
        }
      } else {
        plan
      }
      case None => plan
    }
  }

}
*/

/*
object AddScaleFactor extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case project@Project(projectList, child) => {
      val scaleAttrib = BootStrapUtils.getScaleAttribute(child)
      Project(projectList :+ AttributeReference(scaleAttrib.name,scaleAttrib.dataType,
        false,scaleAttrib.metadata)(scaleAttrib.exprId,scaleAttrib.qualifiers), child)

    }
    case aggregate@SortBasedAggregate(requiredChildDistributionExpressions,
    groupingExpressions,
    nonCompleteAggregateExpressions,
    nonCompleteAggregateAttributes,
    completeAggregateExpressions,
    completeAggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child: SparkPlan) if(nonCompleteAggregateExpressions.exists(_.mode match {
      case Partial => true
      case PartialMerge => true
      case Final => false
      case Complete => false
    } )) => {
      val transformer = (aggExp: AggregateExpression2) => {
        aggExp.aggregateFunction match {
          case org.apache.spark.sql.catalyst.expressions.aggregate.Sum(expression) =>
            val lhs = BootStrapUtils.getScaleAttribute(child)
           // WeightedSum(Multiply(args, mapExpr))
            val (l,r) = OnlinePlannerUtil.widenTypes(lhs,expression)
            val newExp = Multiply(r,l)
            val newFunc = org.apache.spark.sql.catalyst.expressions.aggregate.Sum(newExp)
            AggregateExpression2(newFunc, aggExp.mode, aggExp.isDistinct)
          case _ => aggExp
        }
      }
      val newNonCompleteAggExp = nonCompleteAggregateExpressions.map (transformer)
      val newCompleteAggExp = completeAggregateExpressions.map(transformer)
      var i = 0
      val newResultsExp = resultExpressions.map{named =>
        val ar =   newNonCompleteAggExp(i).aggregateFunction.cloneBufferAttributes(0)
        val newNamed = if( named.exprId ==  ar.exprId) {
          named
        }else {
          ar.withName(ar.name)
        }
        i = i +1
        newNamed
      }
      SortBasedAggregate(requiredChildDistributionExpressions,groupingExpressions,
        newNonCompleteAggExp, nonCompleteAggregateAttributes, completeAggregateExpressions,
      completeAggregateAttributes, initialInputBufferOffset, newResultsExp, child)
    }
  }
}
*/

case class SampledRelation(child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  /** TODO: Hemant these are no more part of unaryExecNode. are these required
   * override def outputsUnsafeRows: Boolean = child.outputsUnsafeRows
   * *
   * override def canProcessUnsafeRows: Boolean = true
   * *
   * override def canProcessSafeRows: Boolean = true */

  override def doExecute(): RDD[InternalRow] = child.execute()
}
