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
package org.apache.spark.sql.execution.bootstrap


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{SnappyHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.common.{SnappyHashAggregate, SnappyPartialHashAggregate, SnappyPartialSortedAggregate, SnappySortAggregate}
import org.apache.spark.sql.execution.common.{AQPRules, SampleTablePlan}
import org.apache.spark.sql.hive.SampledRelation
import org.apache.spark.sql.types._



case class ResamplePlaceholder(child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def doExecute(): RDD[InternalRow] =
    throw new TreeNodeException(this,
      s"No function to execute plan. type: ${this.nodeName}")
}



/**
  * Postpone materializing seed.
  */
object PushUpSeed extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = SampleTablePlan.applyRecursivelyToInvisiblePlan(
    plan, applyRule)


  def applyRule(plan: SparkPlan, position: Int): SparkPlan =
    if (AQPRules.isBootStrapAnalysis(plan, position)) {
      plan.transformUp {

        case sr@SampledRelation(ProjectExec(projectList, child)) =>
          val (seeds, others) = projectList.partition {
            case TaggedAlias(Bootstrap, _, _) => true
            case TaggedAttribute(Bootstrap, _, _, _, _) => true
            case _ => false
          }
          val sr2 = SampledRelation(ProjectExec(others, child))
          ProjectExec(sr2.output ++ seeds, sr2)
        case se@StratifiedSampleExecute(ProjectExec(projectList, child), output, options, qcs) =>
          val (seeds, others) = projectList.partition {
            case TaggedAlias(Bootstrap, _, _) => true
            case TaggedAttribute(Bootstrap, _, _, _, _) => true
            case _ => false
          }
          val se2 = StratifiedSampleExecute(ProjectExec(others, child), output, options, qcs)
          ProjectExec(se2.output ++ seeds, se2)

        case filter@FilterExec(condition, ProjectExec(projectList, child)) =>
          val (seeds, others) = projectList.partition {
            case TaggedAlias(Bootstrap, _, _) => true
            case TaggedAttribute(Bootstrap, _, _, _, _) => true
            case _ => false
          }
          if (AttributeSet(seeds.map(_.toAttribute)).intersect(condition.references).isEmpty) {
            val filter2 = FilterExec(condition, ProjectExec(others, child))
            ProjectExec(filter2.output ++ seeds, filter2)
          } else {
            filter
          }

      }
    } else {
      plan
    }
}





object PropagateBootstrapColumns extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = SampleTablePlan.
    applyRecursivelyToInvisiblePlan(plan, applyRule)

  def applyRule(plan: SparkPlan, position: Int): SparkPlan = if (AQPRules.isBootStrapAnalysis(plan,
    position)) {

    val aqpInfo = AQPRules.getAQPInfo(plan, position).get
    val bootstrapCol = aqpInfo.bootstrapMultiplicities.get

    var checkState = 0 // look for begin condition
    // 1 continue checking, 2 end

    plan.transformUp {

      case y@(SortAggregateExec(_, _, _, _, _, _, _) |
              SnappyHashAggregateExec(_, _, _, _, _, _, _)) =>
        if (y.output.exists(_.name == BootstrapMultiplicity.aggregateName)) {
          checkState = 2
        }
        y

      case x@ProjectExec(projectList, child) if checkState == 0 =>
        BootStrapUtils.getBtCnt(projectList.map(_.toAttribute)) match {
          case Some(_) => checkState = 1
          case None =>
        }
        x

      case x@ProjectExec(projectList, child) if checkState == 1 &&
          child.output.exists( _.exprId == bootstrapCol.exprId) =>
        if (projectList.exists(_.exprId == bootstrapCol.exprId)) {
          x
        } else {
          x.copy(projectList = projectList :+ bootstrapCol)
        }

      /*
      case TakeOrderedAndProject(limit, sortOrder, projectListOption, child) =>
        val x = flattenNamed(projectListOption.get)
        TakeOrderedAndProject(limit, sortOrder, Some(x), child)

      case CollectPlaceholder(projectList, child) =>
        CollectPlaceholder(flattenNamed(projectList), child)
      */
    }
  } else {
    plan
  }
}

case class ApproxColumnExtractor(child: Expression, name: String,
    ordinal: Int, dataType: DataType, override val nullable: Boolean)(
    override val exprId: ExprId = NamedExpression.newExprId,
    override val qualifier: Option[String] = None)
    extends UnaryExpression with NamedExpression  {


  override lazy val resolved: Boolean = true


  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("not implemented")


  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {

      val childEval = child.genCode(ctx)

       val code =
        s"""
        ${childEval.code}
        double ${ev.value} = 0d;
        boolean ${ev.isNull} =  ((InternalRow) ${childEval.value}).isNullAt($ordinal);
        if (!${ev.isNull}) {
          ${ev.value}= ((InternalRow) ${childEval.value}).getDouble($ordinal);
        }
      """
      ev.copy(code = code)

  }

  override def metadata: Metadata = Metadata.empty

  override def toAttribute: Attribute = {
    if (resolved) {
      AttributeReference(name, dataType, nullable, metadata)(exprId,
        qualifier)
    } else {
      UnresolvedAttribute(name)
    }
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
  override def newInstance(): NamedExpression = ApproxColumnExtractor(
    child, name, ordinal, dataType, nullable)(qualifier = qualifier)
}

object CleanupBootstrapAnalysisExpressions extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    val retPlan = SampleTablePlan.applyRecursivelyToInvisiblePlan(
      plan, applyRule)
    AQPRules.clearAQPInfo(plan.sqlContext)
    retPlan
  }

  def applyRule(plan: SparkPlan, position: Int): SparkPlan =
    if (AQPRules.isBootStrapAnalysis(plan, position)) {
      val retVal = plan.transformUp {
        case q: SparkPlan => q.transformExpressionsUp {
            case attr: TaggedAttribute => attr.toAttributeReference
            case alias: TaggedAlias => alias.toAlias
        }
      }
      // SnappyQueryExecution.clearTungstenDisabledFlag()
      retVal
    } else {
      plan
    }
}

object PruneProjects extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = SampleTablePlan.applyRecursivelyToInvisiblePlan(
    plan, applyRule)

  def applyRule(plan: SparkPlan, position: Int): SparkPlan = if (AQPRules.isBootStrapAnalysis(plan,
    position)) {
    plan transformUp {
      case p@ProjectExec(projectList1, ProjectExec(projectList2, child)) =>
        // Create a map of Aliases to their values from the child projection.
        // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
        val aliasMap = AttributeMap[NamedExpression](projectList2.collect {
          case a: Alias => (a.toAttribute, a)
          case a: TaggedAlias => (a.toAttribute, a)
        })

        // We only collapse these two Projects if their overlapped expressions are all
        // deterministic.
        val hasNondeterministic = projectList1.exists(_.collect {
          case a: Attribute if aliasMap.contains(a) => aliasMap(a) match {
            case al: Alias => al.child
            case tal: TaggedAlias => tal.child
          }
        }.exists(!_.deterministic))

        if (hasNondeterministic) {
          p
        } else {
          val substitutedProjection = projectList1.map(_.transform {
            case a: Attribute => aliasMap.getOrElse(a, a)
          }).asInstanceOf[Seq[NamedExpression]]
          // collapse 2 projects may introduce unnecessary Aliases, trim them here.
          val cleanedProjection = substitutedProjection.map(p =>
            CleanupAliases.trimNonTopLevelAliases(p).asInstanceOf[NamedExpression]
          )
          ProjectExec(cleanedProjection, child)
        }
    }
  } else {
    plan
  }
}
