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
package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalysts.plans.logical.SnappyUnaryNode
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.bootstrap.BootstrapMultiplicityAggregate
import org.apache.spark.sql.execution.columnar.impl.BaseColumnFormatRelation
import org.apache.spark.sql.execution.common.{AnalysisType, ErrorAggregateFunction, HAC, MapColumnToWeight}
import org.apache.spark.sql.types.StructType

case class SampleTableQuery(schemaFromExecutedPlan: Option[QueryExecution],
    child: LogicalPlan, error: Double, confidence: Double,
    sampleRatioAttribute: Masquerader, bootstrapMultiplicities: Option[Attribute],
    isAQPDebug: Boolean, isFixedSeed: Boolean,
    analysisType: Option[AnalysisType.Type], bsMultiplictyAliasID: ExprId,
    numBSTrials: Int, behavior: HAC.Type, sampleTable: String, ruleNumProcessing: Int = -1,
    val makeVisible: Boolean, isDistributed: Boolean,
   logicalPlanForQueryRouting: Option[LogicalPlan])
    extends SnappyUnaryNode {


  override def children: Seq[LogicalPlan] = if (makeVisible) super.children else Nil

  /*
  override def output: Seq[Attribute] = {
    schemaFromExecutedPlan match {
      case Some(x) => child.output.filter(_.exprId != bsMultiplictyAliasID)
      case None => errorEstimates match {
        case Some(errorEst) => mixErrorEstimatesInOutput(child.output.filter(
          _.exprId != bsMultiplictyAliasID), errorEst)

        case None => child.output.filter(_.exprId != bsMultiplictyAliasID)
      }
    }
    */
  override def output: Seq[Attribute] = {
    schemaFromExecutedPlan match {
      case Some(x) => child.output.filter(_.exprId != bsMultiplictyAliasID)
      case None => {
        val errorEstimates = getErrorEstimates

        if(errorEstimates.nonEmpty) {
          mixErrorEstimatesInOutput(child.output.filter(
            _.exprId != bsMultiplictyAliasID), errorEstimates)
        } else {
          child.output.filter(_.exprId != bsMultiplictyAliasID)
        }
      }
    }
  }

  def getErrorEstimates : Seq[(NamedExpression, Int)] = {
    val errorColsMap = child.collect {
      case aggs: Aggregate => {
        val hasBSMultiplicity = this.analysisType.map(_ == AnalysisType.Bootstrap).
          getOrElse(false) &&  aggs.aggregateExpressions(0).find( exp => exp match {
          case _: BootstrapMultiplicityAggregate => true
          case _ => false
        } ).isDefined
        aggs.aggregateExpressions.flatMap {
          _.collect {
            case errAggFunc: ErrorAggregateFunction => errAggFunc.errorEstimateProjs.
                map(tup => (if (hasBSMultiplicity) { tup._1 - 1 } else { tup._1 }) -> tup._6)
          }.flatten
        }
      }
    }.flatten.toMap
    errorColsMap.toSeq.sortWith((tup1, tup2) => tup1._1.compareTo(tup2._1) < 0).
      map(tup => tup._2 -> tup._1)
  }

  override def producedAttributes: AttributeSet = bootstrapMultiplicities.map(AttributeSet(_)).
      getOrElse(AttributeSet.empty)

  def mixErrorEstimatesInOutput(baseOutput: Seq[Attribute],
      errorEst: Seq[(NamedExpression, Int)]): Seq[Attribute] = {
    var i = 0
    var j = 0
    val y = for (x <- baseOutput) yield {
      var loopCondition = j < errorEst.length && i == errorEst(j)._2 - 1
      if (loopCondition) {
        i = i + 1
        x +: (for (k <- errorEst.slice(j, errorEst.length) if loopCondition) yield {
          j = j + 1
          i = i + 1
          loopCondition = j < errorEst.length && i == errorEst(j)._2
          k._1
        })
      } else {
        i = i + 1
        Seq(x)
      }
    }
    y.flatten.map(_.toAttribute)
  }

  override lazy val schema = schemaFromExecutedPlan match {
    case Some(x) => x.executedPlan.schema
    case None => StructType.fromAttributes(this.output)
  }

  override def toString: String = s"SampleTableQuery=confidence=$confidence," +
      s" error=$error, behavior=$behavior, child=$child"
}

object SampleTableQuery {

  def masquerade(sr: MapColumnToWeight): Masquerader = new Masquerader(sr)

  private[sql] def createOrDropReservoirRegion(table: String,
      baseRelation: BaseColumnFormatRelation,
      reservoirRegionName: String, isDrop: Boolean, removeSampler: Boolean = false): Unit = {
    val conn = baseRelation.connFactory()
    try {
      val stmt = conn.prepareStatement("CALL SYS.CREATE_OR_DROP_RESERVOIR_REGION(?, ?, ?)")
      stmt.setString(1, reservoirRegionName)
      stmt.setString(2, table)
      stmt.setBoolean(3, isDrop)
      stmt.executeUpdate()
    } finally {
      conn.commit()
      conn.close()
    }
  }
}

class Masquerader(val sampleRatioAttribute: MapColumnToWeight) extends Serializable
