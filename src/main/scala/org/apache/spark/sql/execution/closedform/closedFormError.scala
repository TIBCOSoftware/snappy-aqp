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
/**
  * Aggregates and related classes for error estimates.
  */
package org.apache.spark.sql.execution.closedform

import scala.language.existentials

import org.apache.commons.math3.distribution.TDistribution

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.aggregate.GroupAggregate
import org.apache.spark.sql.execution.common._
import org.apache.spark.sql.types._

case class ClosedFormErrorEstimate(child: Expression, rawWeight: Expression,
    @transient _confidence: Double, _aggregateType: ErrorAggregate.Type, @transient _error: Double,
    _errorEstimateProjs: Seq[(Int, ErrorAggregate.Type, String, String, ExprId, NamedExpression)],
    @transient _behavior: HAC.Type)
    extends ErrorAggregateFunction(_confidence, _error, _behavior, _errorEstimateProjs,
      _aggregateType) with GroupAggregate {

  // val weightVar = rawWeight.toString.replace('#', '_')

  override def nullable: Boolean = aggregateType != ErrorAggregate.Count

  override def dataType: DataType = StatCounterUDT

  private lazy val state: AttributeReference = AttributeReference("CURRENT_STATE", dataType)()

  override def children: Seq[Expression] = child :: rawWeight :: Nil

  override def inputTypes: Seq[AbstractDataType] =
    child.dataType :: rawWeight.dataType :: Nil

  override lazy val aggBufferAttributes: List[AttributeReference] = state :: Nil

  override lazy val aggBufferAttributesForGroup: Seq[AttributeReference] = {
    if (child.nullable) aggBufferAttributes
    else state.copy(nullable = false)(state.exprId, state.qualifier, state.isGenerated) :: Nil
  }

  override lazy val initialValues =
    Seq(StatCounterAggregatorCreator(aggregateType))

  override lazy val initialValuesForGroup: Seq[StatCounterAggregatorCreator] = initialValues

  override lazy val updateExpressions = Seq(StatCounterUpdater(state, child,
    rawWeight, child.dataType, aggregateType == ErrorAggregate.Count))

  override lazy val mergeExpressions =
    Seq(StatCounterMergerer(state.left, state.right, aggregateType))

  lazy val evaluateExpression = Cast(state, dataType)

  override def toString: String = {
    s"ERROR ESTIMATE ($confidence) $aggregateType($child)"
  }

  override def convertToBypassErrorCalcFunction(weight: MapColumnToWeight): AggregateFunction =
    aggregateType match {
      case ErrorAggregate.Sum => AQPSum(weight, Cast(child, DoubleType))
      case ErrorAggregate.Avg => AQPAverage(weight, Cast(child, DoubleType))
      case ErrorAggregate.Count => AQPCount(weight, child :: Nil)
    }

}

private[spark] class StatCounterUDTCF extends StatCounterUDTBase {

  override def typeName: String = "StatCounterCF"

  override protected def getUDTClassName: String =
    classOf[StatCounterUDTCF].getName
}

private[spark] object StatCounterUDTCF {

  def finalizeEvaluation(errorStats: ClosedFormStats,
      confidence: Double, confFactor: Double, aggType: ErrorAggregate.Type,
      error: Double, behavior: HAC.Type): Double = {
    errorStats.triggerSerialization()
    val baseAggregateType = ErrorAggregate.getBaseAggregateType(aggType)

    var pointEstimate = baseAggregateType match {
      case ErrorAggregate.Count => errorStats.weightedCount
      case ErrorAggregate.Sum => errorStats.trueSum
      case ErrorAggregate.Avg => errorStats.trueSum / errorStats.weightedCount
    }

    var stdDev: Double = 0.0
    baseAggregateType match {
      case ErrorAggregate.Avg =>
        stdDev = math.sqrt(errorStats.nvariance /
            (errorStats.weightedCount * errorStats.weightedCount))
      case ErrorAggregate.Sum | ErrorAggregate.Count =>
        stdDev = math.sqrt(errorStats.nvariance)
        // If point estimate is null, so should all error stats
        stdDev = if ( pointEstimate.isNaN) {
          Double.NaN
        } else if (pointEstimate == 0 && baseAggregateType == ErrorAggregate.Count) {
          Double.NaN
        } else {
          stdDev
        }
      case _ => throw new UnsupportedOperationException(
        "Unknown aggregate type = " + baseAggregateType)
    }

    val sampleCount = errorStats.count.toDouble
    // 30 is taken to be cut-off limit in most statistics calculations
    // for z vs t distributions (unlike StudentTCacher that uses 100)
    val errorBoundForPopulation =
    if (sampleCount >= 30) stdDev * confFactor
    // TODO: somehow cache this at the whole evaluation level
    // (wrapper LogicalPlan with StudentTCacher?)
    // the expensive t-distribution
    else stdDev * new TDistribution(math.max(1.0, errorStats.count - 1))
        .inverseCumulativeProbability(0.5 + confidence / 2.0)

    def checkLocalOmit(errorBound: Double, estimate: Double): Boolean = {

      val relError = if (pointEstimate < -1 || pointEstimate > 1) {
        math.abs(errorBound / pointEstimate)
      }
      else {
        math.abs(errorBound / (math.abs(pointEstimate) + 1))
      }
      (relError > error) && (behavior == HAC.SPECIAL_SYMBOL)
    }

    if (ErrorAggregate.isBaseAggType(aggType)) {
      val relError = if (pointEstimate < -1 || pointEstimate > 1) {
        math.abs(errorBoundForPopulation / pointEstimate)
      }
      else {
        math.abs(errorBoundForPopulation / (math.abs(pointEstimate) + 1))
      }

      if (relError > error) {
        if (behavior == HAC.SPECIAL_SYMBOL) {
          pointEstimate = if (aggType != ErrorAggregate.Count) Double.NaN else -1
        }
      }

      pointEstimate
    } else if (ErrorAggregate.isLowerAggType(aggType)) {
      if (checkLocalOmit(errorBoundForPopulation, pointEstimate)) {
        Double.NaN
      } else {
        pointEstimate - errorBoundForPopulation
      }
    } else if (ErrorAggregate.isUpperAggType(aggType)) {
      if (checkLocalOmit(errorBoundForPopulation, pointEstimate)) {
        Double.NaN
      } else {
        pointEstimate + errorBoundForPopulation
      }
    } else if (ErrorAggregate.isRelativeErrorAggType(aggType)) {
      if (checkLocalOmit(errorBoundForPopulation, pointEstimate)) {
        Double.NaN
      } else {
        if (pointEstimate.isNaN) {
          Double.NaN
        } else if (pointEstimate < -1 || pointEstimate > 1) {
          math.abs(errorBoundForPopulation / pointEstimate)
        }
        else {
          math.abs(errorBoundForPopulation / (math.abs(pointEstimate) + 1))
        }
      }
    } else if (ErrorAggregate.isAbsoluteErrorAggType(aggType)) {
      if (checkLocalOmit(errorBoundForPopulation, pointEstimate)) {
        Double.NaN
      } else {
        if (pointEstimate.isNaN) {
          Double.NaN
        } else {
          math.abs(errorBoundForPopulation)
        }

      }
    } else {
      throw new IllegalStateException("Unknown aggregate type = " + aggType)
    }
  }
}

/**
  * The exception thrown when error percent is more than specified.
  */
class ErrorLimitExceededException(message: String)
    extends RuntimeException(message)

case class ErrorEstimateAttribute(name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty, exprId: ExprId,
    realExprId: ExprId)(val qualifier: Option[String] = None) extends Attribute with Unevaluable {
  /**
    * Returns true iff the expression id is the same for both attributes.
    */
  def sameRef(other: AttributeReference): Boolean =
  this.exprId == other.exprId

  override def equals(other: Any): Boolean = other match {
    case ar: AttributeReference => name == ar.name && dataType == ar.dataType &&
      nullable == ar.nullable && metadata == ar.metadata && exprId == ar.exprId &&
      qualifier == ar.qualifier
    case eea: ErrorEstimateAttribute => (eea eq this) || (name == eea.name &&
      dataType == eea.dataType && nullable == eea.nullable && metadata == eea.metadata &&
      exprId == eea.exprId && qualifier == eea.qualifier)
    case _ => false
  }

  override def semanticEquals(other: Expression): Boolean = other match {
    case ar: AttributeReference => sameRef(ar)
    case _ => false
  }

  override def semanticHash(): Int = {
    this.exprId.hashCode()
  }

  override def hashCode(): Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    var h = 17
    h = h * 37 + name.hashCode()
    h = h * 37 + dataType.hashCode()
    h = h * 37 + nullable.hashCode()
    h = h * 37 + metadata.hashCode()
    h = h * 37 + exprId.hashCode()
    h = h * 37 + qualifier.hashCode()
    h
  }

  override def newInstance(): AttributeReference =
    AttributeReference(name, dataType, nullable, metadata)(qualifier = qualifier,
      isGenerated = isGenerated)

  /**
    * Returns a copy of this [[ErrorEstimateAttribute]] with changed nullability.
    */
  override def withNullability(newNullability: Boolean): ErrorEstimateAttribute = {
    if (nullable == newNullability) {
      this
    } else {
      ErrorEstimateAttribute(name, dataType, newNullability, metadata, exprId,
        realExprId)(qualifier)
    }
  }

  override def withName(newName: String): ErrorEstimateAttribute = {
    if (name == newName) {
      this
    } else {
      ErrorEstimateAttribute(newName, dataType, nullable, metadata, exprId, realExprId)(qualifier)
    }
  }

  /**
    * Returns a copy of this [[ErrorEstimateAttribute]] with new qualifier.
    */
  override def withQualifier(newQualifier: Option[String]): Attribute = {
    if (newQualifier == qualifier) {
      this
    } else {
      ErrorEstimateAttribute(name, dataType, nullable, metadata, exprId, realExprId)(newQualifier)
    }
  }

  def withExprId(newExprId: ExprId): ErrorEstimateAttribute = {
    if (exprId == newExprId) {
      this
    } else {
      ErrorEstimateAttribute(name, dataType, nullable, metadata, newExprId, realExprId)(qualifier)
    }
  }

  override def references: AttributeSet = AttributeSet( AttributeReference(name, dataType, nullable,
    metadata)(exprId, qualifier, isGenerated))


  override def withMetadata(newMetadata: Metadata): Attribute = {
    ErrorEstimateAttribute(name, dataType, nullable, newMetadata, exprId, realExprId)(qualifier)
  }

  /** Used to signal the column used to calculate an eventTime watermark (e.g. a#1-T{delayMs}) */
  private def delaySuffix = if (metadata.contains(EventTimeWatermark.delayKey)) {
    s"-T${metadata.getLong(EventTimeWatermark.delayKey)}ms"
  } else {
    ""
  }

  override def toString: String = s"$name#${exprId.id}$typeSuffix$delaySuffix"

  // Since the expression id is not in the first constructor it is missing from the default
  // tree string.
  override def simpleString: String = s"$name#${exprId.id}: ${dataType.simpleString}"

  override def sql: String = {
    val qualifierPrefix = qualifier.map(_ + ".").getOrElse("")
    s"$qualifierPrefix${quoteIdentifier(name)}"
  }
}

case class StatCounterMergerer(left: Expression, right: Expression,
    aggregateType: ErrorAggregate.Type) extends BinaryOperator {
  val symbol: String = "merge"

  override def dataType: DataType = StatCounterUDT

  override def inputType: AbstractDataType =
    ObjectType(classOf[Any]) // ArrayType(ObjectType(classOf[Any]))*/

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val errorAggregateClass = ErrorAggregate.getClass.getName
    val statCounterClass = StatCounterWithFullCount.getClass.getName
    val aggTypeForJava = s"""$errorAggregateClass.MODULE$$.$aggregateType()"""

    defineCodeGen(ctx, ev, (eval1, eval2) =>
      s"""
            $statCounterClass.MODULE$$.$symbol($eval1, $eval2, $aggTypeForJava)
         """
    )
  }
}



case class StatCounterUpdater(buffer: Expression, col: Expression, rawWeight: Expression,
    colDataType: DataType, isCount: Boolean) extends TernaryExpression {
  val symbol: String = "add"

  override def dataType: DataType = StatCounterUDT

  override def children: Seq[Expression] = buffer :: col :: rawWeight :: Nil

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val colAsJson = colDataType.json
    val strippedQuotesColType = colAsJson.substring(1, colAsJson.length - 1)
    val dataTypeClass = DataType.getClass.getName
    val numericAnyClass = classOf[Numeric[Any]].getName

    val  numericInitializerCode = if (!isCount) {
      colDataType match {
        case _: NumericType =>
          val numericTypeClass = classOf[NumericType].getName
          s""" ($numericAnyClass)(($numericTypeClass)
             $dataTypeClass.MODULE$$.fromJson("\\"$strippedQuotesColType\\""))
             .numeric()"""

        case _ =>
          s"""($numericAnyClass)$dataTypeClass.MODULE$$.fromJson(
             "\\"$strippedQuotesColType\\"")"""
      }
    } else {
      null
    }

    val aggClass = classOf[StatCounterAggregator].getName
    val statCounterClass = classOf[StatCounterWithFullCount].getName
    val numericConverterClass = classOf[Numeric[Any]].getName
    val bufferEval = buffer.genCode(ctx)
    val colEval = col.genCode(ctx)
    val rawWeightEval = rawWeight.genCode(ctx)
    val raw = ctx.freshName("rawWeight")
    val colValue = ctx.freshName("colValue")
    val numericConverter = ctx.freshName("numericConverter")
    val currentStatCounterAggregator = ctx.freshName("currentAggregator")
    val currentStrata = ctx.freshName("currentStrata")
    val currentRawWeight = ctx.freshName("currentRawWeight")
    ctx.addMutableState(numericConverterClass, numericConverter,
      s"$numericConverter = $numericInitializerCode;")
    ctx.addMutableState("long", currentRawWeight,
      s"$currentRawWeight = -1L;")
    ctx.addMutableState(statCounterClass, currentStrata,
      s"$currentStrata = null;")
    ctx.addMutableState(aggClass, currentStatCounterAggregator,
      s"$currentStatCounterAggregator = null;")

    val code = s"""
      ${bufferEval.code}
      ${colEval.code}
      ${rawWeightEval.code}
      boolean ${ev.isNull} = false;
      $aggClass ${ev.value} = ($aggClass)${bufferEval.value};
      long $raw = 0l;
      if($currentStatCounterAggregator != ${ev.value}) {
        $currentStatCounterAggregator = ${ev.value};
        $currentRawWeight = -1L;
        $currentStrata = $currentStatCounterAggregator.getStrataErrorStat();
      }
      if (!${colEval.isNull}) {

        $raw = ${rawWeightEval.value};

        double $colValue =  $isCount? 1d: $numericConverter.toDouble(${colEval.value});
        if ($colValue != Double.NEGATIVE_INFINITY) {
          if ($raw == $currentRawWeight ) {
            $currentStrata.add($colValue);
          }else  {
            $currentStatCounterAggregator.store();
            final double cfeWeight;
            final long cfeLeft, cfeRight;
            if ($raw != 0 ) {
              long leftTemp = ($raw >> 40) & 0xffffffffL;
              long rightTemp = ($raw >> 8) & 0xffffffffL;
              if (leftTemp != 0) {
                cfeWeight = ((double)rightTemp) / leftTemp;
                cfeLeft = leftTemp;
                cfeRight = rightTemp;
              } else {
                cfeWeight =1.0;
                cfeLeft = leftTemp;
                cfeRight = rightTemp;
              }
            } else {
              cfeWeight = 1.0d;
              cfeLeft = 0l;
              cfeRight = 0l;
            }
            if (cfeWeight != 0) {
              $currentStrata.reset(cfeLeft, cfeRight, cfeWeight);
              $currentRawWeight = $raw;
              $currentStrata.add($colValue);
            }
          }
        }
      }
    """
    ev.copy(code = code)
  }
}
