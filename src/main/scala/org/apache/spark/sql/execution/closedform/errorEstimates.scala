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

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

object ErrorAggregate extends Enumeration {
  type Type = Value
  val separator = '_'
  val Avg = Value("Avg")
  val Sum = Value("Sum")
  val Count = Value("Count")

  val Sum_Lower = Value(Sum.toString + separator + "Lower")
  val Avg_Lower = Value(Avg.toString + separator + "Lower")
  val Count_Lower = Value(Count.toString + separator + "Lower")

  val Sum_Upper = Value(Sum.toString + separator + "Upper")
  val Avg_Upper = Value(Avg.toString + separator + "Upper")
  val Count_Upper = Value(Count.toString + separator + "Upper")

  // relative error
  val Sum_Relative = Value(Sum.toString + separator + "Relative")
  val Avg_Relative = Value(Avg.toString + separator + "Relative")
  val Count_Relative = Value(Count.toString + separator + "Relative")

  // absolute error
  val Sum_Absolute = Value(Sum.toString + separator + "Absolute")
  val Avg_Absolute = Value(Avg.toString + separator + "Absolute")
  val Count_Absolute = Value(Count.toString + separator + "Absolute")

  def getBaseAggregateType(param: ErrorAggregate.Type): ErrorAggregate.Type = {
    val name = param.toString
    val sepIndex = name.indexOf(separator)
    if (sepIndex == -1) {
      param
    } else {
      val baseName = name.substring(0, sepIndex)
      ErrorAggregate.withName(baseName)
    }
  }

  def getRelativeErrorTypeForBaseType(baseAggregateType: Type): Type = {
    val relErrorName = baseAggregateType.toString + separator + "Relative"
    ErrorAggregate.withName(relErrorName)
  }

  def isBaseAggType(aggType: Type): Boolean = {
    val name = aggType.toString
    val sepIndex = name.indexOf(separator)
    sepIndex == -1
  }

  private def getSuffix(name: String): Option[String] = {
    val sepIndex = name.indexOf(separator)
    if (sepIndex == -1) {
      None
    } else {
      Some(name.substring(sepIndex + 1))
    }
  }

  private def getPrefix(name: String): Option[String] = {
    val sepIndex = name.indexOf(separator)
    if (sepIndex == -1) {
      None
    } else {
      Some(name.substring(0, sepIndex))
    }
  }

  def checkFor(suffix: String, aggType: Type): Boolean = {
    getSuffix(aggType.toString) match {
      case Some(x) => x == suffix
      case None => false
    }
  }

  def checkFor(prefix: String, errorEstimateFuncName: String): Boolean = {
    getPrefix(errorEstimateFuncName) match {
      case Some(x) => x == prefix
      case None => false
    }
  }

  def isLowerAggType(aggType: Type): Boolean = checkFor("Lower", aggType)

  def isUpperAggType(aggType: Type): Boolean = checkFor("Upper", aggType)

  def isRelativeErrorAggType(aggType: Type): Boolean =
    checkFor("Relative", aggType)

  def isAbsoluteErrorAggType(aggType: Type): Boolean =
    checkFor("Absolute", aggType)

  def isLowerAggType(errorEstimateFuncName: String): Boolean =
    checkFor("Lower", errorEstimateFuncName)

  def isUpperAggType(errorEstimateFuncName: String): Boolean =
    checkFor("Upper", errorEstimateFuncName)

  def isRelativeErrorAggType(errorEstimateFuncName: String): Boolean =
    checkFor("Relative", errorEstimateFuncName)

  def isAbsoluteErrorAggType(errorEstimateFuncName: String): Boolean =
    checkFor("Absolute", errorEstimateFuncName)


}

/**
 * A class for tracking the statistics of a set of numbers (count, mean,
 * variance and weightedCount) in a numerically robust way.
 * Includes support for merging two counters.
 *
 * Taken from Spark's StatCounter implementation removing max and min.
 */
class StatCounterWithFullCount(var weightedCount: Double,
    var trueSum: Double, var leftWeight: Double = 0.0,
    var rightWeight: Double = 0.0, var weight: Double = Double.NaN) extends BaseGenericInternalRow
    with ClosedFormStats {

  protected var valSquareByCount: Double = 0.0
  private[closedform] var cfMeanTotal: Double = 0.0

  /** No-arg constructor for serialization. */
  def this() = this(0, Double.NaN)

  def this(grow: InternalRow) = {
    this(0, 0)
    if (grow.numFields == StatCounterUDT.numFields) {
      count = grow.getLong(0)
      mean = grow.getDouble(1)
      nvariance = grow.getDouble(2)
      weightedCount = grow.getDouble(3)
      trueSum = grow.getDouble(4)
    }
  }

  def reset(lw: Double, rw: Double, w: Double): Unit = {
    this.weightedCount = 0
    this.trueSum = 0
    this.leftWeight = lw
    this.rightWeight = rw
    this.weight = w
    this.count = 0
    this.mean = 0
    this.nvariance = 0
    this.valSquareByCount = 0
    this.cfMeanTotal = 0
  }

  def cfmerge(value: Double) {
    val delta = value - mean
    count += 1
    mean += delta / count
    valSquareByCount += value * value
    cfMeanTotal += value
  }

  override def copy(other: ClosedFormStats): Unit = {
    super.copy(other)
    other match {
      case s: StatCounterWithFullCount =>
        s.valSquareByCount = valSquareByCount
        s.cfMeanTotal = cfMeanTotal
      case _ =>
    }
  }

  override def copy(): StatCounterWithFullCount = {
    val other = new StatCounterWithFullCount(weightedCount, trueSum,
      leftWeight, rightWeight, weight)
    copy(other)
    other
  }

  /** Evaluate once at each partition */
  // Noop
  def triggerSerialization() {}


  override def toString: String = {
    ("(count: %d, mean: %f, stdDev: %f, weightedCount: %f, trueSum: %f," +
        " nvariance: %f)").format(count, mean, stdev, weightedCount, trueSum,
      nvariance)
  }

  def computeVariance(aggregateType: ErrorAggregate.Type) {
    aggregateType match {
      case ErrorAggregate.Sum | ErrorAggregate.Avg =>
        this.nvariance = if (this.leftWeight != 0) {
          val mulFactor = this.rightWeight * (this.rightWeight - this.leftWeight) /
              (this.leftWeight - 1.0)
          /**
           * The strataVariance mentioned below is variance over sample strata
           * calculated for conditional sum.
           *
           * The variance here is calculated in single pass without using any
           * online algorithm, with following derivation.
           * Given n number of values i.e. a, b, c, d .....
           * Also 'x' as there 'mean' i.e. x = (a + b + c + d + ...) / n
           *
           * Then formula for variance, V is as given below.
           * V = ((a - x)*(a - x) + (b - x)*(b -x) + (c - x)*(c - x) + ... ) / n
           *
           * We can reduce above formula for Variance using following algebra property.
           * (A - B)*(A - B) = (A*A + B*B - 2*A*B)
           *
           * So above formula for Variance reduces to,
           * V = ((a*a + b*b * c*c + d*d + ... till n times)
           * + (x*x + x*x + x*x + ... till n times)
           * - 2*x*(a + b + c + ....till n times)) / n
           *
           * This reduces to,
           * V = (a*a + b*b * c*c + d*d + ... till n times) / n
           * + x*x
           * - 2*x*(a + b + c + ....till n times) / n
           *
           * You will notice that, x = (a + b + c + ....till n times) / n
           * So above formula for variance further reduces to,
           * V = (a*a/n + b*b/n * c*c/n + d*d/n + ... till n times) - x*x
           *
           * And above factors can be calculated in single pass since we already
           * store n. x can further be calculated when we have all n items.
           */
          val cfMean = this.cfMeanTotal / this.leftWeight
          val strataVariance = this.valSquareByCount / this.leftWeight - cfMean * cfMean
          this.summableStrataVariance(mulFactor, strataVariance)
        } else 0.0

      case ErrorAggregate.Count =>
        this.nvariance = if (this.leftWeight != 0) {
          val mulFactor = this.rightWeight * (this.rightWeight - this.leftWeight) /
              (this.leftWeight - 1.0)
          val strataVariance = (this.count / this.leftWeight) *
              (1 - (this.count / this.leftWeight))
          this.summableStrataVariance(mulFactor, strataVariance)
        } else 0.0
      case _ => // TODO throw?
    }
  }

  def add(v: Double): Unit = {
    // this.cfmerge(v)
    val delta = v - mean
    count += 1
    mean += delta / count
    valSquareByCount += v * v
    cfMeanTotal += v
    // do not add with weight here. will multiply by the count with weight at end during variance
    // computation
    // this.weightedCount += 1
    // do not multiply with weight here. will do it at end during variance computation
    // using cfMeanTotal instead .
    // this.trueSum += v
  }

  private def summableStrataVariance(mulFactor: Double,
      strataVariance: Double): Double = {
    val cfVar = mulFactor * strataVariance

    /** variance should not be negative or NaN. If so, ignore this strata */
    if (cfVar.isNaN || cfVar < 0.0) {
      StatCounterWithFullCount.trace(s"Strata ignored. Variance=%.2f".format(cfVar)
          + s",LeftWeight=%.2f".format(this.leftWeight)
          + s",RightWeight=%.2f".format(this.rightWeight)
          + s",Count=" + this.count
          + s",Strata Variance=%.2f".format(strataVariance)
          + s",Multiplication Factor=%.2f".format(mulFactor)
      )
      0.0
    } else cfVar
  }

  override def setNullAt(i: Int): Unit = {}

  override def update(i: Int, value: Any): Unit = {}
}

final class MultiTableStatCounterWithFullCount(_weightedCount: Double,
    _trueSum: Double, _leftWeight: Double = 0.0, _rightWeight: Double = 0.0)
    extends StatCounterWithFullCount(_weightedCount, _trueSum, _leftWeight,
      _rightWeight) {
  private var part2: Double = 0.0

  override def computeVariance(aggregateType: ErrorAggregate.Type) {
    aggregateType match {
      case ErrorAggregate.Sum | ErrorAggregate.Avg =>
        val multiplicationFactor = if (this.leftWeight != 0) {
          this.rightWeight * (this.rightWeight / this.leftWeight - 1)
        } else 0.0
        this.nvariance = (this.valSquareByCount / this.leftWeight) * multiplicationFactor
        val partAvg = this.cfMeanTotal / this.leftWeight
        this.part2 = partAvg * partAvg * multiplicationFactor
      case ErrorAggregate.Count =>
        this.nvariance = if (this.leftWeight != 0) {
          /*
          val mulFactor = this.rightWeight * (this.rightWeight - this.leftWeight) /
              (this.leftWeight - 1.0)
          val strataVariance = (this.count / this.leftWeight) *
              (1 - (this.count / this.leftWeight))
          this.summableStrataVariance(mulFactor, strataVariance)
          */
          0.0
        } else 0.0
      case _ => // TODO throw?
    }
  }

  def setPart2(part2: Double): Unit = {
    this.part2 = part2
  }

  override def numFields: Int = 6

  protected override def genericGet(ordinal: Int): Any = {
    triggerSerialization()
    if (ordinal < 5) {
      super.genericGet(ordinal)
    } else {
      ordinal match {
        case 5 => this.part2
      }
    }

  }


  override def getDouble(ordinal: Int): Double = {
    triggerSerialization()
    if (ordinal < 5) {
      super.getDouble(ordinal)
    } else {
      ordinal match {
        case 5 => this.part2
      }
    }

  }

  override def merge(other: ClosedFormStats) {
    if (other ne this) {
      super.mergeDistinctCounter(other)
      weightedCount += other.weightedCount
      mergeTrueSum(other)
      // this.nvariance += (other.nvariance -
      //   other.asInstanceOf[MultiTableStatCounterWithFullCount].part2 / m)

    } else {
      merge(other.copy()) // Avoid overwriting fields in a weird order
    }
  }

  override def setNullAt(i: Int): Unit = {}

  override def update(i: Int, value: Any): Unit = {}

  override def copy(other: ClosedFormStats): Unit = {
    super.copy(other)
    other match {
      case s: MultiTableStatCounterWithFullCount => s.part2 = part2
      case _ =>
    }
  }

  override def copy(): MultiTableStatCounterWithFullCount = {
    val other = new MultiTableStatCounterWithFullCount(weightedCount, trueSum,
      leftWeight, rightWeight)
    copy(other)
    other
  }
}

object StatCounterWithFullCount extends Logging {

  private def convertInternalRow(ir: InternalRow): ClosedFormStats = {
    ir match {
      case x: StatCounterWithFullCount => x
      case x: StatCounterAggregator => x
      case _ => new StatCounterWithFullCount(ir)
    }
  }

  def merge(thisGR: InternalRow, value: InternalRow, // colDataType: DataType,
      aggType: ErrorAggregate.Type): InternalRow = {
    val thisObj = convertInternalRow(thisGR)
    val param = convertInternalRow(value)
    thisObj.triggerSerialization()
    param.triggerSerialization()
    thisObj.merge(param)
    thisObj.asInstanceOf[InternalRow]
  }

  def trace(trace: String): Unit = {
    this.logTrace(trace)
  }
}

private[spark] class MultiTableStatCounterUDT extends StatCounterUDTBase {

  override protected def getUDTClassName: String =
    classOf[MultiTableStatCounterUDT].getName

  override def sqlType: StructType = {
    val baseType = super.sqlType
    val fields = baseType.seq :+ StructField("multiplicationFactor",
      DoubleType, nullable = false)
    // types for various serialized fields of StatCounterWithFullCount
    StructType(fields)
  }

  // This method is not used, but sqlType above. See triggerSerialization
  override def serialize(obj: ClosedFormStats): InternalRow = {
    obj match {
      case s: StatCounterWithFullCount => s
      case s: StatCounterAggregator => s
      // due to bugs in UDT serialization (SPARK-7186)
      case row: InternalRow => row
    }
  }

  override def deserialize(datum: Any): ClosedFormStats = {
    datum match {
      case s: ClosedFormStats => s
      // due to bugs in UDT serialization (SPARK-7186)
      case row: InternalRow =>
        require(row.numFields == 6, "StatCounterUDTBase.deserialize given " +
            s"row with length ${row.numFields} but requires length == 7")
        val s = new MultiTableStatCounterWithFullCount(row.getDouble(3),
          row.getDouble(4))
        s.initStats(count = row.getLong(0), mean = row.getDouble(1),
          nvariance = row.getDouble(2))
        // s.setClosedFormVariance(cfvar = row.getDouble(5))
        s.setPart2(row.getDouble(5))
        s
    }
  }
}

private[spark] case object StatCounterUDT extends StatCounterUDTBase {
  val numFields: Int = 5
}

private[spark] class StatCounterUDTBase
    extends UserDefinedType[ClosedFormStats] {

  override def sqlType: StructType = {
    // types for various serialized fields of StatCounterWithFullCount
    StructType(Seq(
      StructField("count", LongType, nullable = false),
      StructField("mean", DoubleType, nullable = false),
      StructField("nvariance", DoubleType, nullable = false),
      StructField("weightedCount", DoubleType, nullable = false),
      StructField("trueSum", DoubleType, nullable = false)
    ))
  }

  // This method is not used, but sqlType above. See triggerSerialization
  override def serialize(obj: ClosedFormStats): InternalRow = {
    obj.asInstanceOf[InternalRow]
  }

  override def deserialize(datum: Any): ClosedFormStats = {
    datum match {
      case s: ClosedFormStats => s
      // due to bugs in UDT serialization (SPARK-7186)
      case row: InternalRow =>
        require(row.numFields == StatCounterUDT.numFields,
          s"StatCounterUDTBase.deserialize given row with length ${row.numFields}" +
              s"but requires length == ${StatCounterUDT.numFields}")
        val s = new StatCounterWithFullCount(row.getDouble(3), row.getDouble(4))
        s.initStats(count = row.getLong(0), mean = row.getDouble(1),
          nvariance = row.getDouble(2))
        s
    }
  }

  override def typeName: String = "StatCounter"

  override private[sql] def jsonValue: JValue = {
    ("type" -> "udt") ~
    ("class" -> classOf[StatCounterUDTBase].getName) ~
    ("pyClass" -> pyUDT) ~
    ("sqlType" -> sqlType.jsonValue)
  }

  protected def getUDTClassName: String = classOf[StatCounterUDTBase].getName

  override def userClass: Class[ClosedFormStats] =
    classOf[ClosedFormStats]

  private[spark] override def asNullable = this
}
