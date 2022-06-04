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
package org.apache.spark.sql.execution.bootstrap


import org.apache.commons.math3.distribution.PoissonDistribution
import org.apache.spark.mllib.random.RandomDataGenerator
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.{InternalRow, util => newUtil}
import org.apache.spark.sql.execution.bootstrap.PoissonCreator.PoissonType.PoissonType
import org.apache.spark.sql.execution.closedform.ErrorAggregate
import org.apache.spark.sql.execution.common.HAC
import org.apache.spark.sql.types._
import org.apache.spark.util.random.{XORShiftRandom, XORShiftRandomAccessor}

class PoissonGenerator(
    val lambda: Int) extends RandomDataGenerator[Int] {

  private val rng = new PoissonDistribution(lambda)

  override def nextValue(): Int = rng.sample()

  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
  }

  override def copy(): PoissonGenerator = new PoissonGenerator(lambda)
}

case class SnappyPoissonSeed(usedFixedSeed: Boolean = false) extends LeafExpression {
  type EvaluatedType = Any



  def dataType: DataType = IntegerType

  def nullable: Boolean = false

  override def foldable: Boolean = false

  override def toString: String =
    "SnappyPoissonSeed"

  override def deterministic: Boolean = true

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("eval not implemented")

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    ev.isNull = "false"
    val seed = ctx.freshName("seed")
    val tempVar = ctx.freshName("temp")
    val initSeed = if (usedFixedSeed) 123456789L else XORShiftRandomAccessor.hashSeed(System.
      nanoTime())
    // TODO: Asif, shouldn't the code of initSeed itself go inside for !useFixedSeed
    ctx.addMutableState(ctx.JAVA_LONG, seed, s"$seed = ${initSeed}L;")
    ev.isNull = "false"
    val code = s"""
       long $tempVar = $seed;
       $tempVar ^= ($tempVar << 21);
       $tempVar ^= ($tempVar >>> 35);
       $tempVar ^= ($tempVar << 4);
       $seed = $tempVar;
       int ${ev.value}= (int)($tempVar & 0xFFFFFFFFL);
    """
    ev.copy(code = code)
  }
}


case class DebugFixedSeed(initSeed: Int = 1) extends LeafExpression {
  type EvaluatedType = Any

  val dataType: DataType = IntegerType

  def nullable: Boolean = false

  override def foldable: Boolean = false

  override def toString: String = "DebugFixedSeed()"

  override def deterministic: Boolean = true

  override def eval(input: InternalRow): Any = initSeed

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val seed = ctx.freshName("seed")
    ctx.addMutableState(ctx.JAVA_INT, seed, s"$seed = $initSeed;")
    // TODO: Can we cache the array instead of recreating every time?
    ev.isNull = "false"
    val code =
      s"""
       int tempVal = $seed;
       $seed = 1;
       int ${ev.value} = tempVal;
    """
    ev.copy(code = code)
  }
}

object PoissonCreator {

  object PoissonType extends Enumeration {
    type PoissonType = Value
    val Real, DbgIndpPredictable, DbgDepPredictable = Value
  }

  def apply(poissonType: PoissonType, numBootstrapTrials: Int,
      seed: Expression): SuperPoisson = poissonType match {
    case PoissonType.Real => Poisson(seed, numBootstrapTrials)
    case PoissonType.DbgIndpPredictable => DebugIndpndntPredictable(seed,
      numBootstrapTrials)
  }
}

trait SuperPoisson extends UnaryExpression {

  val poissonValueGenerator: Expression

  def dataType: DataType = ArrayType(ByteType, containsNull = false)

  def child: Expression = poissonValueGenerator

  override def foldable: Boolean = false

  override def toString: String = "Poisson()"

  type EvaluatedType = Any
}

// !Hack: In general, we represent the multiplicity vector as an array of long.
// But in order to pack them tight together, we should note that the number of bits
// needs to represent a single multiplicity may grow. E.g., at the input, each multiplicity
// will probably fall within 0-4,
// but after a two-way join, the range may grow to 0-16, so on so forth.
case class Poisson(poissonValueGenerator: Expression,
    numBootstrapTrials: Int) extends SuperPoisson {

  override def eval(input: InternalRow): Any = {
    val poissonizedNumbers = poissonValueGenerator.eval(input)
        .asInstanceOf[newUtil.ArrayData]
    val multiplcityCols = for (i <- 0 until numBootstrapTrials) yield {
      if (i == 0) {
        1.toByte
      } else {
        val current = poissonizedNumbers.getInt(i - 1)
        if (current <= -567453481) 0.toByte
        else if (current <= 1012576688) 1.toByte
        else if (current <= 1802591772) 2.toByte
        else if (current <= 2065930134) 3.toByte
        else if (current <= 2131764724) 4.toByte
        else if (current <= 2144931642) 5.toByte
        else if (current <= 2147126128) 6.toByte
        else if (current <= 2147439627) 7.toByte
        else if (current <= 2147478814) 8.toByte
        else if (current <= 2147483168) 9.toByte
        else if (current <= 2147483603) 10.toByte
        else if (current <= 2147483643) 11.toByte
        else if (current == 2147483647) 15.toByte
        else 12.toByte
      }
    }
    new GenericArrayData(multiplcityCols)
  }


  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {

    ev.isNull = "false"
    val eval = child.genCode(ctx)
    val code =
      s"""
      ${eval.code}
      final ArrayData ${ev.value} = ${eval.value};
    """
    ev.copy(code = code)
  }

  override def toString: String = "Poisson(" + numBootstrapTrials + ")"
}

object Poisson {
  def getMultiplicity(current: Int): Byte = {
    if (current <= -567453481) 0.toByte
    else if (current <= 1012576688) 1.toByte
    else if (current <= 1802591772) 2.toByte
    else if (current <= 2065930134) 3.toByte
    else if (current <= 2131764724) 4.toByte
    else if (current <= 2144931642) 5.toByte
    else if (current <= 2147126128) 6.toByte
    else if (current <= 2147439627) 7.toByte
    else if (current <= 2147478814) 8.toByte
    else if (current <= 2147483168) 9.toByte
    else if (current <= 2147483603) 10.toByte
    else if (current <= 2147483643) 11.toByte
    else if (current == 2147483647) 15.toByte
    else 12.toByte
  }
}




case class DebugIndpndntPredictable(poissonValueGenerator: Expression,
    numBootstrapTrials: Int) extends SuperPoisson {

  override def eval(input: InternalRow): Any = {
    poissonValueGenerator.eval(input)
  }

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    ev.isNull = "false"
    val eval = child.genCode(ctx)
    val code =
      s"""
      ${eval.code}
      final ArrayData ${ev.value} = ${eval.value};
    """
    ev.copy(code = code)
  }

  def applyMultiplicity(rows: Array[Row], numIterations: Int): Array[Row] = {
    val sf = rows(0).schema.iterator.next
    val sfs = Seq.fill[StructField](numIterations)(sf)
    val newSchema = StructType(sfs)
    rows.map(row => {
      var multiplicity = 1
      var i = 0
      val newData = Array.fill[Any](numIterations) {
        val value = row.getFloat(0) * multiplicity
        i += 1
        if (i > 0) {
          multiplicity += 1
        }
        value
      }
      new GenericRowWithSchema(newData, newSchema)
    })
  }

  def getMultiplicity(numIterations: Int): Array[Int] = {
    var multiplicity = 1
    var i = 0
    Array.fill[Int](numIterations) {
      val currentMultiplicity = multiplicity
      i += 1
      if (i > 0) {
        multiplicity += 1
      }
      currentMultiplicity
    }
  }
}


case class BootstrapReferencer(child: LogicalPlan, bsAliasID: ExprId,
                               bootstrapMultiplicities: Expression)
    extends logical.UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def toString: String = s"BootstrapReferencer"

  override def references: AttributeSet = super.references ++
      AttributeSet(AttributeReference(BootstrapMultiplicity.aggregateName,
        ByteType)(bsAliasID) :: Nil)

  override def producedAttributes: AttributeSet = AttributeSet(bootstrapMultiplicities.
    asInstanceOf[Attribute])
}

case class ApproxColumn(
    column: Expression,
    multiplicityResultExpr: Expression,
    confidence: Double,
    baseAggregateType: ErrorAggregate.Type,
    error: Double, behavior: HAC.Type)
    extends Expression {

  type EvaluatedType = Any
  override def nullable: Boolean = false

  override lazy val resolved = true


  override def dataType: DataType = {
    val eNull = column.nullable
    StructType(
      StructField("point_estimate", DoubleType, eNull) ::
          StructField("conf_inv_lower", DoubleType, eNull) ::
          StructField("conf_inv_upper", DoubleType, eNull) ::
          StructField("abs_errr", DoubleType, eNull) ::
          StructField("rel_err", DoubleType, eNull) ::
          Nil
    )
  }

  override def children: Seq[Expression] =
    column :: multiplicityResultExpr :: Nil

  override def toString: String = s"Approx($column)"

  private[this] val lower = (1 - confidence) / 2
  private[this] val upper = (1 + confidence) / 2


  override def eval(input: InternalRow): Any = {
    val multiplicityData = multiplicityResultExpr.eval(input).asInstanceOf[ByteMutableRow]
    val columnData = {
      column.eval(input).asInstanceOf[InternalRow]

    }
          BootstrapFunctions.evalApproxColumn(multiplicityData, columnData,
        lower, upper, baseAggregateType, error, behavior)

  }

  override protected def doGenCode(ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val errorAggregateClass = ErrorAggregate.getClass.getName
    val aggTypeForJava = s"""$errorAggregateClass.MODULE$$.$baseAggregateType()"""

    val behaviorClass = HAC.getClass.getName
    val behaviorTypeForJava = s"""$behaviorClass.MODULE$$.$behavior()"""
    val stateClass = classOf[ByteMutableRow].getName
    val columnEval = column.genCode(ctx)
    val multiplicityEval = multiplicityResultExpr.genCode(ctx)
    val functionClass = BootstrapFunctions.getClass.getName
    val code =
      s"""
      boolean ${ev.isNull} = false;
      InternalRow ${ev.value} = null;
      ${columnEval.code}
      ${multiplicityEval.code}
      ${ev.value} = $functionClass.MODULE$$.${
        BootstrapFunctions.
            evalApproxColumn
      }(($stateClass)${multiplicityEval.value},
        ${columnEval.value},$lower, $upper, $aggTypeForJava, $error,
        $behaviorTypeForJava);
    """
    ev.copy(code = code)
  }
}

object ApproxColumn {
  val TOTAL_STRUCT_FIELDS = 5
  val ORDINAL_ESTIMATE = 0
  val ORDINAL_LB = 1
  val ORDINAL_UB = 2
  val ORDINAL_ABSERR = 3
  val ORDINAL_RELERR = TOTAL_STRUCT_FIELDS - 1

  def getOrdinal(aggregateType: ErrorAggregate.Type): Int = aggregateType match {
    case ErrorAggregate.Avg | ErrorAggregate.Sum | ErrorAggregate.Count =>
      ORDINAL_ESTIMATE
    case ErrorAggregate.Avg_Lower | ErrorAggregate.Sum_Lower |
         ErrorAggregate.Count_Lower => ORDINAL_LB
    case ErrorAggregate.Sum_Upper | ErrorAggregate.Avg_Upper |
         ErrorAggregate.Count_Upper => ORDINAL_UB
    case ErrorAggregate.Sum_Relative | ErrorAggregate.Avg_Relative |
         ErrorAggregate.Count_Relative => ORDINAL_RELERR
    case ErrorAggregate.Sum_Absolute | ErrorAggregate.Avg_Absolute |
         ErrorAggregate.Count_Absolute => ORDINAL_ABSERR
  }
}

case class ApproxColumnDebug(column: Expression, numBootstrapTrials: Int)
    extends Expression {

  override def nullable: Boolean = false

  override lazy val resolved = childrenResolved &&
      column.dataType.asInstanceOf[ArrayType].elementType
          .isInstanceOf[NumericType]

  override val dataType: DataType = StructType(Seq.tabulate[StructField](
    numBootstrapTrials)(i => StructField("bootsrap_" + i, DoubleType,
    nullable = false)))

  override def children: Seq[Expression] = column :: Nil

  override def toString: String = s"Approx($column)"

  override def eval(input: InternalRow): Any = {
    val arrayData = column.eval(input).asInstanceOf[InternalRow]
    val row = new GenericInternalRow(numBootstrapTrials)
    (0 until numBootstrapTrials).foreach {
      i => row(i) = arrayData.getDouble(i)
    }
    row
  }


  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {
    val columnEval = column.genCode(ctx)
    val rowVar = ctx.freshName("rowVar")
    val retunRowVar = ctx.freshName("retunRowVar")
    val genRowClass = classOf[GenericInternalRow].getName
    val code =
      s"""
      boolean ${ev.isNull} = false;
      ${columnEval.code}
      InternalRow $rowVar = (InternalRow)${columnEval.value};
      $genRowClass $retunRowVar = new $genRowClass($numBootstrapTrials);
      for(int x =0; x < $numBootstrapTrials; ++x) {
        $retunRowVar.update(x, Double.valueOf($rowVar.getDouble(x)));
      }
      InternalRow ${ev.value} = $retunRowVar;
    """
    ev.copy(code = code)
  }

}
