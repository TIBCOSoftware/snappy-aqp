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
package io.snappydata.core.aqp


import scala.reflect.io.Path
import scala.util.Try

import io.snappydata.{Property, Constant => Constants}
import org.junit.Assert.assertEquals

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Alias, GenericRowWithSchema}
import org.apache.spark.sql.execution.common.AnalysisType
import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopJoinExec, HashJoin, SortMergeJoinExec}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.hive.IdentifySampledRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.{AssertAQPAnalysis, SparkConf}


class BootStrapAggregateFunctionTest
    extends ErrorEstimateFunctionTest {

  protected def addSpecificProps(conf: SparkConf): Unit = {

    conf.setAppName("BootStrapAggregateFunctionTest")
      .set(Property.ClosedFormEstimates.name, "false").set(Property.NumBootStrapTrials.name, "8")
        .set(Property.AqpDebugFixedSeed.name, "true")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator",
          "org.apache.spark.sql.execution.serializer.SnappyKryoRegistrator")

  }

  override def assertAnalysis(): Unit = {
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
  }


  test("Sample Table Query on Count aggregate with error estimates") {
    val result = snc.sql(s"SELECT count(l_quantity) as x, lower_bound(x) , " +
        s"upper_bound(x), absolute_error(x), " +
        s"relative_error(x) FROM $mainTable with error $DEFAULT_ERROR confidence .95 ")

    val rows2 = result.collect()
    val estimate = rows2(0).getLong(0)
    msg("estimate=" + estimate)
    assert(Math.abs(estimate - 13) < .8)
    msg("lower bound=" + rows2(0).getDouble(1) + ", upper bound" + rows2(0).getDouble(2)
        + ", absolute error =" + rows2(0).getDouble(3) +
        ", relative error =" + rows2(0).getDouble(4))
  }



  test("Sample Table Query with error %  should get error value correctly set") {
    val error = 0.13
    val result = snc.sql(s"SELECT sum(l_quantity) as T, absolute_error(T) FROM $mainTable"
      + " WITH ERROR " + error)

    val analysis = DebugUtils.getAnalysisApplied(result.queryExecution.executedPlan)
    assert(analysis match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val errorAggFunc = DebugUtils.getAnyErrorAggregateFunction(result.queryExecution.executedPlan)

    assert(errorAggFunc.get.confidence === Constants.DEFAULT_CONFIDENCE)
    assert(errorAggFunc.get.error === error)

  }


  ignore("Bug SNAP-225 Sample Table Query with  confidence % & error % " +
      "should get both values correctly set ") {

    val confidence = .85
    val error = 0.07
    val path: Path = Path(" /tmp/hive")
    Try(path.deleteRecursively())

    val result = snc.sql(s"SELECT sum(l_quantity) as T FROM $mainTable"
        + " confidence " + confidence + " with error " + error)

    val analysis = DebugUtils.getAnalysisApplied(result.queryExecution.executedPlan)
    assert(analysis match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val errorAggFunc = DebugUtils.getAnyErrorAggregateFunction(result.queryExecution.executedPlan)

    assert(errorAggFunc.get.confidence === confidence)
    assert(errorAggFunc.get.error === error)
  }

  test("Sample Table Query with error % & confidence % should get both values correctly set ") {

    // val numBootStrapTrials = 100
    val confidence = .87
    val error = 0.07

    val result = snc.sql(s"SELECT sum(l_quantity) as T, absolute_error(T) FROM $mainTable"

        + "  with error " + error + " confidence " + confidence)

    val analysis = DebugUtils.getAnalysisApplied(result.queryExecution.executedPlan)
    assert(analysis match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val errorAggFunc = DebugUtils.getAnyErrorAggregateFunction(result.queryExecution.executedPlan)

    assert(errorAggFunc.get.confidence === confidence)
    assert(errorAggFunc.get.error === error)
    result.collect()
  }

  test("same Sample Table Query fired multiple times should  be parsed correctly  ") {
    val result = snc.sql(s"SELECT sum(l_quantity) as T, absolute_error(T) FROM $mainTable"

        + " with error " + 0.07 + " confidence " + .92)

    val analysis = DebugUtils.getAnalysisApplied(result.queryExecution.executedPlan)
    assert(analysis match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val errorAggFunc = DebugUtils.getAnyErrorAggregateFunction(result.queryExecution.executedPlan)

    assert(errorAggFunc.get.confidence === .92)
    assert(errorAggFunc.get.error === 0.07)
    result.collect()

    val result1 = snc.sql(s"SELECT sum(l_quantity) as T, absolute_error(T) FROM $mainTable"

        + "  with error " + 0.07 + " confidence " + .92)

    val analysis1 = DebugUtils.getAnalysisApplied(result1.queryExecution.executedPlan)
    assert(analysis1 match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val errorAggFunc1 = DebugUtils.getAnyErrorAggregateFunction(result1.queryExecution.executedPlan)

    assert(errorAggFunc1.get.confidence === .92)
    assert(errorAggFunc1.get.error === 0.07)

    result1.collect()

    val result2 = snc.sql(s"SELECT sum(l_quantity) as T, absolute_error(T) FROM $mainTable"

        + " with error " + 0.09 + " confidence " + .93)


    val analysis2 = DebugUtils.getAnalysisApplied(result2.queryExecution.executedPlan)
    assert(analysis2 match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val errorAggFunc2 = DebugUtils.getAnyErrorAggregateFunction(result2.queryExecution.executedPlan)

    assert(errorAggFunc2.get.confidence === .93)
    assert(errorAggFunc2.get.error === 0.09)
    result2.collect()
  }

  test("Sample Table Query with confidence %   should get confidence value correctly set ") {

    // val numBootStrapTrials = 100

    val confidence = .87

    val result = snc.sql(s"SELECT sum(l_quantity) as T, absolute_error(T) FROM $mainTable" +
        s" with error $DEFAULT_ERROR Confidence " + confidence)


    val analysis = DebugUtils.getAnalysisApplied(result.queryExecution.executedPlan)
    assert(analysis match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val errorAggFunc = DebugUtils.getAnyErrorAggregateFunction(result.queryExecution.executedPlan)

    assert(errorAggFunc.get.confidence === confidence)
    assert(errorAggFunc.get.error === Constants.DEFAULT_ERROR)
  }


  test("Hashjoin Bug - 1") {
    snc.setConf(Property.DisableHashJoin.name, "false")
    snc.sql(s"create view airline_view as " +
      s"(select t1.*, t2.description from $airlineMainTable t1, " +
      s"$airlineRefTable t2 where t1.uniquecarrier = t2.code)")

    var rs1 = snc.sql("select count(*) couunt, count(*) sample_count from airline_view with error")
    AssertAQPAnalysis.bypassErrorCalc(snc)

    var rs2 = snc.sql(s"select count(*) couunt, count(*) sample_count " +
      s" from $airlineMainTable t1, $airlineRefTable t2 where  t1.uniquecarrier = t2.code" +
      s" with error")
    AssertAQPAnalysis.bypassErrorCalc(snc)

    var row1 = rs1.collect()(0)
    var row2 = rs2.collect()(0)
    assertEquals(row1.getLong(0), row2.getLong(0))
    assertEquals(row1.getLong(1), row2.getLong(1))

    rs1 = snc.sql("select count(*) couunt, count(*) sample_count, absolute_error(couunt)," +
      "relative_error(couunt) from airline_view with error")
    assertAnalysis()

    rs2 = snc.sql(s"select count(*) couunt, count(*) sample_count, absolute_error(couunt)," +
      s" relative_error(couunt) from $airlineMainTable t1, $airlineRefTable t2 where " +
      s" t1.uniquecarrier = t2.code with error")
    assertAnalysis()

    row1 = rs1.collect()(0)
    row2 = rs2.collect()(0)
    assertEquals(row1.getLong(0), row2.getLong(0))
    assertEquals(row1.getLong(1), row2.getLong(1))
    assertEquals(row1.getDouble(2), row2.getDouble(2), 0d)
    assertEquals(row1.getDouble(3), row2.getDouble(3), 0d)
    snc.sql("drop view airline_view")
  }

  test("Verify position of bootstrap seed is above the filter for single sample table query") {
    val result = snc.sql(s"SELECT sum(l_quantity) as x, absolute_error(x) FROM $sampleTable " +
      s" where  l_partKey > 0")
    result.collect()
    val plan = result.queryExecution.executedPlan
    var filterSeen = false
    var bootstrapSeen = false
    plan.foreachUp {
      case _: FilterExec => filterSeen = true
      case pr: ProjectExec => assert(!pr.projectList.exists {
        case Alias(_, name) => if (name == IdentifySampledRelation.seedName) {
          bootstrapSeen = true
          bootstrapSeen
        } else {
          false
        }
        case _ => false
      } || filterSeen)

      case _ =>
    }
    assert(filterSeen)
    assert(bootstrapSeen)
  }

  test("Verify position of bootstrap seed is below the join for multi table query") {
    val result = snc.sql(s"SELECT avg(arrDelay) as x, absolute_error(x) " +
      s" FROM $airlineMainTable, $airlineRefTable " +
        s"where  code = UniqueCarrier with error $DEFAULT_ERROR confidence .95")
    result.collect()
    val plan = result.queryExecution.executedPlan

    var bootstrapSeen = false
    val checker: PartialFunction[SparkPlan, Unit] = {
      case pr: ProjectExec =>
        pr.projectList.foreach {
          case Alias(_, name) => if (name == IdentifySampledRelation.seedName) {
            bootstrapSeen = true
          }
          case _ =>
        }
      case _ =>
    }

    plan.foreachUp {
      case x: HashJoin =>
        x.left.foreach(checker)
        x.right.foreach(checker)
      case x: BroadcastNestedLoopJoinExec =>
        x.left.foreach(checker)
        x.right.foreach(checker)
      case x: SortMergeJoinExec =>
        x.left.foreach(checker)
        x.right.foreach(checker)
      case _ =>
    }

    if (!bootstrapSeen) {
      plan.foreach {
        pl: SparkPlan =>
          logInfo("class=" + pl.getClass.getName)
      }
    }

    assert(bootstrapSeen)
  }

  test("Verify position of bootstrap seed is below the join but above the " +
      "filter for multi table query") {
    val result = snc.sql(s"SELECT avg(arrDelay) as x, absolute_error(x) " +
        s"FROM $airlineMainTable, $airlineRefTable " +
        s"where  code = UniqueCarrier and tailnum > 0 " +
        s"with error $DEFAULT_ERROR confidence .95")
    result.collect()
    val plan = result.queryExecution.executedPlan
    var bootstrapSeen = false
    val checker: PartialFunction[SparkPlan, Unit] = {
      case pr: ProjectExec =>
        pr.projectList.foreach {
          case Alias(_, name) => if (name == IdentifySampledRelation.seedName) {
            assert(pr.child match {
              case _: FilterExec => true
              case _ => false
            })
            bootstrapSeen = true
          }
          case _ =>
        }
      case _ =>
    }

    plan.foreachUp {
      case x: HashJoin =>
        x.left.foreach(checker)
        x.right.foreach(checker)
      case x: BroadcastNestedLoopJoinExec =>
        x.left.foreach(checker)
        x.right.foreach(checker)
      case x: SortMergeJoinExec =>
        x.left.foreach(checker)
        x.right.foreach(checker)
      case _ =>
    }

    if (!bootstrapSeen) {
      plan.foreach {
        pl: SparkPlan =>
          logInfo("class=" + pl.getClass.getName)
      }
    }

    assert(bootstrapSeen)
  }

}

object Sum {

  def applySum(rows: Array[Row]): Row = {
    val currentSum = Array.ofDim[Any](rows(0).schema.length)
    rows.foreach {
      row => 0 until row.schema.length foreach { i =>
        currentSum(i) = currentSum(i).asInstanceOf[Float] + row.getFloat(i)
      }
    }
    new GenericRowWithSchema(currentSum, rows(0).schema)
  }
}

object Count {

  def applyCount(numBaseRows: Int, multiplicity: Array[Int], schemaToAppy: StructType): Row = {

    val count = multiplicity.map(_ * numBaseRows)
    var i = 0
    val totalCount = Array.fill[Any](multiplicity.length) {
      val retVal = count(i)
      i += 1
      retVal
    }
    new GenericRowWithSchema(totalCount, schemaToAppy)

  }
}

object Avg {

  def applyAvg(rows: Array[Row], multiplicity: Array[Int]): Row = {
    val summedRow = Sum.applySum(rows)
    val count = multiplicity.map(_ * rows.length)
    var i = 0
    val average = Array.fill[Any](multiplicity.length) {
      val retVal = summedRow.getFloat(i) / count(i)
      i += 1
      retVal
    }
    new GenericRowWithSchema(average, rows(0).schema)

  }
}
