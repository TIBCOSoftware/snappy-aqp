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

import org.junit.Assert.assertTrue

import org.apache.spark.sql.execution.common.{AQPInfo, AnalysisType}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{AssertAQPAnalysis, SparkConf}

/**
 * Created by ashahid on 5/11/16.
 */
class ClosedFormBugTest extends CommonBugTest {

  override val bugAQP214Asserter : ((Row, Row)) => Unit = {
    case (row1, row2) => {
      val totalCols = row1.length
      assert(Math.abs(row1.getLong(totalCols - 2) - row2.getLong(totalCols -2)) < 2)
      assert(row1.getDouble(totalCols - 1) === 0)
    }
  }

  val suppressTestAsserts : Set[Int] = Set[Int]()

  protected def addSpecificProps(conf: SparkConf): Unit = {
    conf.setAppName("ClosedFormBugTest")
        .set(io.snappydata.Property.ClosedFormEstimates.name, "true")
    // .set("spark.master", "local[8]")
    // .set(Constants.keyNumBootStrapTrials, "4")
  }

  def assertAnalysis(position: Int = 0): Unit = {
    AssertAQPAnalysis.closedFormAnalysis(this.snc, position)
  }

  test("Bug AQP211: A table with weight column should be treated as a sample table") {
    val testNo = 31
    val nameTestTable = "TestSample"
    val scaledUpCount = snc.sql(s"select count(*) from $sampleAirlineUniqueCarrier").collect()(0)
      .getLong(0)
    val  sampleResult = snc.sql(s"select * from $sampleAirlineUniqueCarrier")
    val testTable = snc.createTable(nameTestTable, "column", sampleResult.schema,
      Map.empty[String, String])
    sampleResult.write.mode(SaveMode.Append).saveAsTable(nameTestTable)
    var testTableCount = snc.sql(s"select count(*) from $nameTestTable ").collect()(0)
      .getLong(0)
    assert(Math.abs(scaledUpCount - testTableCount) < 2)

    val result1 = snc.sql(s"select count(*) as x, absolute_error(x) from $nameTestTable ")
      .collect()
    AssertAQPAnalysis.bootStrapAnalysis(snc)
    testTableCount = result1(0).getLong(0)
    var absError = result1(0).getDouble(1)
    assert(Math.abs(scaledUpCount - testTableCount) < 2)
    // For a sampled data contained in normal table, there is no gurantee that a stratum will
    // be completely seen by the same aggregator, so we do not use closedfrom analysis
    // assert(absError == 0)

    val scaledUpSum = snc.sql(s"select Sum(ArrDelay) from $sampleAirlineUniqueCarrier").collect()(0)
      .getLong(0)
    val result2 = snc.sql(s"select sum(ArrDelay) as x, absolute_error(x) from $nameTestTable ")
      .collect()
    AssertAQPAnalysis.bootStrapAnalysis(snc)
    val testTableSum = result2(0).getLong(0)
    absError = result2(0).getDouble(1)
    assert(Math.abs(scaledUpSum - testTableSum) < 2)
    assert(absError > 0)


    val scaledUpAvg = snc.sql(s"select Avg(ArrDelay) from $sampleAirlineUniqueCarrier").collect()(0)
      .getDouble(0)
    val result3 = snc.sql(s"select Avg(ArrDelay) as x, absolute_error(x) from $nameTestTable ")
      .collect()
    AssertAQPAnalysis.bootStrapAnalysis(snc)
    val testTableAvg = result3(0).getDouble(0)
    absError = result3(0).getDouble(1)
    assert(Math.abs(scaledUpAvg - testTableAvg) < 2)
    assert(absError > 0)

  }

  def assertAqpInfo(buffer: scala.collection.mutable.ArrayBuffer[Array[AQPInfo]]): Unit = {
    assertTrue(buffer.exists(arr => arr.length > 0 && (arr(0).analysisType match {
      case Some(AnalysisType.Closedform) => true
      case _ => false
    })))
  }
}
