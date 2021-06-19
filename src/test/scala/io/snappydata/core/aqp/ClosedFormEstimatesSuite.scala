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
package io.snappydata.core.aqp

import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.execution.common.{AQPRules, AnalysisType}
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.{AssertAQPAnalysis, SparkConf}

class ClosedFormEstimatesSuite
    extends SnappyFunSuite with BeforeAndAfterAll {

  private val ddlStr = "(YearI INT," + // NOT NULL
      "MonthI INT," + // NOT NULL
      "DayOfMonth INT," + // NOT NULL
      "DayOfWeek INT," + // NOT NULL
      "DepTime INT," +
      "CRSDepTime INT," +
      "ArrTime INT," +
      "CRSArrTime INT," +
      "UniqueCarrier VARCHAR(20)," + // NOT NULL
      "FlightNum INT," +
      "TailNum VARCHAR(20)," +
      "ActualElapsedTime INT," +
      "CRSElapsedTime INT," +
      "AirTime INT," +
      "ArrDelay INT," +
      "DepDelay INT," +
      "Origin VARCHAR(20)," +
      "Dest VARCHAR(20)," +
      "Distance INT," +
      "TaxiIn INT," +
      "TaxiOut INT," +
      "Cancelled INT," +
      "CancellationCode VARCHAR(20)," +
      "Diverted INT," +
      "CarrierDelay INT," +
      "WeatherDelay INT," +
      "NASDelay INT," +
      "SecurityDelay INT," +
      "LateAircraftDelay INT," +
      "ArrDelaySlot INT)"


  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    setupData()
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
  }

  override def afterAll(): Unit = {
    val snc = this.snc
    // cleanup metastore
    if (snc != null) {
      snc.clearCache()
    }
    super.afterAll()
    stopAll()
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "false")
  }

  protected override def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
    /**
     * Pls do not change the flag values of Property.TestDisableCodeGenFlag.name
     * and Property.UseOptimizedHashAggregateForSingleKey.name
     * They are meant to suppress CodegenFallback Plan so that optimized
     * byte buffer code path is tested & prevented from false passing.
     * If your test needs CodegenFallback, then override the newConf function
     * & clear the flag from the conf of the test locally.
     */
    new org.apache.spark.SparkConf().setAppName("ClosedFormEstimates")
        .setMaster("local[6]")
        .set("spark.logConf", "true")
        .set(Property.TestDisableCodeGenFlag.name , "true")
        .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")
  }

  def setupData(): Unit = {
    val hfile: String = getClass.getResource("/2015.parquet").getPath

    val snContext = this.snc
    val airlineDataFrame: DataFrame = snContext.read.load(hfile)
    airlineDataFrame.createOrReplaceTempView("airline")

    snc.sql(s"CREATE TABLE airlineSampled $ddlStr" +
        "USING column_sample " +
        "options " +
        "(" +
        "qcs 'UniqueCarrier' ," +
        "fraction '0.01'," +
        "strataReservoirSize '50'," +
        "baseTable 'airline')")

    airlineDataFrame.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable("airlineSampled")

  }

  test(
    "closed form estimation for avg aggregates with having clause referring to error estimates") {
    val results_sampled_table = snc.sql(
      """SELECT avg(ArrDelay) as T ,absolute_error(T) as AE,
        UniqueCarrier FROM airline GROUP BY
        UniqueCarrier order by UniqueCarrier WITH ERROR 0.12 confidence 0.9 """)

    logInfo("=============== APPROX RESULTS WITH AVG AGGREGATES ===============")

    // try/catch to proceed test in case of error exception
    // select only those rows whose absolute error is  > 0.9
    val totalRows = results_sampled_table.collect()
    val filteredRows = totalRows.filter(_.getDouble(1) > 0.9)
    assert(filteredRows.length <= totalRows.length)

    val results1_sampled_table = snc.sql(
      """SELECT avg(ArrDelay) as T ,absolute_error(T) ,
        UniqueCarrier FROM airline GROUP BY
        UniqueCarrier  having absolute_error(T) > 0.9 order by UniqueCarrier
        WITH ERROR 0.12 confidence 0.9""")

    val totalRowsHaving = results1_sampled_table.collect()
    AssertAQPAnalysis.closedFormAnalysis(snc)
    assert(totalRowsHaving.length == filteredRows.length)
    assert(totalRowsHaving === filteredRows)
    logInfo("Success..Test0")

  }

  test("error bounds with closed form estimation for avg aggregates type query on base table") {
    val results_sampled_table = snc.sql(
      """SELECT avg(ArrDelay) as T , lower_bound(T) LB, upper_bound(T),
        relative_error(T), absolute_error(T) as AE,
        UniqueCarrier FROM airline GROUP BY
        UniqueCarrier WITH ERROR 0.12 confidence 0.9""")

    logInfo("=============== APPROX RESULTS WITH AVG AGGREGATES ===============")
    try {
      // try/catch to proceed test in case of error exception
      val results = results_sampled_table.collect()
      logInfo(results.mkString("\n"))
      results.foreach(verifyResult)
      logInfo("Success..Test1")
    }
    catch {
      case rte: RuntimeException =>
        logInfo(rte.getMessage, rte)
        throw rte
    }
  }

  test("error bounds with closed form estimation for sum aggregate type query on base table") {
    val results_sampled_table = snc.sql(
      """SELECT sum(ArrDelay) as T , lower_bound(T) LB, upper_bound(T),
        relative_error(T), absolute_error(T) as AE,
        UniqueCarrier FROM airline GROUP BY
        UniqueCarrier WITH ERROR 0.12 confidence 0.95""")

    logInfo("=============== APPROX RESULTS SUM AGGREGATES ===============")
    try {
      // try/catch to proceed test in case of error exception
      val results = results_sampled_table.collect()
      logInfo(results.mkString("\n"))
      results.foreach(verifyResult)
      logInfo("Success..Test2")
    }
    catch {
      case rte: RuntimeException =>
        logInfo(rte.getMessage, rte)
        throw rte
    }
  }

  test("error bounds with closed form estimation for count aggregate type query on base table") {
    val results_sampled_table = snc.sql(
      """SELECT count(ArrDelay) as T , lower_bound(T) LB, upper_bound(T),
        relative_error(T), absolute_error(T) as AE,
        UniqueCarrier FROM airline GROUP BY
        UniqueCarrier WITH ERROR 0.12 confidence 0.95""")

    logInfo("=============== APPROX RESULTS COUNT AGGREGATES ===============")
    try {
      // try/catch to proceed test in case of error exception
      val results = results_sampled_table.collect()
      logInfo(results.mkString("\n"))
      results.foreach(verifyResult)
      logInfo("Success..Test2")
    }
    catch {
      case rte: RuntimeException =>
        logInfo(rte.getMessage, rte)
        throw rte
    }
  }

  test("error bounds with closed form estimation for query on sampled table") {
    val results_sampled_table = snc.sql(
      """SELECT avg(ArrDelay) as T , lower_bound(T) LB, upper_bound(T),
        relative_error(T), absolute_error(T) as AE,
        UniqueCarrier FROM airlineSampled GROUP BY
        UniqueCarrier with error .2 confidence .8""")

    logInfo("=============== APPROX RESULTS WITH DIRECT QUERY ON SAMPLE TABLE ===============")
    try {
      // try/catch to proceed test in case of error exception
      val results = results_sampled_table.collect()
      logInfo(results.mkString("\n"))
      results.foreach(verifyResult)
      logInfo("Success...Test3")
    }
    catch {
      case rte: RuntimeException =>
        logInfo(rte.getMessage, rte)
        throw rte
    }
  }

  test("error bounds with closed form estimation for query on table with HAC strict") {
    try {
      val results_sampled_table = snc.sql(
        """SELECT avg(ArrDelay) as T , lower_bound(T) LB, upper_bound(T),
        relative_error(T), absolute_error(T) as AE,
        UniqueCarrier FROM airlineSampled GROUP BY
        UniqueCarrier with error .2 confidence .8 behavior 'STRICT'""")

      results_sampled_table.collect()
      fail("should have got error limit exceeded exception")
    }
    catch {
      case e: Exception if e.toString.indexOf("ErrorLimitExceededException") != -1
      => logInfo("Success...Test4") // OK
    }
  }

  test("error bounds with closed form estimation for avg/sum query on table with HAC local_omit") {
    val results_sampled_table = snc.sql(
      """SELECT avg(ArrDelay) as A , sum(ArrDelay) as S,
        lower_bound(S) SUMLB, upper_bound(S) SUMUB,
        relative_error(A) AVGRE, absolute_error(A) AVGAE,
        UniqueCarrier FROM airline GROUP BY
        UniqueCarrier with error .2 confidence .8 behavior 'LOCAL_OMIT'""")

    logInfo("=============== APPROX RESULTS WITH DIRECT QUERY ON SAMPLE TABLE ===============")
    try {
      // try/catch to proceed test in case of error exception
      val results = results_sampled_table.collect()
      logInfo(results.mkString("\n"))
      var isNull: Boolean = false
      results.foreach(r => if (r.anyNull) isNull = true)
      assert(isNull)
      logInfo("Success...Test5")
    }
    catch {
      case rte: RuntimeException =>
        logInfo(rte.getMessage, rte)
        throw rte
    }
  }

  test("error bounds with closed form estimation for count query on table with HAC local_omit") {
    val results_sampled_table = snc.sql(
      """SELECT count(ArrDelay) as A ,relative_error(A) AVGRE,
        UniqueCarrier, MonthI, YearI FROM airline GROUP BY
        UniqueCarrier, MonthI, YearI with error .01 behavior 'LOCAL_OMIT'""")

    logInfo("=============== APPROX RESULTS WITH DIRECT QUERY ON SAMPLE TABLE ===============")
    try {
      // try/catch to proceed test in case of error exception
      val results = results_sampled_table.collect()
      logInfo(results.mkString("\n"))
      var isError: Boolean = false
      results.foreach(r => if (r.get(0) == -1) isError = true)
      assert(isError)
      logInfo("Success...Test6")
    }
    catch {
      case rte: RuntimeException =>
        logInfo(rte.getMessage, rte)
        throw rte
    }
  }

  test("test for correct error estimation method gets applied based on query type") {
    try {
      // For conditional average queries bootstrap should get applied
      var results_sampled_table = snc.sql(
        """SELECT avg(ArrDelay) as T ,
        UniqueCarrier, absolute_error(T) FROM airline WHERE UniqueCarrier = 'AA' GROUP BY
        UniqueCarrier WITH ERROR 0.12 confidence 0.9""")
      results_sampled_table.collect()

      val analysis = DebugUtils.getAnalysisApplied(
        results_sampled_table.queryExecution.executedPlan)
      assert(analysis match {
        case Some(AnalysisType.Bootstrap) => true
        case _ => false
      })

      // For average queries without any where clause, closed form should get applied
      results_sampled_table = snc.sql(
        """SELECT avg(ArrDelay) as T ,
        UniqueCarrier, absolute_error(T) FROM airline GROUP BY
        UniqueCarrier WITH ERROR 0.12 confidence 0.9""")

      val analysis1 = DebugUtils.getAnalysisApplied(
        results_sampled_table.queryExecution.executedPlan)
      assert(analysis1 match {
        case Some(AnalysisType.Closedform) => true
        case _ => false
      })

      logInfo("Success..Test7")
    }
    catch {
      case rte: RuntimeException =>
        logInfo(rte.getMessage, rte)
        throw rte
    }
  }


  def verifyResult(r: Row): Unit = {
    // logInfo(r)
    val diff = math.abs(r.getDouble(2) - r.getDouble(1)) / 2 - r.getDouble(4)
    assert(diff < 0.0001, diff) // (UB-LB)/2 ~ AE
  }
}
