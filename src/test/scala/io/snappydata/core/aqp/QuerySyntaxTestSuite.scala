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

import io.snappydata.{Property, SnappyFunSuite}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.execution.common.{AQPRules, HAC}
import org.apache.spark.sql.{DataFrame, SaveMode}

class QuerySyntaxTestSuite
  extends SnappyFunSuite
  with BeforeAndAfterAll {

    val error = 0.5d
    val confidence = 0.99d
    var behavior = "local_omit"
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val ddlStr: String = "(YearI INT," + // NOT NULL
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
      setupData()
      AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
     // System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
    }

    override def afterAll(): Unit = {
      val snc = this.snc
      // cleanup metastore
      if (snc != null) {
        snc.clearCache()
      }
      super.afterAll()
      stopAll()
      // System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "false")
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
          .set("spark.sql.aqp.error", s"$error")
          .set("spark.sql.aqp.confidence", s"$confidence")
          .set(Property.TestDisableCodeGenFlag.name , "true")
          .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")
    }

    def setupData(): Unit = {

      val snContext = this.snc
      snContext.sql("set spark.sql.shuffle.partitions=6")
      snContext.sql(s"set spark.sql.aqp.behavior=$behavior")
      val airlineDataFrame: DataFrame = snContext.read.load(hfile)
      airlineDataFrame.createOrReplaceTempView("airline")

      snc.sql(s"CREATE TABLE airlineSampled $ddlStr" +
          "USING column_sample " +
          "options " +
          "(" +
          "qcs 'UniqueCarrier' ," +
          "fraction '0.03'," +
          "strataReservoirSize '50', " +
          "baseTable 'airline')")

      airlineDataFrame.write.format("column_sample").mode(SaveMode.Append)
          .saveAsTable("airlineSampled")
    }

  test("Error clause is not needed if error set on connection") {
    snc.sql(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier confidence 0.5 """)

    val results = snc.sql(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier""")
    assert(DebugUtils.getAnyErrorAggregateFunction(results.queryExecution.executedPlan).isDefined)
  }

  test("Error clause is mandatory if not set on connection and only confidence specified") {
    val newSnc = snc.newSession()
    newSnc.setConf("spark.sql.aqp.error", "-1d")
    val airlineDataFrame: DataFrame = newSnc.read.load(hfile)
    airlineDataFrame.createOrReplaceTempView("airline")
    try {
      newSnc.sql(
        """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier confidence 0.5 """)
    } catch {
          case e: UnsupportedOperationException =>
             assert(e.toString.indexOf("With error clause is mandatory") != -1, e.toString)
              logInfo("Success...Test0") // OK
    }
  }

  test("Negative error clause set on context should not use aqp") {
    val newSnc = snc.newSession()
    newSnc.setConf("spark.sql.aqp.error", "-1d")
    val airlineDataFrame: DataFrame = newSnc.read.load(hfile)
    airlineDataFrame.createOrReplaceTempView("airline")

    val results = newSnc.sql(
        """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier confidence .2d""")
    assert(DebugUtils.getAnyErrorAggregateFunction(results.queryExecution.executedPlan).isEmpty)
  }

  test("Error clause is mandatory only behavior specified") {
    snc.sql(
      """SELECT avg(ArrDelay) UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier behavior 'do_nothing' """)
  }

  test("Error value should be in between 0 and 1"){
    try {
      snc.sql(
        """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
          UniqueCarrier order by UniqueCarrier with error 1.3 confidence 0.9 """)
      fail("should have got error within range of 0 to 1")
    } catch {
      case e: UnsupportedOperationException =>
        assert(e.toString.indexOf("error within range of 0 to 1") != -1, e.toString)
        logInfo("Success...Test2") // OK
    }

    try {
      snc.sql(
        """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
        UniqueCarrier order by UniqueCarrier with error 1 confidence 0.9 """)
      fail("should have got error within range of 0 to 1")
    } catch {
      case e: UnsupportedOperationException =>
        assert(e.toString.indexOf("error within range of 0 to 1") != -1, e.toString)
        logInfo("Success...Test3") // OK
    }


    try {
      snc.sql(
        """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
      UniqueCarrier order by UniqueCarrier with error 0 confidence 0.9 """)
      fail("should have got error within range of 0 to 1")
    } catch {
      case e: UnsupportedOperationException =>
        assert(e.toString.indexOf("error within range of 0 to 1") != -1, e.toString)
        logInfo("Success...Test4") // OK
    }

      // a query with -ve error should operate on base table,
     // but if the table is a sample table explictly mentioned in query then it should just
     // operate on it

     val results = snc.sql(
          """SELECT avg(ArrDelay), UniqueCarrier FROM airlineSampled GROUP BY
      UniqueCarrier order by UniqueCarrier with error -1.3 confidence 0.9 """)
  }

   test("Confidence value should be in between 0 and 1"){
     try {
       snc.sql(
         """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
          UniqueCarrier order by UniqueCarrier with error 0.3 confidence 1.2 """)
       fail("should have got confidence within range of 0 to 1")
     } catch {
       case e: UnsupportedOperationException =>
         assert(e.toString.indexOf("confidence within range of 0 to 1") != -1, e.toString)
         logInfo("Success...Test6") // OK
     }


     try {
       snc.sql(
         """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
        UniqueCarrier order by UniqueCarrier with error 0.3 confidence -0.9 """)
       fail("should have got confidence within range of 0 to 1")
     } catch {
       case e: Exception =>
         assert(e.toString.indexOf("confidence within range of 0 to 1") != -1, e.toString)
         logInfo("Success...Test7") // OK
     }


     try {
       snc.sql(
         """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
      UniqueCarrier order by UniqueCarrier with error 0.3 confidence 0.0 """)
       fail("should have got confidence within range of 0 to 1")
     } catch {
       case e: Exception =>
         assert(e.toString.indexOf("confidence within range of 0 to 1") != -1, e.toString)
         logInfo("Success...Test8") // OK
     }


     try {
       snc.sql(
         """SELECT avg(ArrDelay), UniqueCarrier FROM airlineSampled GROUP BY
      UniqueCarrier order by UniqueCarrier with error 0.3 confidence 1.0 """)
       fail("should have got confidence within range of 0 to 1")
     } catch {
       case e: Exception =>
         assert(e.toString.indexOf("confidence within range of 0 to 1") != -1, e.toString)
         logInfo("Success...Test9")
     }
   }

  test("Values set in the context should get used for approx queries") {
    val results = snc.sql(
      """SELECT avg(ArrDelay) ad, relative_error(ad), UniqueCarrier FROM airline GROUP BY
        UniqueCarrier order by UniqueCarrier with error""")
    results.collect()
    val errorAggFunc = DebugUtils.getAnyErrorAggregateFunction(results.queryExecution.executedPlan)

    assert(errorAggFunc.get.confidence === confidence)
    assert(errorAggFunc.get.error === error)
    assert(HAC.getBehaviorAsString(errorAggFunc.get.behavior).equalsIgnoreCase(behavior))

    logInfo("Success...Test10")
  }

  test("Values set in the context should get used for properties " +
      "not set in query clause for approx queries") {

    behavior = "LOCAL_OMIT"
    snc.sql(s"set spark.sql.aqp.behavior=$behavior")

    val results = snc.sql(
      """SELECT avg(ArrDelay) ad, relative_error(ad), UniqueCarrier FROM airline GROUP BY
        UniqueCarrier order by UniqueCarrier with error 0.2 """)
    results.collect()
    val errorAggFunc = DebugUtils.getAnyErrorAggregateFunction(results.queryExecution.executedPlan)

    assert(errorAggFunc.get.confidence === confidence)
    assert(errorAggFunc.get.error === 0.2)
    assert(HAC.getBehaviorAsString(errorAggFunc.get.behavior).equalsIgnoreCase(behavior))

    logInfo("Success...Test11")
  }

  test("Properties set in the query has precedence over context") {
    val results = snc.sql(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
        UniqueCarrier order by UniqueCarrier with error 0.8 confidence 0.90 """)
    results.collect()
    val errorAggFunc = DebugUtils.getAnyErrorAggregateFunction(results.queryExecution.executedPlan)

    assert(errorAggFunc.get.confidence === 0.90)
    assert(errorAggFunc.get.error === 0.8)
    assert(HAC.getBehaviorAsString(errorAggFunc.get.behavior).equalsIgnoreCase(behavior))

    logInfo("Success...Test12")
  }
}
