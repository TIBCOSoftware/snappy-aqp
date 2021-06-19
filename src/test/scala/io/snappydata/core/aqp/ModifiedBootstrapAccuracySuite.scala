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

import java.io.{DataInputStream, FileInputStream, InputStream}

import scala.util.Sorting
import scala.util.control.Breaks._

import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.apache.commons.math3.distribution.NormalDistribution
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{AssertAQPAnalysis, SparkConf}

/**
 * Created by supriya on 29/4/16.
 */
class ModifiedBootstrapAccuracySuite extends SnappyFunSuite
with BeforeAndAfterAll{

  private val disabled = true
  val bootStrapTrials = 100 // default
  val confidence = 0.95
  val confFactor = new NormalDistribution().inverseCumulativeProbability(0.5 + confidence / 2.0)
  val whereConditionM: String = s" MonthI > 0 "
  val whereConditionY: String = s" YearI > 1994 "
  var qcsColumns: String = "UniqueCarrier" // Same as QCS
  var qcs: String = "START"
  var currQCS: String = ""
  val strataReserviorSize = 50
  var results_sampled_table: DataFrame = _
  var airlineDataFrame: DataFrame = _
  var sampleTableCreated = false
  val useClosedForm = true

  val schema = StructType(Seq(
    StructField("YEARI", IntegerType, nullable = true),
    StructField("MONTHI", IntegerType, nullable = true),
    StructField("DAYOFMONTH", IntegerType, nullable = true),
    StructField("DAYOFWEEK", IntegerType, nullable = true),
    StructField("UNIQUECARRIER", StringType, nullable = true),
    StructField("TAILNUM", StringType, nullable = true),
    StructField("FLIGHTNUM", IntegerType, nullable = true),
    StructField("ORIGIN", StringType, nullable = true),
    StructField("DEST", StringType, nullable = true),
    StructField("CRSDEPTIME", IntegerType, nullable = true),
    StructField("DEPTIME", IntegerType, nullable = true),
    StructField("DEPDELAY", DoubleType, nullable = true),
    StructField("TAXIOUT", DoubleType, nullable = true),
    StructField("TAXIIN", DoubleType, nullable = true),
    StructField("CRSARRTIME", StringType, nullable = true),
    StructField("ARRTIME", StringType, nullable = true),
    StructField("ARRDELAY", DoubleType, nullable = true),
    StructField("CANCELLED", StringType, nullable = true),
    StructField("CANCELLATIONCODE", StringType, nullable = true),
    StructField("DIVERTED", DoubleType, nullable = true),
    StructField("CRSELAPSEDTIME", DoubleType, nullable = true),
    StructField("ACTUALELAPSEDTIME", DoubleType, nullable = true),
    StructField("AIRTIME", DoubleType, nullable = true),
    StructField("DISTANCE", DoubleType, nullable = true),
    StructField("CARRIERDELAY", StringType, nullable = true),
    StructField("WEATHERDELAY", StringType, nullable = true),

    StructField("NASDELAY", StringType, nullable = true),
    StructField("SECURITYDELAY", StringType, nullable = true),

    StructField("LATEAIRCRAFTDELAY", StringType, nullable = true),
    StructField("ARRDELAYSLOT", StringType, nullable = true))



  )

  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    /**
     * Setting local[n] here actually supposed to affect number of reservoir created
     * while sampling.
     *
     * Change of 'n' will influence results if they are dependent on weights - derived
     * from hidden column in sample table.
     */
    /**
     * Pls do not change the flag values of Property.TestDisableCodeGenFlag.name
     * and Property.UseOptimizedHashAggregateForSingleKey.name
     * They are meant to suppress CodegenFallback Plan so that optimized
     * byte buffer code path is tested & prevented from false passing.
     * If your test needs CodegenFallback, then override the newConf function
     * & clear the flag from the conf of the test locally.
     */
    new org.apache.spark.SparkConf().setAppName("BootStrapAccuracySuite")
        .setMaster("local[6]")
        .set("spark.logConf", "true")
      .set(Property.ClosedFormEstimates.name, useClosedForm.toString).
      set(Property.NumBootStrapTrials.name, s"$bootStrapTrials")
      .set(Property.TestDisableCodeGenFlag.name , "true")
      .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")

    // .set("spark.executor.instances", "8")
  }


  val ddlStr: String = "( YearI INT," + // NOT NULL
      "MonthI INT," + // NOT NULL
      "DayOfMonth INT," + // NOT NULL
      "DayOfWeek INT," + // NOT NULL
      "UniqueCarrier VARCHAR(20)," + // NOT NULL
      "TailNum VARCHAR(20)," +
      "FlightNum INT," +
      "Origin VARCHAR(20)," +
      "Dest VARCHAR(20)," +
      "CRSDepTime INT," +
      "DepTime INT," +
      "DepDelay Double," +
      "TaxiOut Double," +
      "TaxiIn Double," +
      "CRSArrTime VARCHAR(30)," +
      "ArrTime VARCHAR(30)," +
      "ArrDelay Double," +
      "Cancelled VARCHAR(30)," +
      "CancellationCode VARCHAR(20)," +
      "Diverted Double," +
      "CRSElapsedTime Double," +
      "ActualElapsedTime Double," +
      "AirTime Double," +
      "Distance Double," +
      "CarrierDelay VARCHAR(30)," +
      "WeatherDelay VARCHAR(30)," +
      "NASDelay VARCHAR(30)," +
      "SecurityDelay VARCHAR(30)," +
      "LateAircraftDelay VARCHAR(30)," +
      "ArrDelaySlot VARCHAR(30)) "


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

  def setupData(): Unit = {
    if (disabled) return
    // 1995-2015_ParquetData
    // val hfile: String = getClass.getResource("/2015.parquet").getPath
    val hfile: String = getClass.getResource("/sample.csv").getPath
    val snContext = this.snc
    snContext.sql("set spark.sql.shuffle.partitions=6")
    logInfo("about to read sample.csv")
    airlineDataFrame = snContext.read.format("com.databricks.spark.csv").load(hfile)
        .toDF("YEARI", "MonthI", "DayOfMonth",
          "DayOfWeek", "UniqueCarrier", "TailNum", "FlightNum", "Origin", "Dest", "CRSDepTime",
          "DepTime", "DepDelay", "TaxiOut", "TaxiIn", "CRSArrTime", "ArrTime", "ArrDelay",
          "Cancelled", "CancellationCode", "Diverted", "CRSElapsedTime",
          "ActualElapsedTime", "AirTime", "Distance", "CarrierDelay", "WeatherDelay", "NASDelay",
          "SecurityDelay", "LateAircraftDelay", "ArrDelaySlot", "SNAPPY_SAMPLER_WEIGHTAGE")
    airlineDataFrame.createOrReplaceTempView("airline")
    logInfo("schema of main table is " + airlineDataFrame.schema)

    val rs1 = snc.sql(s"SELECT ArrDelay  FROM airline" +
        s" where $whereConditionM ")
    rs1.collect()

    val rs2 = snc.sql(s"SELECT count(*)  FROM airline")
    logInfo("total number of rows in main table = " + rs2.collect()(0).getLong(0))

    /**
     * Total number of executors (or partitions) = Total number of buckets +
     *                                             Total number of reservoir
     */
    /**
     * Partition by influences whether a strata is handled in totality on a
     * single executor or not, and thus an important factor.
     */
    logInfo("creating sample table")
    snc.sql(s"CREATE TABLE airlineSampled $ddlStr" +
        "USING column_sample " +
        "options " +
        "(" +
        s"qcs '$qcsColumns'," +
        "BUCKETS '4'," +
        s"${Constants.keyBypassSampleOperator} 'true'," +
        s"strataReservoirSize '$strataReserviorSize', " +
        "baseTable 'airline')")

    logInfo("loading data into  sample table")
    /* airlineDataFrame.write.
        format("column_sample")
        .mode(SaveMode.Append)
        .saveAsTable("airlineSampled") */
    airlineDataFrame.write.insertInto("airlineSampled")

    val rs = snc.sql(s"SELECT ArrDelay  FROM airlineSampled" +
        s" where $whereConditionM ")
    rs.collect()

    val rs3 = snc.sql("SELECT count(*) as sample_count  FROM airlineSampled")
    val rows3 = rs3.collect()
    logInfo("actual number of rows ins sampled table =" + rows3(0).getLong(0))
    sampleTableCreated = true
  }

  def writeSampleTable(): Unit = {
    airlineDataFrame.write.insertInto("airlineSampled")
  }

  def deleteSampleTable(): Unit = {
    snc.sql(s"TRUNCATE TABLE airlineSampled")
  }

  def reCreateSampleTable(): Unit = {
    snc.sql(s"CREATE SAMPLE TABLE airlineSampled " +
        " options " +
        " ( " +
        s"qcs '$qcsColumns'," +
        "BUCKETS '4'," +
        s"${Constants.keyBypassSampleOperator} 'true'," +
        s"strataReservoirSize '$strataReserviorSize' " +
        " ) " +
        "AS ( " +
        "SELECT * " +
        " FROM AIRLINE)")
  }

  def dropSampleTable(): Unit = {
    snc.sql(s"DROP TABLE if exists airlineSampled")
  }

  def warmUp(): Unit = {
    // Change the sampleRation accordingly
    logInfo("starting warm up")

    val resultMonth_Sum =
      snc.sql(s"SELECT SUM(ArrDelay) as sum, lower_bound(sum), upper_bound(sum) FROM airline" +
          s" where $whereConditionM confidence " + confidence)

    resultMonth_Sum.collect()



    val resultYear_Sum =
      snc.sql(s"SELECT SUM(ArrDelay) as sum, lower_bound(sum), upper_bound(sum)  FROM airline" +
          s" where $whereConditionY confidence " + confidence)
    resultYear_Sum.collect()

    val resultMonth_Avg =
      snc.sql(s"SELECT AVG(ArrDelay) as AVG, lower_bound(AVG),upper_bound(AVG) FROM" +
          s" airline where $whereConditionM confidence " + confidence)
    resultMonth_Avg.collect()

    val resultYear_Avg =
      snc.sql(s"SELECT AVG(ArrDelay) as AVG, lower_bound(AVG), upper_bound(AVG) FROM airline" +
          s" where $whereConditionY confidence " + confidence)
    resultYear_Avg.collect()

    val resultMonth_Count =
      snc.sql(s"SELECT COUNT(ArrDelay) as count, lower_bound(count), upper_bound(count) " +
          s"FROM airline where $whereConditionM confidence " + confidence)
    resultMonth_Count.collect()

    val resultYear_Count =
      snc.sql(s"SELECT COUNT(ArrDelay) as count, lower_bound(count),upper_bound(count) FROM" +
          s" airline where $whereConditionY confidence " + confidence)
    resultYear_Count.collect()

    val x =
      snc.sql(s"SELECT COUNT(ArrDelay) as count, sum(ArrDelay) as sum, lower_bound(count), " +
          s"upper_bound(count) FROM airline where $whereConditionM confidence " + confidence)
    x.collect()

    val y =
      snc.sql(s"SELECT COUNT(ArrDelay) as count, sum(ArrDelay) as sum, lower_bound(count), " +
          s"upper_bound(count)  FROM airline where $whereConditionY confidence " + confidence)
    y.collect()
    logInfo("warm up end")

    // deleteSampleTable()
  }

  def testBounds(): Unit = {

    val numloop : Integer = 150
    var lbMonth_sum, lbYear_sum, lbMonth_avg, lbYear_avg, lbMonth_cnt, lbYear_cnt = 0.0d
    var ubMonth_sum, ubYear_sum, ubMonth_avg, ubYear_avg, ubMonth_cnt, ubYear_cnt = 0.0d
    val cumulativeTime = Array.fill[Long](12)(0)

    if(!sampleTableCreated) {
      writeSampleTable()
    }
    this.warmUp()
    // writeSampleTable()
    for (i <- 0 until numloop) {
      try {
        // Change the sampleRation accordingly
        // writeSampleTable()
        var startTime, endTime: Long = 0

        startTime = System.currentTimeMillis()
        val resultMonth_Sum =
          snc.sql(s"SELECT SUM(ArrDelay) as sum, lower_bound(sum), upper_bound(sum) FROM airline" +
              s" where $whereConditionM confidence " + confidence)
        // resultMonth_Sum.queryExecution.executedPlan
        // var t = System.currentTimeMillis()
        // println("Time taken to prepare the plan q1= "+ (t - startTime))
        val rowMonth_sum = resultMonth_Sum.collect()
        assertAnalysis()
        val sumOnMonth = rowMonth_sum(0).getDouble(0)
        val lowerBound_MS = rowMonth_sum(0).getDouble(1)
        val upperBound_MS = rowMonth_sum(0).getDouble(2)
        endTime = System.currentTimeMillis()
        cumulativeTime(0) += (endTime - startTime)
        lbMonth_sum += lowerBound_MS
        ubMonth_sum += upperBound_MS


        startTime = System.currentTimeMillis()
        val resultYear_Sum =
          snc.sql(s"SELECT SUM(ArrDelay) as sum, lower_bound(sum), upper_bound(sum)  FROM airline" +
              s" where $whereConditionY confidence " + confidence)
        // resultYear_Sum.queryExecution.executedPlan
        // t = System.currentTimeMillis()
        //  println("Time taken to prepare the plan q2= "+ (t - startTime))
        val rowYear_sum = resultYear_Sum.collect()
        assertAnalysis()
        val sumOnYear = rowYear_sum(0).getDouble(0)
        val lowerBound_YS = rowYear_sum(0).getDouble(1)
        val upperBound_YS = rowYear_sum(0).getDouble(2)
        endTime = System.currentTimeMillis()
        cumulativeTime(1) += (endTime - startTime)

        lbYear_sum += lowerBound_YS
        ubYear_sum += upperBound_YS

        startTime = System.currentTimeMillis()
        val resultMonth_Avg =
          snc.sql(s"SELECT AVG(ArrDelay) as AVG, lower_bound(AVG),upper_bound(AVG) FROM" +
              s" airline where $whereConditionM confidence " + confidence)
        val rowMonth_Avg = resultMonth_Avg.collect()
        AssertAQPAnalysis.bootStrapAnalysis(this.snc)
        val avgOnMonth = rowMonth_Avg(0).getDouble(0)
        val lowerBound_MA = rowMonth_Avg(0).getDouble(1)
        val upperBound_MA = rowMonth_Avg(0).getDouble(2)
        endTime = System.currentTimeMillis()
        cumulativeTime(2) += (endTime - startTime)

        lbMonth_avg += lowerBound_MA
        ubMonth_avg += upperBound_MA

        startTime = System.currentTimeMillis()
        val resultYear_Avg =
          snc.sql(s"SELECT AVG(ArrDelay) as AVG, lower_bound(AVG), upper_bound(AVG) FROM airline" +
              s" where $whereConditionY confidence " + confidence)
        val rowYear_Avg = resultYear_Avg.collect()
        AssertAQPAnalysis.bootStrapAnalysis(this.snc)
        val avgOnYear = rowYear_Avg(0).getDouble(0)
        val lowerBound_YA = rowYear_Avg(0).getDouble(1)
        val upperBound_YA = rowYear_Avg(0).getDouble(2)
        endTime = System.currentTimeMillis()
        cumulativeTime(3) += (endTime - startTime)

        lbYear_avg += lowerBound_YA
        ubYear_avg += upperBound_YA

        startTime = System.currentTimeMillis()
        val resultMonth_Count =
          snc.sql(s"SELECT COUNT(ArrDelay) as count, lower_bound(count), upper_bound(count) " +
              s"FROM airline where $whereConditionM confidence " + confidence)
        val rowMonth_Cnt = resultMonth_Count.collect()
        assertAnalysis()
        val cntOnmonth = rowMonth_Cnt(0).getLong(0)
        val lowerBound_MC = rowMonth_Cnt(0).getDouble(1)
        val upperBound_MC = rowMonth_Cnt(0).getDouble(2)
        endTime = System.currentTimeMillis()
        cumulativeTime(4) += (endTime - startTime)

        lbMonth_cnt += lowerBound_MC
        ubMonth_cnt += upperBound_MC


        startTime = System.currentTimeMillis()
        val resultYear_Count =
          snc.sql(s"SELECT COUNT(ArrDelay) as count, lower_bound(count),upper_bound(count) FROM" +
              s" airline where $whereConditionY confidence " + confidence)
        val rowYear_Cnt = resultYear_Count.collect()
        assertAnalysis()
        val cntOnYear = rowYear_Cnt(0).getLong(0)
        val lowerBound_YC = rowYear_Cnt(0).getDouble(1)
        val upperBound_YC = rowYear_Cnt(0).getDouble(2)
        endTime = System.currentTimeMillis()
        cumulativeTime(5) += (endTime - startTime)

        lbYear_cnt += lowerBound_YC
        ubYear_cnt += upperBound_YC


        startTime = System.currentTimeMillis()
        val x =
          snc.sql(s"SELECT COUNT(ArrDelay) as count, sum(ArrDelay) as sum, lower_bound(count), " +
              s"upper_bound(count)  FROM airline where $whereConditionM confidence " + confidence)
        x.collect()
        assertAnalysis()

        endTime = System.currentTimeMillis()
        cumulativeTime(6) += (endTime - startTime)

        startTime = System.currentTimeMillis()
        val y =
          snc.sql(s"SELECT COUNT(ArrDelay) as count, sum(ArrDelay) as sum, lower_bound(count), " +
              s"upper_bound(count)  FROM airline where $whereConditionY confidence " + confidence)
        y.collect()
        assertAnalysis()

        endTime = System.currentTimeMillis()
        cumulativeTime(7) += (endTime - startTime)


        startTime = System.currentTimeMillis()
        val z =
          snc.sql(s"SELECT avg(ArrDelay) as avg, sum(ArrDelay) as sum, lower_bound(avg), " +
              s"upper_bound(avg)  FROM airline where $whereConditionY confidence " + confidence)
        z.collect()
        AssertAQPAnalysis.bootStrapAnalysis(this.snc)

        endTime = System.currentTimeMillis()
        cumulativeTime(8) += (endTime - startTime)


        startTime = System.currentTimeMillis()
        val a =
          snc.sql(s"Select sum(ArrDelay) as x , absolute_error(x),relative_error(x) from airline " +
              s" confidence " + confidence)
        a.collect()
        assertAnalysis()

        endTime = System.currentTimeMillis()
        cumulativeTime(9) += (endTime - startTime)


        startTime = System.currentTimeMillis()
        val b =
          snc.sql(s" Select avg(ArrDelay) as x , absolute_error(x),relative_error(x) " +
              s"from airline confidence " + confidence)
        b.collect()
        assertAnalysis()

        endTime = System.currentTimeMillis()
        cumulativeTime(10) += (endTime - startTime)

        startTime = System.currentTimeMillis()
        val c =
          snc.sql(s" Select count(ArrDelay) as x , absolute_error(x),relative_error(x) " +
              s"from airline confidence " + confidence)
        c.collect()
        assertAnalysis()

        endTime = System.currentTimeMillis()
        cumulativeTime(11) += (endTime - startTime)
        // deleteSampleTable()

        // Printing point estimates
        if (i > 0 && (i == 5 || i == 10 || (i % 10 == 0))) {
          logInfo("SUM with condition on Month  for [" + i + "] iterration = " + sumOnMonth)
          logInfo("LowerBound = " + lowerBound_MS + " UpperBound = " + upperBound_MS)
          logInfo("deviation LowerBound = " + (3436109.45 - lowerBound_MS) +
              " deviation UpperBound = " + (9038381.33 - upperBound_MS))
          logInfo("Average query execution time so far = " +
              cumulativeTime(0).toFloat / (i.toFloat + 1))
          logInfo("")
          logInfo("SUM with condition On Year for [" + i + "] iterration = " + sumOnYear)
          logInfo("LowerBound = " + lowerBound_YS + " UpperBound = " + upperBound_YS)
          logInfo("deviation LowerBound = " + (6207218.63 - lowerBound_YS) +
              " deviation UpperBound = " + (14138164.56 - upperBound_YS))
          logInfo("Average query execution time so far = " +
              cumulativeTime(1).toFloat / (i.toFloat + 1))
          logInfo("")
          logInfo("Avg with condition On Month for [" + i + "] iterration  = " +
              " %.2f".format(avgOnMonth))
          logInfo("LowerBound = " + lowerBound_MA + " UpperBound = " + upperBound_MA)
          logInfo("deviation LowerBound = " + (3.82 - lowerBound_MA) +
              " deviation UpperBound = " + (10.03 - upperBound_MA))
          logInfo("Average query execution time so far = " +
              cumulativeTime(2).toFloat / (i.toFloat + 1))
          logInfo("")
          logInfo("Avg with condition On Year for [" + i + "] iterration = " +
              " %.2f".format(avgOnYear))
          logInfo("LowerBound = " + lowerBound_YA + " UpperBound = " + upperBound_YA)
          logInfo("deviation LowerBound = " + (3.28 - lowerBound_YA) +
              " deviation UpperBound = " + (7.48 - upperBound_YA))
          logInfo("Average query execution time so far = " +
              cumulativeTime(3).toFloat / (i.toFloat + 1))
          logInfo("")
          logInfo("Count with condition On Month for [" + i + "] iterration = " + cntOnmonth)
          logInfo("LowerBound = " + lowerBound_MC + " UpperBound = " + upperBound_MC)
          logInfo("deviation LowerBound = " + (855744 - lowerBound_MC) +
              " deviation UpperBound = " + (942222 - upperBound_MC))
          logInfo("Average query execution time so far = " +
              cumulativeTime(4).toFloat / (i.toFloat + 1))
          logInfo("")
          logInfo("Count with condition On Year for [" + i + "] iterration = " + cntOnYear)
          logInfo("LowerBound = " + lowerBound_YC + " UpperBound = " + upperBound_YC)
          logInfo("deviation LowerBound = " + (1888612 - lowerBound_YC) +
              " deviation UpperBound = " + (1888618 - upperBound_YC))
          logInfo("Average query execution time so far = " +
              cumulativeTime(5).toFloat / (i.toFloat + 1))

          logInfo("")
          logInfo("Count and sum condition On Month for [" + i + "] iterration ")
          logInfo("Average query execution time so far = " +
              cumulativeTime(6).toFloat / (i.toFloat + 1))

          logInfo("")
          logInfo("Count and sum with condition On Year for [" + i + "]")
          logInfo("Average query execution time so far = " +
              cumulativeTime(7).toFloat / (i.toFloat + 1))

          logInfo("")
          logInfo("avg and sum with condition On Year for [" + i + "]")
          logInfo("Average query execution time so far = " +
              cumulativeTime(8).toFloat / (i.toFloat + 1))

          logInfo("")
          logInfo(" sum with no condition  for [" + i + "]")
          logInfo("Average query execution time so far = " +
              cumulativeTime(9).toFloat / (i.toFloat + 1))

          logInfo("")
          logInfo(" avg with no condition  for [" + i + "]")
          logInfo("Average query execution time so far = " +
              cumulativeTime(10).toFloat / (i.toFloat + 1))

          logInfo("")
          logInfo(" count with no condition  for [" + i + "]")
          logInfo("Average query execution time so far = " +
              cumulativeTime(11).toFloat / (i.toFloat + 1))
          logInfo("***********************************************")
        }
      } catch {
        case e: Exception =>
          val meanLBonMonth_sum = lbMonth_sum / (i + 1)
          val meanUBonMonth_sum = ubMonth_sum / (i + 1)
          val meanLBonYear_sum = lbYear_sum / (i + 1)
          val meanUBonYear_sum = ubYear_sum / (i + 1)

          val meanLBonMonth_avg = lbMonth_avg / (i + 1)
          val meanUBonMonth_avg = ubMonth_avg / (i + 1)
          val meanLBonYear_avg = lbYear_avg / (i + 1)
          val meanUBonYear_avg = ubYear_avg / (i + 1)

          val meanLBonMonth_cnt = lbMonth_cnt / (i + 1)
          val meanUBonMonth_cnt = ubMonth_cnt / (i + 1)
          val meanLBonYear_cnt = lbYear_cnt / (i + 1)
          val meanUBonYear_cnt = ubYear_cnt / (i + 1)

          logInfo("================BootStrap LowerBound UpperBound with " +
              " fraction and BootStrap trails = " + bootStrapTrials + " for " +
              numloop + " run=================")
          logInfo("Lower Bound on MonthSum  = " + meanLBonMonth_sum +
              " Upper Bound on MonthSum = " + meanUBonMonth_sum)
          logInfo("deviation LowerBound = " + (3436109.45 - meanLBonMonth_sum) +
              " deviation UpperBound = " + (9038381.33 - meanUBonMonth_sum))
          logInfo("Average query execution time = " +
              cumulativeTime(0).toFloat / (i.toFloat + 1))
          logInfo("")

          logInfo("Lower Bound on YearSum = " + meanLBonYear_sum +
              " Upper Bound on YearSum = " + meanUBonYear_sum)
          logInfo("deviation LowerBound = " + (6207218.63 - meanLBonYear_sum) +
              " deviation UpperBound = " + (14138164.56 - meanUBonYear_sum))
          logInfo("Average query execution time = " +
              cumulativeTime(1).toFloat / (i.toFloat + 1))
          logInfo("")

          logInfo("Lower Bound on MonthAvg = " + meanLBonMonth_avg +
              " Upper Bound on MonthAvg = " + meanUBonMonth_avg)
          logInfo("deviation LowerBound = " + (3.82 - meanLBonMonth_avg) +
              " deviation UpperBound = " + (10.03 - meanUBonMonth_avg))
          logInfo("Average query execution time = " +
              cumulativeTime(2).toFloat / (i.toFloat + 1))
          logInfo("")

          logInfo("Lower Bound on YearAvg = " + meanLBonYear_avg +
              " Upper Bound on YearAvg = " + meanUBonYear_avg)
          logInfo("deviation LowerBound = " + (3.28 - meanLBonYear_avg) +
              " deviation UpperBound = " + (7.48 - meanUBonYear_avg))
          logInfo("Average query execution time = " +
              cumulativeTime(3).toFloat / (i.toFloat + 1))
          logInfo("")
          logInfo("Lower Bound on MonthCnt = " + meanLBonMonth_cnt +
              " Upper Bound on MonthCnt = " + meanUBonMonth_cnt)
          logInfo("deviation LowerBound = " + (855744 - meanLBonMonth_cnt) +
              " deviation UpperBound = " + (942222 - meanUBonMonth_cnt))
          logInfo("Average query execution time = " +
              cumulativeTime(4).toFloat / (i.toFloat + 1))
          logInfo("")
          logInfo("Lower Bound on YearCnt = " + meanLBonYear_cnt +
              " Upper Bound on YearCnt  = " + meanUBonYear_cnt)
          logInfo("deviation LowerBound = " + (1888612 - meanLBonYear_cnt) +
              " deviation UpperBound = " + (1888618 - meanUBonYear_cnt))
          logInfo("Average query execution time = " +
              cumulativeTime(5).toFloat / (i.toFloat + 1))

          logInfo("")
          logInfo("Count and sum condition On Month for [" + i + "] iterration ")
          logInfo("Average query execution time so far = " +
              cumulativeTime(6).toFloat / (i.toFloat + 1))

          logInfo("")
          logInfo("Count and sum with condition On Year for [" + i + "]")
          logInfo("Average query execution time so far = " +
              cumulativeTime(7).toFloat / (i.toFloat + 1))

          logInfo("")
          throw e
      }

    }

    // Calculating Mean of 1000 LowerBounds and UpperBounds
    val meanLBonMonth_sum = lbMonth_sum / numloop
    val meanUBonMonth_sum = ubMonth_sum / numloop
    val meanLBonYear_sum = lbYear_sum / numloop
    val meanUBonYear_sum = ubYear_sum / numloop

    val meanLBonMonth_avg = lbMonth_avg / numloop
    val meanUBonMonth_avg = ubMonth_avg / numloop
    val meanLBonYear_avg = lbYear_avg / numloop
    val meanUBonYear_avg = ubYear_avg / numloop

    val meanLBonMonth_cnt = lbMonth_cnt / numloop
    val meanUBonMonth_cnt = ubMonth_cnt / numloop
    val meanLBonYear_cnt = lbYear_cnt / numloop
    val meanUBonYear_cnt = ubYear_cnt / numloop

    logInfo("================BootStrap LowerBound UpperBound with " +
        " fraction and BootStrap trails = " + bootStrapTrials + " for " +
        numloop + " run=================")
    logInfo("Lower Bound on MonthSum  = " + meanLBonMonth_sum +
        " Upper Bound on MonthSum = " + meanUBonMonth_sum)
    logInfo("deviation LowerBound = " + (3436109.45 - meanLBonMonth_sum) +
        " deviation UpperBound = " + (9038381.33 - meanUBonMonth_sum))
    logInfo("Average query execution time = " + cumulativeTime(0).toFloat / numloop.toFloat)
    logInfo("")

    logInfo("Lower Bound on YearSum = " + meanLBonYear_sum +
        " Upper Bound on YearSum = " + meanUBonYear_sum)
    logInfo("deviation LowerBound = " + (6207218.63 - meanLBonYear_sum) +
        " deviation UpperBound = " + (14138164.56 - meanUBonYear_sum))
    logInfo("Average query execution time = " + cumulativeTime(1).toFloat / numloop.toFloat)
    logInfo("")

    logInfo("Lower Bound on MonthAvg = " + meanLBonMonth_avg +
        " Upper Bound on MonthAvg = " + meanUBonMonth_avg)
    logInfo("deviation LowerBound = " + (3.82 - meanLBonMonth_avg) +
        " deviation UpperBound = " + (10.03 - meanUBonMonth_avg))
    logInfo("Average query execution time = " + cumulativeTime(2).toFloat / numloop.toFloat)
    logInfo("")

    logInfo("Lower Bound on YearAvg = " + meanLBonYear_avg +
        " Upper Bound on YearAvg = " + meanUBonYear_avg)
    logInfo("deviation LowerBound = " + (3.28 - meanLBonYear_avg) +
        " deviation UpperBound = " + (7.48 - meanUBonYear_avg))
    logInfo("Average query execution time = " + cumulativeTime(3).toFloat / numloop.toFloat)
    logInfo("")
    logInfo("Lower Bound on MonthCnt = " + meanLBonMonth_cnt +
        " Upper Bound on MonthCnt = " + meanUBonMonth_cnt)
    logInfo("deviation LowerBound = " + (855744 - meanLBonMonth_cnt) +
        " deviation UpperBound = " + (942222 - meanUBonMonth_cnt))
    logInfo("Average query execution time = " + cumulativeTime(4).toFloat / numloop.toFloat)
    logInfo("")
    logInfo("Lower Bound on YearCnt = " + meanLBonYear_cnt +
        " Upper Bound on YearCnt  = " + meanUBonYear_cnt)
    logInfo("deviation LowerBound = " + (1888612 - meanLBonYear_cnt) +
        " deviation UpperBound = " + (1888618 - meanUBonYear_cnt))
    logInfo("Average query execution time = " + cumulativeTime(5).toFloat / numloop.toFloat)

    logInfo("")

    Thread.sleep(60 * 60 * 1000)
  }

  ignore("BootStrap LowerBound and UpperBound over 1000 iter") {
    this.testBounds()
  }


  ignore("test for Finding BootStrap SD") {
    val numloop : Integer = 1000

    var errorBoundForPopulationMonth_sum, errorBoundForPopulationYear_sum,
        errorBoundForPopulationMonth_avg, errorBoundForPopulationYear_avg,
        errorBoundForPopulationMonth_cnt, errorBoundForPopulationYear_cnt = 0.0
    var sumSDMonth_sum, sumSDYear_sum, sumSDMonth_cnt, sumSDYear_cnt,
        sumSDMonth_avg, sumSDYear_avg = 0.0
    for (i <- 0 until numloop) {
      writeSampleTable()
      val resultMonth_Sum =
        snc.sql(s"SELECT SUM(ArrDelay) as sum, lower_bound(sum), upper_bound(sum) FROM airline" +
            s" where $whereConditionM confidence " + confidence)
      val resultYear_Sum =
        snc.sql(s"SELECT SUM(ArrDelay) as sum, lower_bound(sum), upper_bound(sum)  FROM airline" +
            s" where $whereConditionY confidence " + confidence)
      val resultMonth_Avg =
        snc.sql(s"SELECT AVG(ArrDelay) as AVG, lower_bound(AVG),upper_bound(AVG) FROM" +
            s" airline where $whereConditionM confidence " + confidence)
      val resultYear_Avg =
        snc.sql(s"SELECT AVG(ArrDelay) as AVG, lower_bound(AVG), upper_bound(AVG) FROM airline" +
            s" where $whereConditionY confidence " + confidence)
      val resultMonth_Count =
        snc.sql(s"SELECT COUNT(ArrDelay) as count, lower_bound(count), upper_bound(count) " +
            s"FROM airline where $whereConditionM confidence " + confidence)
      val resultYear_Count =
        snc.sql(s"SELECT COUNT(ArrDelay) as count, lower_bound(count),upper_bound(count) FROM" +
            s" airline where $whereConditionY confidence " + confidence)

      val rowMonth_sum = resultMonth_Sum.collect()
      val lowerBound_MS = rowMonth_sum(0).getDouble(1)
      val upperBound_MS = rowMonth_sum(0).getDouble(2)

      val rowYear_sum = resultYear_Sum.collect()
      val lowerBound_YS = rowYear_sum(0).getDouble(1)
      val upperBound_YS = rowYear_sum(0).getDouble(2)

      val rowMonth_Avg = resultMonth_Avg.collect()
      val lowerBound_MA = rowMonth_Avg(0).getDouble(1)
      val upperBound_MA = rowMonth_Avg(0).getDouble(2)

      val rowYear_Avg = resultYear_Avg.collect()
      val lowerBound_YA = rowYear_Avg(0).getDouble(1)
      val upperBound_YA = rowYear_Avg(0).getDouble(2)

      val rowMonth_Cnt = resultMonth_Count.collect()
      val lowerBound_MC = rowMonth_Cnt(0).getDouble(1)
      val upperBound_MC = rowMonth_Cnt(0).getDouble(2)

      val rowYear_Cnt = resultYear_Count.collect()
      val lowerBound_YC = rowYear_Cnt(0).getDouble(1)
      val upperBound_YC = rowYear_Cnt(0).getDouble(2)

      errorBoundForPopulationMonth_sum = (upperBound_MS - lowerBound_MS) / 2
      errorBoundForPopulationYear_sum = (upperBound_YS - lowerBound_YS) / 2
      errorBoundForPopulationMonth_avg = (upperBound_MA - lowerBound_MA) / 2
      errorBoundForPopulationYear_avg = (upperBound_YA - lowerBound_YA) / 2
      errorBoundForPopulationMonth_cnt = (upperBound_MC - lowerBound_MC) / 2
      errorBoundForPopulationYear_cnt = (upperBound_YC - lowerBound_YC) / 2

      deleteSampleTable()

      val SDOnMonth_sum = errorBoundForPopulationMonth_sum/confFactor
      val SDOnYear_sum = errorBoundForPopulationYear_sum/confFactor
      val SDOnMonth_avg = errorBoundForPopulationMonth_avg/confFactor
      val SDOnYear_avg = errorBoundForPopulationYear_avg/confFactor
      val SDOnMonth_cnt = errorBoundForPopulationMonth_cnt/confFactor
      val SDOnYear_cnt = errorBoundForPopulationYear_cnt/confFactor

      sumSDMonth_sum += SDOnMonth_sum
      sumSDYear_sum += SDOnYear_sum
      sumSDMonth_avg += SDOnMonth_avg
      sumSDYear_avg += SDOnYear_avg
      sumSDMonth_cnt += SDOnMonth_cnt
      sumSDYear_cnt += SDOnYear_cnt

      if (i > 0 && (i == 5 || i == 10 || (i % 100 == 0))) {
        logInfo("BootStrap SD for SUM with condition on Month [" + i + "] = " +
            " %.2f".format(SDOnMonth_sum))
        logInfo("BootStrap SD for SUM with condition On Year [" + i + "] = " +
            " %.2f".format(SDOnYear_sum))
        logInfo("BootStrap SD for Avg with condition On Month [" + i + "] = " +
            " %.2f".format(SDOnMonth_avg))
        logInfo("BootStrap SD for Avg with condition On Year [" + i + "] = " +
            " %.2f".format(SDOnYear_avg))
        logInfo("BootStrap SD for Count with condition On Month [" + i + "] = " +
            " %.2f".format(SDOnMonth_cnt))
        logInfo("BootStrap SD for Count with condition On Year [" + i + "] = " +
            " %.2f".format(SDOnYear_cnt))
      }
    }

    // Calculating mean of 1000 SDs
    val meanSDOnMonth_sum = sumSDMonth_sum/numloop
    val meanSDOnYear_sum = sumSDYear_sum/numloop
    val meanSDOnMonth_avg = sumSDMonth_avg/numloop
    val meanSDOnYear_avg = sumSDYear_avg/numloop
    val meanSDOnMonth_cnt = sumSDMonth_cnt/numloop
    val meanSDOnYear_cnt = sumSDYear_cnt/numloop

    logInfo("================BootStrap SD with "  +
        " fraction for " + numloop + " run=================")
    logInfo("Mean SD for SUM with condition on Month  ->>>>>>>>>> " + meanSDOnMonth_sum)
    logInfo("Mean SD For SUM with condition On Year ->>>>>>>>>> " + meanSDOnYear_sum)
    logInfo("Mean SD For Avg with condition On Month  ->>>>>>>>>> " + meanSDOnMonth_avg)
    logInfo("Mean SD for Avg with condition on Year  ->>>>>>>>>> " + meanSDOnYear_avg)
    logInfo("Mean SD For Count with condition On Month ->>>>>>>>>> " + meanSDOnMonth_cnt)
    logInfo("Mean SD For Count with condition On Year  ->>>>>>>>>> " + meanSDOnYear_cnt)
  }

  ignore("test to calculate GT for LB UB") {

    val rand : Integer = 32 // r
    val sampleNum : Integer = 0 // s

    logInfo(s"Read: Start...r$rand")

    // TODO : Change the path as needed
    val pathName = "/home/supriya/snappy/textFiles"

    val is = new Array[InputStream](6)
    val dis = new Array[DataInputStream](6)
    for (_ <- 0 to 5) {
      is(0) = null
      dis(0) = null
    }

    try {
      val fileName = Array(pathName + s"/SUM_Year_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/SUM_Month_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/Count_Year_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/Count_Month_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/Mean_Year_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/Mean_Month_s$sampleNum" + s"_r$rand.txt"
      )
      for (i <- 0 to 5) {
        is(i) = new FileInputStream(fileName(i))
        dis(i) = new DataInputStream(is(i))
      }

      for (i <- 0 to 5) {
        logInfo("")
        logInfo(s"Reading: ${fileName(i)} : ")
        var k = 0
        val tempArray = new Array[Double](10000)
        while (dis(i).available() > 0)
        {
          val value = dis(i).readDouble()
          tempArray(k) = value
          breakable {
            if (k == 9999) {
              Sorting.quickSort(tempArray)
              logInfo("LB and UB for file " + fileName(i) + " is")
              logInfo("LowerBound = " + tempArray(249) + " UpperBound = " + tempArray(9749))
              break
            }
            k += 1
          }
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      for (elem <- is) {if (elem != null) elem.close()}
      for (elem <- dis) {if (elem != null) elem.close()}
    }

    logInfo("")
    logInfo(s"Read: Done...r$rand")
  }

  ignore("test null handling") {
    logInfo("Inside test")
    deleteSampleTable()
    writeSampleTable()
    val totalBaseCnt = snc.sql("select count(*) from airline")
    val totalSampleCnt = snc.sql(
      "select count(*) as population_count, count(*) as sample_ from airlineSampled")
    val r2 = totalSampleCnt.collect()
    val r1 = totalBaseCnt.collect()

    logInfo("TotalBaseCnt = " + r1(0).getLong(0))
    logInfo("TotalSampleCnt = " + r2(0).getLong(1))
    logInfo(" Total Estimated Cnt= " + r2(0).getLong(0))

    /* val r3 = snc.sql("Select sum(ArrDelay) as x , absolute_error(x) from airline where " +
        "Origin = 'DL' confidence 0.95")
    r3.show()
    val r33 = r3.collect()
    println("Sum Value is "+ r33(0).getDouble(0))
    println("Absolute Value "+ r33(0).getDouble(1)) */
  }


  def assertAnalysis(): Unit = {
    if (useClosedForm) {
      AssertAQPAnalysis.closedFormAnalysis(this.snc)
    } else {
      AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    }
  }

}
