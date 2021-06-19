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
import org.apache.commons.math3.distribution.NormalDistribution
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{AssertAQPAnalysis, SparkConf}

class CompareBootstrapClosedFormSuite
    extends SnappyFunSuite with BeforeAndAfterAll {

  private val disabled = true
  val bootStrapTrials = 100
  // default
  val confidence = 0.95
  val confFactor = new NormalDistribution().inverseCumulativeProbability(
    0.5 + confidence / 2.0)
  val whereConditionM: String = s" MonthI > 0 "
  val whereConditionY: String = s" YearI > 1994 "
  var qcsColumns: String = "UniqueCarrier"
  // Same as QCS
  var qcs: String = "START"
  var currQCS: String = ""
  val strataReserviorSize = 50
  var results_sampled_table: DataFrame = _
  var airlineDataFrame: DataFrame = _

  var sampleTableCreated = false
  var useClosedForm = true

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
        .setMaster("local[10]")
        .set("spark.logConf", "true")
        .set(io.snappydata.Property.NumBootStrapTrials.name, s"$bootStrapTrials")
        .set(io.snappydata.Property.ClosedFormEstimates.name, useClosedForm.toString)
        .set(Property.TestDisableCodeGenFlag.name , "true")
        .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")
    //  .set("spark.executor.instances", "8")
  }


  val ddlStr = "( YearI INT," + // NOT NULL
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
    val hfile: String = getClass.getResource("/1995-2015_ParquetData").getPath
    // val hfile: String = getClass.getResource("/sample.csv").getPath
    val snContext = this.snc
    snc.dropTable("airline", ifExists = true)
    snc.dropTable("airlineSampled", ifExists = true)
    snContext.sql("set spark.sql.shuffle.partitions=6")
    logInfo("about to load parquet")
    // format("com.databricks.spark.csv")
    /*
    airlineDataFrame = snContext.read.format("com.databricks.spark.csv").load(hfile).toDF(
      "YEARI", "MonthI", "DayOfMonth", "DayOfWeek", "UniqueCarrier", "TailNum", "FlightNum",
      "Origin", "Dest", "CRSDepTime", "DepTime", "DepDelay", "TaxiOut", "TaxiIn", "CRSArrTime",
      "ArrTime", "ArrDelay", "Cancelled", "CancellationCode", "Diverted", "CRSElapsedTime",
      "ActualElapsedTime", "AirTime", "Distance", "CarrierDelay", "WeatherDelay", "NASDelay",
      "SecurityDelay", "LateAircraftDelay", "ArrDelaySlot", "SNAPPY_SAMPLER_WEIGHTAGE")
    */
    airlineDataFrame = snContext.read.parquet(hfile).toDF("YEARI", "MonthI", "DayOfMonth",
      "DayOfWeek", "UniqueCarrier", "TailNum", "FlightNum", "Origin", "Dest",
      "CRSDepTime", "DepTime", "DepDelay", "TaxiOut",
      "TaxiIn", "CRSArrTime", "ArrTime", "ArrDelay", "Cancelled",
      "CancellationCode", "Diverted", "CRSElapsedTime",
      "ActualElapsedTime", "AirTime", "Distance", "CarrierDelay",
      "WeatherDelay", "NASDelay", "SecurityDelay",
      "LateAircraftDelay", "ArrDelaySlot")
    logInfo("airline data frame created")

    logInfo(" schema of main table is " + airlineDataFrame.schema)
    // airlineDataFrame = snContext.read.parquet(hfile)
    snContext.createTable("airline", "column", airlineDataFrame.schema, Map("buckets" -> "8"))

    airlineDataFrame.write.insertInto("airline")






    val rs1 = snc.sql(s"SELECT ArrDelay  FROM airline" +
        s" where $whereConditionM ")
    rs1.collect()

    val rs2 = snc.sql(s"SELECT count(*)  FROM airline")
    logInfo("********************total number of rows in main table = " +
        rs2.collect()(0).getLong(0))

    /**
     * Total number of executors (or partitions) = Total number of buckets +
     * Total number of reservoir
     */
    /**
     * Partition by influences whether a strata is handled in totality on a
     * single executor or not, and thus an important factor.
     */

  }

  def createSampleTable(): Unit = {
    logInfo("creating sample table")
    snc.sql(s"CREATE TABLE airlineSampled $ddlStr" +
        "USING column_sample " +
        "options " +
        "(" +
        s"qcs '$qcsColumns'," +
        s"fraction '0.01'," +
        "BUCKETS '8'," +
        // s"${Constants.keyBypassSampleOperator} 'true'," +
        s"strataReservoirSize '$strataReserviorSize', " +
        "baseTable 'airline')")

    logInfo("loading data into  sample table")
    airlineDataFrame.write.
        format("column_sample")
        .mode(SaveMode.Append)
        .saveAsTable("airlineSampled")
    // airlineDataFrame.write.insertInto("airlineSampled")


    val rs = snc.sql(s"SELECT ArrDelay  FROM airlineSampled" +
        s" where $whereConditionM ")
    rs.collect()

    val rs3 = snc.sql("SELECT count(*) as sample_count  FROM airlineSampled")
    logInfo("result of query = SELECT count(*) as sample_count  FROM airlineSampled")
    val rows3 = rs3.collect()

    val rs4 = snc.sql("SELECT count(*)  FROM airlineSampled with error 0")
    logInfo("result of query = SELECT count(*)  FROM airlineSampled with error 0")
    rs4.collect()

    val rs5 = snc.sql("SELECT count(*)  FROM airlineSampled ")
    logInfo("result of query = SELECT count(*)  FROM airlineSampled")
    rs5.collect()

    logInfo("***********************actual number of rows ins sampled table =" +
        rows3(0).getLong(0))
    sampleTableCreated = true
  }

  def deleteSampleTable(): Unit = {
    snc.sql(s"TRUNCATE TABLE airlineSampled")
  }


  def dropSampleTable(): Unit = {
    snc.sql(s"DROP TABLE if exists airlineSampled")
  }

  ignore(" accuracy numbers ") {

    val t1 = System.currentTimeMillis()
    val actualSumRS = snc.sql(s"select AVG(ArrDelay) avg_arrival_delay, " +
        s"YearI from airline  group by YEARI order by YEARI ")
    val actualSumEstimate = actualSumRS.collect()(0).getDouble(0)
    actualSumRS.collect()
    val t2 = System.currentTimeMillis()
    logInfo("actual avg = " + actualSumEstimate + "; time taken = " + (t2 - t1))

    this.createSampleTable()
    val t3 = System.currentTimeMillis()
    val closedformSum = snc.sql(s"select AVG(ArrDelay) avg_arrival_delay, " +
        s"absolute_error(avg_arrival_delay) abs_err, YearI from airline " +
        s"group by YEARI order by YEARI with error 0.1 behavior 'do_nothing'")

    val closedFormEstimate = closedformSum.collect()(0).getDouble(0)
    val t4 = System.currentTimeMillis()
    closedformSum.collect()
    logInfo("closedform estimate avg = " + closedFormEstimate +
        "; time taken = " + (t4 - t3))
    AssertAQPAnalysis.closedFormAnalysis(this.snc)

    this.useClosedForm = false

    val t5 = System.currentTimeMillis()
    val bootstrapSum = snc.sql(s"select AVG(ArrDelay) avg_arrival_delay, " +
        s"absolute_error(avg_arrival_delay) abs_err, YearI from airline " +
        s" where ArrDelay > 0  group by YEARI order by YEARI " +
        s"with error 0.1 behavior 'do_nothing'")

    val bootstrapEstimate = bootstrapSum.collect()(0).getDouble(0)
    val t6 = System.currentTimeMillis()
    logInfo("bootstrap Estimate sum  = " + bootstrapEstimate + "; time taken = " + (t6 - t5))
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    bootstrapSum.collect()
    // assert(math.abs(closedFormEstimate - bootstrapEstimate) < 5)
    this.deleteSampleTable()
  }

  ignore("compare bootstrap sum result with closedform result ") {

    val t1 = System.currentTimeMillis()
    val actualSumRS = snc.sql(s"SELECT SUM(ArrDelay) as sum FROM airline ")
    val actualSumEstimate = actualSumRS.collect()(0).getDouble(0)
    val t2 = System.currentTimeMillis()
    logInfo("actual sum = " + actualSumEstimate + "; time taken = " + (t2 - t1))

    this.createSampleTable()
    val t3 = System.currentTimeMillis()
    val closedformSum = snc.sql(s"SELECT SUM(ArrDelay) as sum, lower_bound(sum), " +
        s"upper_bound(sum) FROM airline confidence $confidence")

    val closedFormEstimate = closedformSum.collect()(0).getDouble(0)
    val t4 = System.currentTimeMillis()
    logInfo("closedform estimate sum = " + closedFormEstimate +
        "; time taken = " + (t4 - t3))
    AssertAQPAnalysis.closedFormAnalysis(this.snc)
    this.deleteSampleTable()
    this.afterAll()
    this.useClosedForm = false
    this.beforeAll()
    this.createSampleTable()
    val t5 = System.currentTimeMillis()
    val bootstrapSum = snc.sql(s"SELECT SUM(ArrDelay) as sum, lower_bound(sum), " +
        s"upper_bound(sum) FROM airline confidence $confidence")

    val bootstrapEstimate = bootstrapSum.collect()(0).getDouble(0)
    val t6 = System.currentTimeMillis()
    logInfo("bootstrap Estimate sum  = " + bootstrapEstimate +
        "; time taken = " + (t6 - t5))
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    assert(math.abs(closedFormEstimate - bootstrapEstimate) < 5)
    this.deleteSampleTable()
  }

  def assertAnalysis(): Unit = {
    if (useClosedForm) {
      AssertAQPAnalysis.closedFormAnalysis(this.snc)
    } else {
      AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    }
  }

}
