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

import scala.util.Random

import com.gemstone.gemfire.internal.AvailablePort
import io.snappydata.{SnappyFunSuite, Constant => Constants}
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.sql.execution.common.{AQPRules, AnalysisType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{AssertAQPAnalysis, SparkConf}

abstract class AbstractPerfDebugTest
  extends SnappyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  val DEFAULT_ERROR = Constants.DEFAULT_ERROR

  val airlineRefTable = "airlineref"
  val airlineMainTable: String = "airline"
  val airlineSampleTable: String = "airline_sampled"
  val airlineTableWOE: String = "airline_woe"
  var airlineSampleDataFrame: DataFrame = _

  var airlineMainTableDataFrame: DataFrame = _
  var airLineCodeDataFrame: DataFrame = _
  // Set up sample & Main table
  var hfile: String = "" // getClass.getResource("/1995-2015_ParquetData").getPath
  val codetableFile = "" // getClass.getResource("/airlineCode_Lookup.csv").getPath

  // Set up sample & Main table

  override def beforeAll(): Unit = {
    System.setProperty("gemfire.PREFER_SERIALIZED" , "false");
    System.setProperty("gemfire.PREFER_RAW_OBJECT" , "true");
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    initTestTables()
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

  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    val random = new Random(731)
    var maxCodeGen: Int = 0
    while (maxCodeGen == 0) {
      maxCodeGen = random.nextInt(500)
    }
    /**
     * Pls do not change the flag values of Property.TestDisableCodeGenFlag.name
     * and Property.UseOptimizedHashAggregateForSingleKey.name
     * They are meant to suppress CodegenFallback Plan so that optimized
     * byte buffer code path is tested & prevented from false passing.
     * If your test needs CodegenFallback, then override the newConf function
     * & clear the flag from the conf of the test locally.
     */
    val conf = new org.apache.spark.SparkConf()
      .setAppName("ErrorEstimationFunctionTest")
      .setMaster("local[8]")
      .set("spark.sql.hive.metastore.sharedPrefixes",
        "com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc," +
          "com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity," +
          "com.mapr.fs.jni,org.apache.commons")
      .set("master.ui.port", "" + AvailablePort.getRandomAvailablePort(0))
      .set(io.snappydata.Property.TestDisableCodeGenFlag.name , "true")
      .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")
      // .set("spark.sql.codegen.maxFields", maxCodeGen.toString)
      .set("spark.sql.codegen.maxFields", "800")
      // .set("spark.sql.aqp.reservoirNotInTable", "true" )
    addSpecificProps(conf)
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  protected def addSpecificProps(conf: SparkConf): Unit

  protected def initTestTables(): Unit = {
    val snc = this.snc
   // this.initAirlineTables1()
    this.initAirlineTablesDummy()
  }

  protected def initAirlineTablesDummy(): Unit = {

  }


  protected def initAirlineTables(): Unit = {
    snc.sql(s"drop table  if exists $airlineTableWOE")
    snc.sql(s"drop table  if exists $airlineSampleTable")
    snc.sql(s"drop table  if exists $airlineRefTable")
    snc.sql(s"drop table  if exists $airlineMainTable")
    snc.sql(s"drop table  if exists airlinestaging")
    snc.sql(s"drop table  if exists airlinerefstaging")

    val stagingDF = snc.read.parquet(hfile).toDF("YEARI", "MonthI", "DayOfMonth",
      "DayOfWeek", "UniqueCarrier", "TailNum", "FlightNum", "Origin", "Dest", "CRSDepTime",
      "DepTime", "DepDelay", "TaxiOut", "TaxiIn", "CRSArrTime", "ArrTime", "ArrDelay", "Cancelled",
      "CancellationCode", "Diverted", "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Distance",
      "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
      "LateAircraftDelay", "ArrDelaySlot")

    airlineMainTableDataFrame = snc.createTable(airlineMainTable, "column",
      stagingDF.schema, Map.empty[String, String])
    stagingDF.write.mode(SaveMode.Append).saveAsTable(airlineMainTable)

    airlineSampleDataFrame = snc.createSampleTable(airlineSampleTable,
      Some(airlineMainTable), Map("qcs" -> "UniqueCarrier,YearI,MonthI",
        "fraction" -> "0.5", "strataReservoirSize" -> "500"),
      allowExisting = false)
    airlineMainTableDataFrame.write.insertInto(airlineSampleTable)

    val df: DataFrame = snc.sql(s"select * from $airlineSampleTable")
    df.registerTempTable("X")
    df.write.format("column").mode(SaveMode.Append).saveAsTable(airlineTableWOE)

    /*
    snc.createTable(airlineTableWOE, "column",
      airlineSampleDataFrame.schema, Map.empty[String, String])
    airlineSampleDataFrame.write.mode(SaveMode.Append).saveAsTable(airlineTableWOE)
    */

    val codeTabledf = snc.read
      .format("com.databricks.spark.csv") // CSV to DF package
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("maxCharsPerColumn", "4096")
      .load(codetableFile)

    airLineCodeDataFrame = snc.createTable(airlineRefTable, "column",
      codeTabledf.schema, Map.empty[String, String])
    codeTabledf.write.mode(SaveMode.Append).saveAsTable(airlineRefTable)
  }
  protected def initAirlineTables1(): Unit = {
    snc.sql(s"drop table  if exists $airlineTableWOE")
    snc.sql(s"drop table  if exists $airlineSampleTable")
    snc.sql(s"drop table  if exists $airlineRefTable")
    snc.sql(s"drop table  if exists $airlineMainTable")
    snc.sql(s"drop table  if exists airlinestaging")
    snc.sql(s"drop table  if exists airlinerefstaging")

    val stagingDF = snc.read.parquet(hfile).toDF("YEARI", "MonthI", "DayOfMonth",
      "DayOfWeek", "UniqueCarrier", "TailNum", "FlightNum", "Origin", "Dest", "CRSDepTime",
      "DepTime", "DepDelay", "TaxiOut", "TaxiIn", "CRSArrTime", "ArrTime", "ArrDelay", "Cancelled",
      "CancellationCode", "Diverted", "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Distance",
      "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
      "LateAircraftDelay", "ArrDelaySlot")
    stagingDF.registerTempTable(airlineMainTable)
    val t1 = System.currentTimeMillis()
   /* airlineMainTableDataFrame = snc.createTable(airlineMainTable, "column",
      stagingDF.schema, Map( "buckets" -> "13")) */

    airlineSampleDataFrame = snc.createSampleTable(airlineSampleTable,
      Some(airlineMainTable), Map("qcs" -> "UniqueCarrier",
        "fraction" -> "0.3", "strataReservoirSize" -> "25", "buckets" -> "23"),
      allowExisting = false)
   // airlineMainTableDataFrame.write.insertInto(airlineSampleTable)
   // stagingDF.write.mode(SaveMode.Append).saveAsTable(airlineMainTable)
    val t2 = System.currentTimeMillis()
    // scalastyle:off println
    println("populated base table. time taken = " + ((t2 -t1) * 1d)/1000)
    val t3 = System.currentTimeMillis()


    stagingDF.write.insertInto(airlineSampleTable)
    // airlineMainTableDataFrame.write.insertInto(airlineSampleTable)
    val t4 = System.currentTimeMillis()

    println("populated sample table .time taken = " + ((t4 -t3) * 1d)/1000)

    val df: DataFrame = snc.sql(s"select * from $airlineSampleTable")
   // df.registerTempTable("X")
    df.write.format("column").option("eviction_by", "NONE").mode(SaveMode.Append).
      saveAsTable (airlineTableWOE)
    // val df1: DataFrame = snc.sql(s"select count(*) from $airlineTableWOE")
    // df1.collect()
    println("populated woe table")
    // scalastyle:on println

    /*
    snc.createTable(airlineTableWOE, "column",
      airlineSampleDataFrame.schema, Map.empty[String, String])
    airlineSampleDataFrame.write.mode(SaveMode.Append).saveAsTable(airlineTableWOE)
    */

    val codeTabledf = snc.read
      .format("com.databricks.spark.csv") // CSV to DF package
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("maxCharsPerColumn", "4096")
          .load(codetableFile)

    airLineCodeDataFrame = snc.createTable(airlineRefTable, "column",
      codeTabledf.schema, Map.empty[String, String])
    codeTabledf.write.mode(SaveMode.Append).saveAsTable(airlineRefTable)
  }

  protected def initAirlineTables2(): Unit = {
    snc.sql(s"drop table  if exists $airlineTableWOE")
    snc.sql(s"drop table  if exists $airlineSampleTable")
    snc.sql(s"drop table  if exists $airlineRefTable")
    snc.sql(s"drop table  if exists $airlineMainTable")
    snc.sql(s"drop table  if exists airlinestaging")
    snc.sql(s"drop table  if exists airlinerefstaging")
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val stagingDF = snc.read.load(hfile)
    stagingDF.registerTempTable(airlineMainTable)
    /*
    airlineMainTableDataFrame = snc.createTable(airlineMainTable, "column",
      stagingDF.schema, Map.empty[String, String])
    stagingDF.write.mode(SaveMode.Append).saveAsTable(airlineMainTable)
    */

    airlineSampleDataFrame = snc.createSampleTable(airlineSampleTable,
      Some(airlineMainTable), Map("qcs" -> "UniqueCarrier",
        "fraction" -> "0.3", "strataReservoirSize" -> "500", "buckets" -> "128"),
      allowExisting = false)
    stagingDF.write.insertInto(airlineSampleTable)

    // scalastyle:off println
    println("populated sample table")

    val df: DataFrame = snc.sql(s"select * from $airlineSampleTable")
    // df.registerTempTable("X")
    df.write.format("column").mode(SaveMode.Append).saveAsTable(airlineTableWOE)

    println("populated woe table")
    // scalastyle:on println
    Thread.sleep(6000)

    /*
    snc.createTable(airlineTableWOE, "column",
      airlineSampleDataFrame.schema, Map.empty[String, String])
    airlineSampleDataFrame.write.mode(SaveMode.Append).saveAsTable(airlineTableWOE)
    */

    val codeTabledf = snc.read
      .format("com.databricks.spark.csv") // CSV to DF package
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("maxCharsPerColumn", "4096")
      .load(codetableFile)

    airLineCodeDataFrame = snc.createTable(airlineRefTable, "column",
      codeTabledf.schema, Map.empty[String, String])
    codeTabledf.write.mode(SaveMode.Append).saveAsTable(airlineRefTable)
  }


  ignore("XXX") {

    //

    val sql1_1 = "SELECT sum(ArrDelay) as SAMPLE_1 FROM airline " +
      "  with error .3 behavior 'do_nothing' "

    val sql1_2 = "SELECT sum(ArrDelay) as summ FROM airline " +
      "  with error .3 behavior 'do_nothing' "

    val sql1_3 = "SELECT sum(ArrDelay) as summ, absolute_error(summ) FROM airline " +
      "  with error .3 behavior 'do_nothing' "

    val t1_1 = runQuery(sql1_1)

    val t1_2 = runQuery(sql1_2)

    val t1_3 = runQuery(sql1_3)

    // scalastyle:off println
    println(s" for query $sql1_2 aqp overhead for by pass error calc = " + t1_2/ t1_1)

    println(s" for query $sql1_3 aqp overhead for  error calc = " + t1_3/ t1_1)


    val sql2_1 = "SELECT UniqueCarrier, sum(ArrDelay) as SAMPLE_1 FROM airline group by " +
      " UniqueCarrier order by UniqueCarrier with error .3 behavior 'do_nothing' "

    val sql2_2 = "SELECT UniqueCarrier, sum(ArrDelay) as summ FROM airline group by " +
      " UniqueCarrier order by UniqueCarrier with error .3 behavior 'do_nothing' "

    val sql2_3 = "SELECT UniqueCarrier, sum(ArrDelay) as summ, absolute_error(summ) FROM airline " +
      " group by  UniqueCarrier order by UniqueCarrier with error .3 behavior 'do_nothing' "

    val t2_1 = runQuery(sql2_1)

    val t2_2 = runQuery(sql2_2)

    val t2_3 = runQuery(sql2_3)

    println(s" for query $sql2_2 aqp overhead = " + t2_2/ t2_1)

    println(s" for query $sql2_3 aqp overhead for  error calc = " + t2_3/ t2_1)
    // scalastyle:on println
  }

  ignore("YYY") {

    //

    val sql1_1 = s"SELECT sum(ArrDelay) as SAMPLE_1 FROM $airlineSampleTable " +
      s"  with error .3 behavior 'do_nothing' "

    val sql1_2 = s"SELECT sum(ArrDelay) as summ, absolute_error(summ) FROM $airlineSampleTable " +
      s"  with error .3 behavior 'do_nothing' "

    val sql1_3 = s"SELECT sum(ArrDelay) as summ FROM $airlineTableWOE "

    val t1_2 = runQuery(sql1_2)
    this.assertAnalysis()

    val t1_1 = runQuery(sql1_1)

    val t1_3 = runQuery(sql1_3)

    // scalastyle:off println
    println(s" for query $sql1_2 aqp overhead for  error calc = " + t1_2/ t1_3)

    println(s" for query $sql1_1 aqp overhead for  just becuase sample table = " + t1_1/ t1_3)
    // scalastyle:on println
  }

  private def runQuery(sql: String, expectedAnalysis: AnalysisType.Type = AnalysisType.Defer):
  Double = {
    val rs2 = snc.sql(sql)
    rs2.show
    val rows2 = rs2.collect()
    // assert(AssertAQPAnalysis.getAnalysisType(snc).map(_ == expectedAnalysis).getOrElse(false))

    for(i <- 0 until 2) {
      val rsX = snc.sql(sql)
      rsX.collect()
    }

    val loop = 5
    var t1: Long = 0L

    for(i <- 0 until loop) {

      val rsX = snc.sql(sql)
      val t11 = System.currentTimeMillis()
      rsX.collect()
      val t12 = System.currentTimeMillis()
      t1 += (t12 - t11)
    }
    val avg = (t1*1d)/loop
    // scalastyle:off println
    println( s"avg time for query $sql = " + avg)
    // scalastyle:on println
    avg
  }

  def msg(m: String): Unit = DebugUtils.msg(m)

  def assertAnalysis()
}

