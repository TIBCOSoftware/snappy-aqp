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

import com.gemstone.gemfire.internal.AvailablePort
import io.snappydata.{Property, SnappyFunSuite}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers}

import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql._
import org.apache.spark.sql.aqp.functions._
import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.sql.functions._
import org.apache.spark.sql.snappy._

abstract class AbstractAQPDataFrameAPIPart2Test
    extends SnappyFunSuite
        with Matchers
        with BeforeAndAfterAll with BeforeAndAfter {

  val mainTable: String = "airline"
  val sampleTable: String = "airline_sampled"
  var sampleDataFrame: DataFrame = _
  var mainTableDataFrame: DataFrame = _
  var airLineCodeDataFrame: DataFrame = _
  // Set up sample & Main table
  var hfile: String = getClass.getResource("/2015.parquet").getPath
  val codetableFile: String = getClass.getResource("/airlineCode_Lookup.csv").getPath

  protected def addSpecificProps(conf: SparkConf): Unit

  override def beforeAll(): Unit = {
    stopAll()
    super.beforeAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
  }

  override def afterAll(): Unit = {
    val snc = this.snc
    snc.sql(s"drop table  if exists $sampleTable")
    snc.sql(s"drop table  if exists $mainTable")

    snc.sql(s"drop table  if exists airlinestaging")
    snc.sql(s"drop table  if exists airlinerefstaging")
    snc.sql(s"drop table  if exists airlineref")
    // cleanup metastore
    if (snc != null) {
      snc.clearCache()
    }
    super.afterAll()
    stopAll()
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
    val conf = new org.apache.spark.SparkConf()
        .setAppName("AbstractAQPDataFrameAPITest")
        .setMaster("local[6]")
        .set("spark.sql.hive.metastore.sharedPrefixes",
          "com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc," +
              "com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity," +
              "com.mapr.fs.jni,org.apache.commons")
        .set("master.ui.port", "" + AvailablePort.getRandomAvailablePort(0))
        .set(Property.TestDisableCodeGenFlag.name , "true")
        .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")

    addSpecificProps(conf)
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }


  protected def initTestDataWithCreateTable(): Unit = {
    val snc = this.snc
    snc.sql(s"drop table  if exists $sampleTable")
    snc.sql(s"drop table  if exists $mainTable")

    snc.sql(s"drop table  if exists airlinestaging")
    snc.sql(s"drop table  if exists airlinerefstaging")
    snc.sql(s"drop table  if exists airlineref")

    val stagingDF = snc.read.load(hfile)
    snc.createTable("airlinestaging", "column", stagingDF.schema,
      Map.empty[String, String])
    stagingDF.write.insertInto("airlinestaging")

    mainTableDataFrame = snc.createTable(mainTable, "column", stagingDF.schema,
      Map.empty[String, String])
    stagingDF.write.insertInto(mainTable)

    sampleDataFrame = snc.createSampleTable(sampleTable, Some(mainTable),
      Map("qcs" -> "UniqueCarrier,YearI,MonthI", "fraction" -> "0.06",
        "strataReservoirSize" -> "1000"), allowExisting = false)
    mainTableDataFrame.write.insertInto(sampleTable)
    assert(sampleDataFrame.collect().length > 0)

    val codeTabledf = snc.read
        .format("com.databricks.spark.csv") // CSV to DF package
        .option("header", "true") // Use first line of all files as header
        .option("inferSchema", "true") // Automatically infer data types
        .option("maxCharsPerColumn", "4096")
        .load(codetableFile)

    airLineCodeDataFrame = snc.createTable("airlineref", "column",
      codeTabledf.schema, Map.empty[String, String])
    codeTabledf.write.insertInto("airlineref")
  }


  test("sample data frame api test for data frames created with createTableAPI") {
    this.initTestDataWithCreateTable()
    this.executeTest()
  }

  test("sample data frame api test for data frames created with createTableAPI " +
      "having joins on sample data frame") {
    this.initTestDataWithCreateTable()
    sampleDataFrame.cache()
    val x = sampleDataFrame.join(airLineCodeDataFrame, sampleDataFrame.col("UniqueCarrier").
        equalTo(airLineCodeDataFrame("CODE"))).groupBy(sampleDataFrame("UniqueCarrier"),
      airLineCodeDataFrame("DESCRIPTION")).agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")
    logInfo(x.collect().mkString("\n"))

    // Check if strict error spec throws exception
    try {
      val x = sampleDataFrame.join(airLineCodeDataFrame, sampleDataFrame.col("UniqueCarrier").
          equalTo(airLineCodeDataFrame("CODE"))).groupBy(sampleDataFrame("UniqueCarrier"),
        airLineCodeDataFrame("DESCRIPTION")).agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")
      val y = x.withError(.0000001, .9999, "STRICT").collect()
      fail(s"should have got error limit exceeded exception: " +
          s"precise=${x.collect().mkString("\n")} result=${y.mkString("\n")}")
    } catch {
      case e: Exception if e.toString.indexOf("ErrorLimitExceededException") != -1 => // OK
    }
  }

  test("test validation of error fraction & confidence on queries fired" +
      " on base data frame & sample dataframe using create table") {
    this.initTestDataWithCreateTable()

    try {
      mainTableDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
        absolute_error("sum_arr_delay")).withError(1.2, 0.95)
    }
    catch {
      case e: UnsupportedOperationException => assert(e.toString.indexOf(
        "error and confidence within range of 0 to 1") != -1, e.toString)
        logInfo("Success...error fraction should be in the range 0 < error < 1.0") // OK
    }
    try {
      sampleDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
        absolute_error("sum_arr_delay")).withError(.5, 0)
    }
    catch {
      case e: UnsupportedOperationException => assert(e.toString.indexOf(
        "error and confidence within range of 0 to 1") != -1, e.toString)
        logInfo("Success...confidence should be in the range 0 < confidence < 1.0") // OK
    }
  }


  private def executeTest(): Unit = {
    val finalDF = mainTableDataFrame.agg(Map("arrdelay" -> "sum"))
        .withError(.5, .5)

    logInfo(finalDF.collect().mkString("\n"))

    var rows = finalDF.collect()
    assert(rows.length == 1)
    val totalApproxSum1 = rows(0).getLong(0)

    rows = snc.sql(s"Select sum(arrdelay) from  $mainTable with error .5").collect()
    assert(rows.length == 1)
    val totalApproxSum3 = rows(0).getLong(0)
    assert(math.abs(totalApproxSum1 - totalApproxSum3) < 10)

    val x = mainTableDataFrame.agg(Map("arrdelay" -> "sum"))
    logInfo(x.collect().mkString("\n"))
    rows = x.collect()
    assert(rows.length == 1)
    val totalExactSum = rows(0).getLong(0)
    assert(math.abs(totalExactSum - totalApproxSum1) > 100)

    // check if strict error validation works
    try {
      val x = mainTableDataFrame.agg(Map("arrdelay" -> "sum"))
      val y = x.withError(.00001, .95, "STRICT").collect()
      fail(s"should have got error limit exceeded exception: " +
          s"precise=${x.collect().mkString("\n")} result=${y.mkString("\n")}")
    } catch {
      case e: Exception if e.toString.indexOf("ErrorLimitExceededException") != -1 => // OK
    }

  }

  def msg(m: String): Unit = DebugUtils.msg(m)
}
