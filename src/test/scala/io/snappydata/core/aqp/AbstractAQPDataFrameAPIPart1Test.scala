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
import org.scalatest.tagobjects.Retryable
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers}

import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql._
import org.apache.spark.sql.aqp.functions._
import org.apache.spark.sql.execution.common.{AQPRules, ReplaceWithSampleTable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.snappy._

abstract class AbstractAQPDataFrameAPIPart1Test
  extends SnappyFunSuite
  with Matchers
  with BeforeAndAfterAll with BeforeAndAfter  {

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
    this.initTestDataWithoutDataSource()
  }

  override def afterAll(): Unit = {
    val snc = this.snc
    snc.sql(s"drop table  if exists $sampleTable")
    snc.sql(s"drop table  if exists $mainTable")
    snc.sql(s"drop table  if exists airlinestaging")
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


  private def initTestDataWithoutDataSource(): Unit = {
    val snc = this.snc
    snc.sql(s"drop table  if exists $sampleTable")
    snc.sql(s"drop table  if exists $mainTable")
    snc.sql(s"drop table  if exists airlinestaging")

    mainTableDataFrame = snc.read.load(hfile).filter("arrDelay > 0")
    mainTableDataFrame.cache()
    val finalDF = mainTableDataFrame.agg(Map("arrdelay" -> "sum"))
    logInfo(finalDF.collect().mkString("\n"))
    sampleDataFrame = mainTableDataFrame.stratifiedSample(Map(
      "qcs" -> "UniqueCarrier,YearI,MonthI", "fraction" -> 0.03,
      "strataReservoirSize" -> "50"))
    sampleDataFrame.cache()
  }

  private def initTestDataWithDataSource(): Unit = {
    val snc = this.snc


    snc.sql(s"drop table  if exists $mainTable")
    snc.sql(s"drop table  if exists $sampleTable")
    snc.sql(s"drop table  if exists airlinestaging")
    snc.sql(s"drop table  if exists airlinerefstaging")
    snc.sql(s"drop table  if exists airlineref")


   val stagingDF = snc.read.load(hfile)
    snc.createTable("airlinestaging", "column", stagingDF.schema,
      Map.empty[String, String])
    stagingDF.write.mode(SaveMode.Ignore).saveAsTable("airlinestaging")
    mainTableDataFrame = snc.sql(s"CREATE TABLE $mainTable USING column OPTIONS(buckets '8') AS " +
      s"(SELECT *  FROM airlinestaging where arrDelay > 0)")

    sampleDataFrame = snc.sql(s"CREATE SAMPLE TABLE $sampleTable " +
      s"ON $mainTable options " +
      "(" +
      "qcs 'UniqueCarrier,YearI,MonthI'," +
      "fraction '0.02'," +
      s"strataReservoirSize '50') AS (SELECT * FROM $mainTable)")

    val codeTabledf = snc.read
            .format("com.databricks.spark.csv") // CSV to DF package
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .option("maxCharsPerColumn", "4096")
            .load(codetableFile)

    snc.createTable("airlinerefstaging", "column", codeTabledf.schema,
      Map.empty[String, String])
    codeTabledf.write.mode(SaveMode.Ignore).saveAsTable("airlinerefstaging")

    airLineCodeDataFrame = snc.sql(s"CREATE TABLE AIRLINEREF USING row AS " +
      s"(SELECT CODE, DESCRIPTION FROM airlinerefstaging)")
  }




  test("sample data frame api test for data frames created without DataSource API", Retryable) {

    val finalDF = mainTableDataFrame.agg(Map("arrdelay" -> "sum"))
      .withError(.5, .5)

    var rows = finalDF.collect()
    assert(rows.length == 1)
    val totalApproxSum1 = rows(0).getLong(0)

    val x = mainTableDataFrame.agg(Map("arrdelay" -> "sum"))
    rows = x.collect()
    assert(rows.length == 1)
    val totalExactSum = rows(0).getLong(0)

    assert(math.abs(totalExactSum - totalApproxSum1) > 100 )
    assert( (math.abs(totalExactSum - totalApproxSum1) *100)/totalExactSum < 30)

    // check if strict error is given we get an exception

    try {
      val x = mainTableDataFrame.agg(Map("arrdelay" -> "sum"))
      val y = x.withError(.00001, .95, "STRICT").collect()
      fail(s"should have got error limit exceeded exception: " +
          s"precise=${x.collect().mkString("\n")} result=${y.mkString("\n")}")
    } catch {
      case e: Exception if e.toString.indexOf("ErrorLimitExceededException") != -1 =>
    }
  }


  test("Bug AQP211: A table with weight column should be treated as a sample table") {
    val expectedCount = mainTableDataFrame.agg(Map("arrdelay" -> "count")).withError(.9, .1).
      collect()(0).getLong(0)
    sampleDataFrame.write.format("column").option("buckets", "8").saveAsTable("testSampleCol")
    try {
      val newDF = snc.table("testSampleCol")
      val actualCount = newDF.agg(Map("arrdelay" -> "count")).collect()(0).getLong(0)
      assert(Math.abs(expectedCount - actualCount) < 2)
    } finally {
      snc.dropTable("testSampleCol")
    }
  }


  test("Bug AQP-130 order by on error functions not supported") {

    val results1 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias
    ("sum_arr_delay"), relative_error("sum_arr_delay").alias("rel_err")).
      orderBy(col("rel_err")).withError(.9, .1).collect()

    for(i <- 0 until results1.length - 1) {
      val row1 = results1(i)
      val row2 = results1(i + 1)
      assert(row1.getDouble(2) <= row2.getDouble(2))
    }
  }

  test("sample data frame api test for error estimates retrieval with alias") {

    var df2 = mainTableDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
      absolute_error("sum_arr_delay")).withError(.5, .5)
    var rows = df2.collect()
    assert(rows.length == 1)
    assert(rows(0).schema.length == 2)
    assert(rows(0).getDouble(1) > 0)

    df2 = mainTableDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
      relative_error("sum_arr_delay")).withError(.5, .5)
    rows = df2.collect()
    assert(rows.length == 1)
    assert(rows(0).schema.length == 2)
    assert(rows(0).getDouble(1) > 0)

    df2 = mainTableDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
      upper_bound("sum_arr_delay")).withError(.5, .5)
    rows = df2.collect()
    assert(rows.length == 1)
    assert(rows(0).schema.length == 2)
    assert(rows(0).getDouble(1) > 0)


    df2 = mainTableDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
      lower_bound("sum_arr_delay")).withError(.5, .5)
    rows = df2.collect()
    assert(rows.length == 1)
    assert(rows(0).schema.length == 2)
    assert(rows(0).getDouble(1) > 0)

    // test multiple aggregate functions & error estimates


    df2 = mainTableDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
      lower_bound("sum_arr_delay"), avg("arrdelay").alias("avg_arr_delay"),
      lower_bound("avg_arr_delay") ).withError(.5, .5)
    rows = df2.collect()
    assert(rows.length == 1)
    assert(rows(0).schema.length == 4)
    assert(rows(0).getDouble(1) > 0)
    assert(rows(0).getDouble(3) > 0)
    assert(rows(0).getDouble(3) !=  rows(0).getDouble(1))

  }

  ignore("Bug SNAP-1580") {
    val df2 =
      mainTableDataFrame.filter("UniqueCarrier IN ( 'NK', 'VX', 'EV', 'B6')").
          groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
        absolute_error("sum_arr_delay")).withError(.5, .5, "DO_NOTHING")
    val rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) > 0)
  }

  test("sample data frame api test for error estimates retrieval with alias and group by") {

    var df2 =
      mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      absolute_error("sum_arr_delay")).withError(.5, .5, "DO_NOTHING")
    var rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) > 0)

    df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      relative_error("sum_arr_delay")).withError(.5, .5, "DO_NOTHING")
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) > 0)

    df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      upper_bound("sum_arr_delay")).withError(.5, .5, "DO_NOTHING")
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) > 0)


    df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      lower_bound("sum_arr_delay")).withError(.5, .5, "DO_NOTHING")
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) > 0)

    // test multiple aggregate functions & error estimates

    df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      lower_bound("sum_arr_delay"), avg("arrdelay").alias("avg_arr_delay"),
      lower_bound("avg_arr_delay") ).withError(.5, .5, "DO_NOTHING")
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 5)
    assert(rows(0).getDouble(2) > 0)
    assert(rows(0).getDouble(4) > 0)
    assert(rows(0).getDouble(4) !=  rows(0).getDouble(2))

  }

  test("sample data frame api test for explicit routing on error estimates failure") {

    var df2 =
      mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      absolute_error("sum_arr_delay")).withError(0.00001, .5, "RUN_ON_FULL_TABLE")
    var rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) == 0)

    df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      relative_error("sum_arr_delay")).withError(0.00001, .5, "RUN_ON_FULL_TABLE")
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) == 0)

    df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      upper_bound("sum_arr_delay")).withError(0.00001, .5, "RUN_ON_FULL_TABLE")
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).isNullAt(2))


    df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      lower_bound("sum_arr_delay")).withError(0.00001, .5, "RUN_ON_FULL_TABLE")
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).isNullAt(2))

    // test multiple aggregate functions & error estimates


    df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      lower_bound("sum_arr_delay"), avg("arrdelay").alias("avg_arr_delay"),
      lower_bound("avg_arr_delay") ).withError(0.00001, .5, "RUN_ON_FULL_TABLE")
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 5)
    assert(rows(0).isNullAt(2))
    assert(rows(0).isNullAt(4))

  }

  test("sample data frame api test for default behavior routing on error estimates failure") {

    var df2 =
      mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
        absolute_error("sum_arr_delay")).withError(0.00001, .5)
    var rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) == 0)

    df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      relative_error("sum_arr_delay")).withError(0.00001, .5)
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) == 0)

    df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      upper_bound("sum_arr_delay")).withError(0.00001, .5)
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).isNullAt(2))


    df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      lower_bound("sum_arr_delay")).withError(0.00001, .5)
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).isNullAt(2))

    // test multiple aggregate functions & error estimates


    df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      lower_bound("sum_arr_delay"), avg("arrdelay").alias("avg_arr_delay"),
      lower_bound("avg_arr_delay") ).withError(0.00001, .5)
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 5)
    assert(rows(0).isNullAt(2))
    assert(rows(0).isNullAt(4))

  }

  // No need to run in precheckin
  ignore("test for no routing on sampledataframe on error estimates failure") {

    var df2 =
      sampleDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
        absolute_error("sum_arr_delay")).withError(.5, .5, "RUN_ON_FULL_TABLE")
    var rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) > 0)

    df2 = sampleDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      relative_error("sum_arr_delay")).withError(.5, .5, "RUN_ON_FULL_TABLE")
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) > 0)

    df2 = sampleDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      upper_bound("sum_arr_delay")).withError(.5, .5, "RUN_ON_FULL_TABLE")
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) > 0)


    df2 = sampleDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      lower_bound("sum_arr_delay")).withError(.5, .5, "RUN_ON_FULL_TABLE")
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 3)
    assert(rows(0).getDouble(2) > 0)

    // test multiple aggregate functions & error estimates


    df2 = sampleDataFrame.groupBy("UniqueCarrier").agg(sum("arrdelay").alias("sum_arr_delay"),
      lower_bound("sum_arr_delay"), avg("arrdelay").alias("avg_arr_delay"),
      lower_bound("avg_arr_delay") ).withError(.5, .5, "RUN_ON_FULL_TABLE")
    rows = df2.collect()
    assert(rows.length > 1)
    assert(rows(0).schema.length == 5)
    assert(rows(0).getDouble(2) > 0)
    assert(rows(0).getDouble(4) > 0)
    assert(rows(0).getDouble(4) !=  rows(0).getDouble(2))
  }

  // does not look like we will be able to support it without making changes in spark
  ignore("sample data frame api test for error estimates retrieval without alias") {

    val df2 = mainTableDataFrame.agg(Map("ArrDelay" -> "avg",
      "avg(ArrDelay)" -> "absolute_error")).withError(.5, .5)
    val rows = df2.collect()
    assert(rows.length == 1)
    assert(rows(0).schema.length == 2)
    assert(rows(0).getDouble(1) > 0)
  }


  test("Test consistency of sum , count & average queries") {

    sampleDataFrame.cache()
    var df2 = mainTableDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
      absolute_error("sum_arr_delay")).withError(.5, .5)
    val sum1 = df2.collect()(0).getLong(0)


    df2 = mainTableDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
      relative_error("sum_arr_delay")).withError(.5, .5)
    val sum2 = df2.collect()(0).getLong(0)

    assert(sum1 === sum2)

    df2 = mainTableDataFrame.agg(avg("arrdelay").alias("avg_arr_delay"),
      absolute_error("avg_arr_delay")).withError(.5, .5)
    val avg1 = df2.collect()(0).getDouble(0)


    df2 = mainTableDataFrame.agg(avg("arrdelay").alias("avg_arr_delay"),
      relative_error("avg_arr_delay")).withError(.5, .5)
    val avg2 = df2.collect()(0).getDouble(0)

    assert(avg1 === avg2)


    df2 = mainTableDataFrame.agg(count("arrdelay").alias("count_arr_delay"),
      absolute_error("count_arr_delay")).withError(.5, .5)
    val count1 = df2.collect()(0).getLong(0)


    df2 = mainTableDataFrame.agg(count("arrdelay").alias("count_arr_delay"),
      relative_error("count_arr_delay")).withError(.5, .5)
    val count2 = df2.collect()(0).getLong(0)

    assert(count1 === count2)



  }

  // This test will fail because the data frame returned by create table is useless.
  ignore("sample data frame api test for data frames created using DataSource API") {
    this.initTestDataWithDataSource()
    this.executeTest()
  }






  test("test HAC local_omit option with base data frame") {


    val result = mainTableDataFrame.agg(avg("arrdelay").alias("avg_arr_delay"),
      absolute_error("avg_arr_delay"), relative_error("avg_arr_delay")).withError(.00001, .95,
      "LOCAL_OMIT")
    logInfo(result.collect().mkString("\n"))

    if (!result.collect()(0).anyNull) {
      assert(result.collect()(0).getDouble(0).isNaN)
      assert(result.collect()(0).getDouble(1).isNaN)
      assert(result.collect()(0).getDouble(2).isNaN)
    }
  }

  test("test group by queries with limit not throwing exception if the relative error for valid " +
    "rows is within limit") {
    val df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("distance").
        alias("sum_dist"), relative_error("sum_dist")).orderBy(desc("sum_dist")).
        limit(5).withError(.075, .5, "strict")
    logInfo(df2.collect().mkString("\n"))
  }

  test("test bug absolute error 0 for closedform") {

    val df2 = mainTableDataFrame.groupBy("UniqueCarrier").agg(sum("distance").alias("sum_dist"),
      absolute_error("sum_dist")).orderBy(desc("sum_dist")).limit(5).withError(.075, .5, "strict")
    val rows = df2.collect()
    rows.foreach(row => assert(!row.isNullAt(1) && row.getLong(1) != 0)  )
  }

  test("test HAC local_omit option sample dataframe") {
    val result = sampleDataFrame.agg(avg("arrdelay").alias("avg_arr_delay"),
      lower_bound("avg_arr_delay"), upper_bound("avg_arr_delay")).
      withError(.00001, .95, "LOCAL_OMIT")
    logInfo(result.collect().mkString("\n"))

    if (!result.collect()(0).anyNull) {
      assert(result.collect()(0).getDouble(0).isNaN)
      assert(result.collect()(0).getDouble(1).isNaN)
      assert(result.collect()(0).getDouble(2).isNaN)
    }
  }

  test("test correct setting of error fraction on queries fired on base data frame" +
    " & sample dataframe") {

    var df2 = mainTableDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
      absolute_error("sum_arr_delay")).withError(0.5, 0.5)
    var error = AQPTestUtils.getError(df2.queryExecution.executedPlan)
    assert(0.5d === error)

    df2 = sampleDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
      absolute_error("sum_arr_delay")).withError(.5, .5)
    error = AQPTestUtils.getError(df2.queryExecution.executedPlan)
    assert(0.5d === error)

    df2 = sampleDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
      absolute_error("sum_arr_delay"))
    error = AQPTestUtils.getError(df2.queryExecution.executedPlan)
    assert(ReplaceWithSampleTable.INIFINITE_ERROR_TOLERANCE === error)
  }

  test("test validation of error fraction & confidence on queries fired" +
      " on base data frame & sample dataframe without datasource API ") {

    try {
      mainTableDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
        absolute_error("sum_arr_delay")).withError(-0.2, 0.95)
    }
    catch {
      case e: UnsupportedOperationException => assert(e.toString.indexOf(
        "error and confidence within range of 0 to 1") != -1, e.toString)
       logInfo("Success...error fraction should be in the range 0 < error < 1.0")
    }
  try {
    sampleDataFrame.agg(sum("arrdelay").alias("sum_arr_delay"),
      absolute_error("sum_arr_delay")).withError(.5, -0.95)
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

    var rows = finalDF.collect()
    assert(rows.length == 1)
    val totalApproxSum1 = rows(0).getLong(0)

    rows = snc.sql(s"Select sum(arrdelay) from  $mainTable with error .5").collect()
    assert(rows.length == 1)
    val totalApproxSum3 = rows(0).getLong(0)
    assert(math.abs(totalApproxSum1 - totalApproxSum3) < 10)

    val x = mainTableDataFrame.agg(Map("arrdelay" -> "sum"))
    rows = x.collect()
    assert(rows.length == 1)
    val totalExactSum = rows(0).getLong(0)
    assert(math.abs(totalExactSum - totalApproxSum1) > 100)

    // check if strict error validation works
    try {
      val x = mainTableDataFrame.agg(Map("arrdelay" -> "sum"))
        .withError(.00001, .95, "STRICT")
      logInfo(x.collect().mkString("\n"))
      fail("should have got error limit exceeded exception")
    } catch {
      case e: Exception if e.toString.indexOf("ErrorLimitExceededException") != -1 =>  // OK
    }

  }

  def msg(m: String): Unit = DebugUtils.msg(m)
}
