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

import java.lang.management.ManagementFactory
import java.sql.Date
import java.text.SimpleDateFormat

import scala.util.Random

import com.gemstone.gemfire.internal.AvailablePort
import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.junit.Assert._
import org.scalatest.tagobjects.Retryable
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.bootstrap.DeclarativeBootstrapAggregateFunction
import org.apache.spark.sql.execution.closedform.ClosedFormErrorEstimate
import org.apache.spark.sql.execution.common.{AQPRules, AnalysisType, ErrorAggregateFunction, ReplaceWithSampleTable, SnappyHashAggregate, SnappySortAggregate}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildLeft, BuildRight}
import org.apache.spark.sql.execution.{DataSourceScanExec, SparkPlan}
import org.apache.spark.sql.sampling.ColumnFormatSamplingRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{AssertAQPAnalysis, Logging, SparkConf}

abstract class ErrorEstimateFunctionTest
  extends SnappyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  val DEFAULT_ERROR: Double = Constants.DEFAULT_ERROR

  val lineTable: String = getClass.getSimpleName + "_" + "line"
  val mainTable: String = getClass.getSimpleName + "_" + "mainTable"
  val sampleTable: String = getClass.getSimpleName + "_" + "mainTable_sampled"
  val airline_sampleTable2: String = "airline_sampled2"
  val airlineRefTable = "airlineref"
  val airlineMainTable: String = "airline"
  val airlineSampleTable: String = "airline_sampled"
  var airlineSampleDataFrame: DataFrame = _
  var airlineMainTableDataFrame: DataFrame = _
  var airLineCodeDataFrame: DataFrame = _
  // Set up sample & Main table
  var hfile: String = getClass.getResource("/2015.parquet").getPath
  val codetableFile: String = getClass.getResource("/airlineCode_Lookup.csv").getPath

  // Set up sample & Main table
  val LINEITEM_DATA_FILE: String = getClass.getResource("/datafile.tbl").getPath
  val LINEITEM_DATA_FILE_1: String = getClass.getResource("/datafile-1.tbl").getPath

  override def beforeAll(): Unit = {
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

  protected override def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
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
      .setMaster("local[6]")
      .set("spark.sql.hive.metastore.sharedPrefixes",
        "com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc," +
          "com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity," +
          "com.mapr.fs.jni,org.apache.commons")
      .set("master.ui.port", "" + AvailablePort.getRandomAvailablePort(0))
      .set("spark.sql.codegen.maxFields", maxCodeGen.toString)
      .set(Property.TestDisableCodeGenFlag.name, "true")
      .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")
    addSpecificProps(conf)
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  protected def addSpecificProps(conf: SparkConf): Unit

  protected def initTestTables(): Unit = {
    val snc = this.snc
    createLineitemTable(snc, lineTable)
    val mainTableDF = createLineitemTable(snc, mainTable)
    snc.createSampleTable(sampleTable, Some(mainTable),
      Map(
        "qcs" -> "l_quantity",
        "fraction" -> "0.01",
        "strataReservoirSize" -> "50"),
      allowExisting = false)
    this.initAirlineTables()
  }

  protected def initAirlineTables(): Unit = {
    snc.sql(s"drop table  if exists $airlineSampleTable")
    snc.sql(s"drop table  if exists $airlineRefTable")
    snc.sql(s"drop table  if exists $airline_sampleTable2")
    snc.sql(s"drop table  if exists $airlineMainTable")
    snc.sql(s"drop table  if exists airlinestaging")
    snc.sql(s"drop table  if exists airlinerefstaging")

    val stagingDF = snc.read.load(hfile)
    snc.createTable("airlinestaging", "column", stagingDF.schema,
      Map.empty[String, String])
    stagingDF.write.mode(SaveMode.Ignore).saveAsTable("airlinestaging")

    airlineMainTableDataFrame = snc.createTable(airlineMainTable, "column",
      stagingDF.schema, Map.empty[String, String])
    stagingDF.write.mode(SaveMode.Append).saveAsTable(airlineMainTable)

    airlineSampleDataFrame = snc.createSampleTable(airlineSampleTable,
      Some(airlineMainTable), Map("qcs" -> "UniqueCarrier,YearI,MonthI",
        "fraction" -> "0.06", "strataReservoirSize" -> "1000"),
      allowExisting = false)

    snc.createSampleTable(airline_sampleTable2,
      Some(airlineMainTable), Map("qcs" -> "round(tan(ActualElapsedTime))",
        "fraction" -> "0.5", "strataReservoirSize" -> "1000"),
      allowExisting = false)

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

  test("Ensuring the query works for count which implies non tungsten " +
    "aggregate is not getting created") {

    val result = snc.sql(s"SELECT count(l_quantity) as x, absolute_error(x) FROM $sampleTable ")
    this.assertAnalysis()
    val rows2 = result.collect()
    val estimate = rows2(0).getLong(0)
    msg("estimate=" + estimate)
  }

  test("Ensuring the query works for avg which implies non tungsten " +
    "aggregate is not getting created") {
    val result = snc.sql(s"SELECT avg(l_quantity) as x, absolute_error(x) FROM $sampleTable")
    this.assertAnalysis()
    val rows2 = result.collect()
    val estimate = rows2(0).getDouble(0)
    msg("estimate=" + estimate)
  }

  test("Ensuring the query works for sum which implies non tungsten " +
    "aggregate is not getting created ") {
    val result = snc.sql(s"SELECT sum(l_quantity) as x, absolute_error(x) FROM $sampleTable")
    this.assertAnalysis()
    val rows2 = result.collect()
    val estimate = rows2(0).getDouble(0)
    msg("estimate=" + estimate)
    assert(estimate ===
      (17 + 36 + 8 + 28 + 24 + 32 + 38 + 45 + 49 + 27 + 2 + 28 + 26))
  }

  test("basic sanity test") {
    val result = snc.sql("SELECT avg(l_quantity) as x, lower_bound(x), " +
      "upper_bound(x), l_orderkey, absolute_error(x), " +
      s"relative_error(x) FROM $mainTable group by l_orderkey " +
      "having avg(l_quantity) > 25 order by  l_orderkey " +
      s"with error $DEFAULT_ERROR confidence .95")
    // avg(l_quantity)

    val schema = result.schema
    assert("x" === schema.fieldNames(0))
    assert("Lower_Bound(x)" === schema.fieldNames(1))
    assert("Upper_Bound(x)" === schema.fieldNames(2))
    assert("l_orderkey" === schema.fieldNames(3))
    assert("Absolute_Error(x)" === schema.fieldNames(4))
    assert("Relative_Error(x)" === schema.fieldNames(5))
    result.collect()
    this.assertAnalysis()
  }

  test("Test sample table query on  aggregates with just sum & average") {
    val resultY = snc.sql(s"SELECT sum(l_quantity) as x, l_orderkey, absolute_error(x) " +
      s" FROM $mainTable  group by l_orderkey " +
      s" with error $DEFAULT_ERROR confidence .95  ")
    /*
    val resultX = snc.sql(s"SELECT avg(l_quantity) as x, l_orderkey " +
      s"FROM $mainTable  group by l_orderkey  having avg(l_quantity) > 25")
   */
    resultY.collect()

    val schema = resultY.schema
    assert("x" === schema.fieldNames(0))
    assert("l_orderkey" === schema.fieldNames(1))

    this.assertAnalysis()

    val resultZ = snc.sql(s"SELECT avg(l_quantity) as x, l_orderkey , " +
      s"absolute_error(x) FROM $mainTable  group by l_orderkey " +
      s" with error $DEFAULT_ERROR confidence .95  ")
    /*
    val resultX = snc.sql(s"SELECT avg(l_quantity) as x, l_orderkey " +
      s"FROM $mainTable  group by l_orderkey  having avg(l_quantity) > 25")
    */
    resultZ.collect()
    this.assertAnalysis()
  }

  test("Test sample table query on  aggregates with having clause containing " +
    "aggregate function") {
    val resultX = snc.sql(s"SELECT avg(l_quantity) as x, l_orderkey, absolute_error(x)  FROM " +
      s"$mainTable group by l_orderkey  having avg(l_quantity) > 25 " +
      s"with error $DEFAULT_ERROR confidence .95  ")
    /*
    val resultX = snc.sql(s"SELECT avg(l_quantity) as x, l_orderkey  FROM " +
        s"$mainTable  group by l_orderkey  having avg(l_quantity) > 25 ")
    */
    resultX.collect()
    val schema = resultX.schema
    assert("x" === schema.fieldNames(0))
    assert("l_orderkey" === schema.fieldNames(1))

    this.assertAnalysis()
  }

  test("Test sample table query on  aggregates with having clause containing " +
    "error estimate function") {
    this.testErrorEstimateInHavingClause()
  }

  def testErrorEstimateInHavingClause() {
    val resulty = snc.sql(s"SELECT avg(l_quantity) as x, absolute_error(x), " +
      s" l_orderkey FROM $mainTable group by l_orderkey " +
      s" having absolute_error(x) >= 0 with error $DEFAULT_ERROR confidence .95")
    resulty.collect()
    val schema = resulty.schema
    assert("x" === schema.fieldNames(0))
    assert("Absolute_Error(x)" === schema.fieldNames(1))
    assert("l_orderkey" === schema.fieldNames(2))

    this.assertAnalysis()
  }

  test("Test sample table query on mixed aggregates with having clause on " +
    "aggregate function") {
    val resultY = snc.sql(s"SELECT sum(l_quantity) as x, l_orderkey, absolute_error(x) " +
      s" FROM $mainTable group by l_orderkey with error $DEFAULT_ERROR confidence .95")
    resultY.collect()
    this.assertAnalysis()

    val resultX = snc.sql(s"SELECT avg(l_quantity) as x, l_orderkey, absolute_error(x) " +
      s"FROM $mainTable  group by l_orderkey having avg(l_quantity) > 25 " +
      s" with error $DEFAULT_ERROR confidence .95")
    resultX.collect()
    this.assertAnalysis()

    val result = snc.sql(s"SELECT avg(l_quantity) as x, lower_bound(x), " +
      s"upper_bound(x), l_orderkey , absolute_error(x), " +
      s"relative_error(x) FROM $mainTable group by l_orderkey " +
      "having avg(l_quantity) > 25 order by  l_orderkey " +
      s"with error $DEFAULT_ERROR confidence .95")
    // avg(l_quantity)

    val schema = result.schema
    assert("x" === schema.fieldNames(0))
    assert("Lower_Bound(x)" === schema.fieldNames(1))
    assert("Upper_Bound(x)" === schema.fieldNames(2))
    assert("l_orderkey" === schema.fieldNames(3))

    assert("Absolute_Error(x)" === schema.fieldNames(4))
    assert("Relative_Error(x)" === schema.fieldNames(5))

    this.assertAnalysis()

    val rows = result.collect()
    assert(rows.length === 2)

    val result1 = snc.sql(s"SELECT avg(l_quantity) as x, l_orderkey, absolute_error(x) " +
      s" FROM $mainTable group by l_orderkey having l_orderkey >= 2 " +
      s"order by l_orderkey with error $DEFAULT_ERROR confidence 0.95")
    val rows1 = result1.collect()
    AssertAQPAnalysis.bootStrapAnalysis(snc)

    assert(rows(0).getDouble(0) == rows1(0).getDouble(0))
    assert(rows(0).getInt(3) == rows1(0).getInt(1))
    assert(rows(0).getDouble(0) > 25)

    assert(rows(1).getDouble(0) == rows1(1).getDouble(0))
    assert(rows(1).getInt(3) == rows1(1).getInt(1))
    assert(rows(1).getDouble(0) > 25)
  }

  test("Test sample table query on mixed aggregates with having clause on a column") {
    val result = snc.sql(s"SELECT avg(l_quantity) as x, lower_bound(x) , " +
      s"upper_bound(x), l_orderkey , absolute_error(x), " +
      s"relative_error(x) FROM $mainTable  group by l_orderkey " +
      "having l_orderkey >= 2 order by  l_orderkey" +
      s" with error $DEFAULT_ERROR confidence .95  ")

    val schema = result.schema
    assert("x" === schema.fieldNames(0))
    assert("Lower_Bound(x)" === schema.fieldNames(1))
    assert("Upper_Bound(x)" === schema.fieldNames(2))
    assert("l_orderkey" === schema.fieldNames(3))

    assert("Absolute_Error(x)" === schema.fieldNames(4))
    assert("Relative_Error(x)" === schema.fieldNames(5))

    AssertAQPAnalysis.bootStrapAnalysis(snc)

    val rows = result.collect()
    assert(rows.length === 2)

    val result1 = snc.sql(s"SELECT avg(l_quantity) as x , l_orderkey, relative_error(x) " +
      s" FROM $mainTable group by l_orderkey  having l_orderkey >= 2 " +
      s"order by l_orderkey with error $DEFAULT_ERROR confidence 0.95")
    val rows1 = result1.collect()
    AssertAQPAnalysis.bootStrapAnalysis(snc)

    assert(rows(0).getDouble(0) == rows1(0).getDouble(0))
    assert(rows(0).getInt(3) == rows1(0).getInt(1))

    assert(rows(1).getDouble(0) == rows1(1).getDouble(0))
    assert(rows(1).getInt(3) == rows1(1).getInt(1))
  }

  test("Test sample table query on mixed aggregates with group by and order by") {
    this.testMixedAggregatesWithGroupByAndOrderBy()
  }

  test("Test sample table query on mixed aggregates") {
    this.testMixedAggregates()
  }

  test("Test sample table query on mixed aggregates with group by") {
    this.testMixedAggregatesWithGroupBy()
  }

  test("Test SNAP-696") {
    val result = snc.sql(s"select l_orderkey, avg(l_quantity) as x,lower_bound(x) " +
      s"from $mainTable group by l_orderkey order by avg(l_quantity) desc " +
      s"limit 10 with error $DEFAULT_ERROR confidence .95 ")

    val rows = result.collect()
    val schema = rows(0).schema
    assert("l_orderkey" === schema.fieldNames(0))
    assert("x" === schema.fieldNames(1))
    assert("Lower_Bound(x)" === schema.fieldNames(2))
    assert(Math.abs(rows(0).getDouble(2)) > 0)
  }

  def testMixedAggregatesWithGroupByAndOrderBy(): Unit = {
    val result = snc.sql(s"SELECT sum(l_quantity) as x , avg(l_quantity) as y, " +
      s"lower_bound(x), count(l_quantity) as z, l_orderkey, upper_bound(z) " +
      s"FROM $mainTable group by l_orderkey order by l_orderkey " +
      s"with error $DEFAULT_ERROR confidence 0.95")

    val schema = result.schema
    assert("x" === schema.fieldNames(0))
    assert("y" === schema.fieldNames(1))
    assert("Lower_Bound(x)" === schema.fieldNames(2))
    assert("z" === schema.fieldNames(3))

    assert("l_orderkey" === schema.fieldNames(4))

    assert("Upper_Bound(z)" === schema.fieldNames(5))

    this.assertAnalysis()
    val rows = result.collect()
    assert(rows.length === 3)
    rows(0).getDouble(5)

    val result1 = snc.sql(s"SELECT sum(l_quantity) as x , l_orderkey, lower_bound(x) " +
      s" FROM $mainTable group by l_orderkey order by  l_orderkey " +
      s"with error $DEFAULT_ERROR confidence 0.95")

    val rows1 = result1.collect()
    this.assertAnalysis()
    val result2 = snc.sql(s"SELECT avg(l_quantity) as x , l_orderkey, relative_error(x) " +
      s" FROM $mainTable group by l_orderkey  order by  l_orderkey " +
      s"with error $DEFAULT_ERROR confidence 0.95")
    val rows2 = result2.collect()
    this.assertAnalysis()

    val result3 = snc.sql(s"SELECT count(l_quantity) as x , l_orderkey, upper_bound(x) " +
      s" FROM $mainTable group by l_orderkey order by  l_orderkey " +
      s"with error $DEFAULT_ERROR confidence 0.95")
    val rows3 = result3.collect()
    this.assertAnalysis()

    assert(rows(0).getDouble(0) == rows1(0).getDouble(0))
    assert(rows(0).getDouble(1) == rows2(0).getDouble(0))
    assert(Math.abs(rows(0).getDouble(2) - rows1(0).getDouble(2)) < 2)
    assert(rows(0).getLong(3) == rows3(0).getLong(0))
    assert(rows(0).getInt(4) == rows1(0).getInt(1))
    assert(Math.abs(rows(0).getDouble(5) - rows3(0).getDouble(2)) < 2)

    assert(rows(1).getDouble(0) == rows1(1).getDouble(0))
    assert(rows(1).getDouble(1) == rows2(1).getDouble(0))
    assert(Math.abs(rows(1).getDouble(2) - rows1(1).getDouble(2)) < 2)
    assert(rows(1).getLong(3) == rows3(1).getLong(0))
    assert(rows(1).getInt(4) == rows2(1).getInt(1))
    assert(Math.abs(rows(1).getDouble(5) - rows3(1).getDouble(2)) < 2)

    assert(rows(2).getDouble(0) == rows1(2).getDouble(0))
    assert(rows(2).getDouble(1) == rows2(2).getDouble(0))
    assert(Math.abs(rows(2).getDouble(2) - rows1(2).getDouble(2)) < 2)
    assert(rows(2).getLong(3) == rows3(2).getLong(0))
    assert(rows(2).getInt(4) == rows3(2).getInt(1))
    assert(Math.abs(rows(2).getDouble(5) - rows3(2).getDouble(2)) < 2)
  }

  def testMixedAggregatesWithGroupBy(): Unit = {
    val result = snc.sql(s"SELECT sum(l_quantity) as x , avg(l_quantity) as y, " +
      s"count(l_quantity) as z, l_orderkey, relative_error(x) FROM $mainTable " +
      s"group by l_orderkey with error $DEFAULT_ERROR confidence 0.95")

    val schema = result.schema
    assert("x" === schema.fieldNames(0))
    assert("y" === schema.fieldNames(1))

    assert("z" === schema.fieldNames(2))

    assert("l_orderkey" === schema.fieldNames(3))

    this.assertAnalysis()
    val rows = result.collect()
    assert(rows.length === 3)
    rows(0).getDouble(1)

    val result1 = snc.sql(s"SELECT sum(l_quantity) as x , l_orderkey, relative_error(x) " +
      s" FROM $mainTable group by l_orderkey  with error $DEFAULT_ERROR confidence 0.95")
    val rows1 = result1.collect()
    this.assertAnalysis()
    val result2 = snc.sql(s"SELECT avg(l_quantity) as x , l_orderkey, relative_error(x) " +
      s" FROM $mainTable group by l_orderkey  with error $DEFAULT_ERROR confidence 0.95")
    val rows2 = result2.collect()
    this.assertAnalysis()
    val result3 = snc.sql(s"SELECT count(l_quantity) as x , l_orderkey, relative_error(x) " +
      s" FROM $mainTable group by l_orderkey  with error $DEFAULT_ERROR confidence 0.95")
    val rows3 = result3.collect()
    this.assertAnalysis()

    assert(rows(0).getDouble(0) == rows1(0).getDouble(0))
    assert(rows(0).getDouble(1) == rows2(0).getDouble(0))
    assert(rows(0).getLong(2) == rows3(0).getLong(0))
    assert(rows(0).getInt(3) == rows1(0).getInt(1))

    assert(rows(1).getDouble(0) == rows1(1).getDouble(0))
    assert(rows(1).getDouble(1) == rows2(1).getDouble(0))
    assert(rows(1).getLong(2) == rows3(1).getLong(0))
    assert(rows(1).getInt(3) == rows2(1).getInt(1))

    assert(rows(2).getDouble(0) == rows1(2).getDouble(0))
    assert(rows(2).getDouble(1) == rows2(2).getDouble(0))
    assert(rows(2).getLong(2) == rows3(2).getLong(0))
    assert(rows(2).getInt(3) == rows3(2).getInt(1))
  }

  def testMixedAggregates(): Unit = {
    val result = snc.sql(s"SELECT avg(l_quantity) as x, sum(l_quantity) as y, " +
      s"count(l_quantity) as z, lower_bound(x), upper_bound(x), " +
      s"absolute_error(x), relative_error(x), lower_bound(y), " +
      s"upper_bound(y), absolute_error(y), relative_error(y), " +
      s"lower_bound(z), upper_bound(z), absolute_error(z), relative_error(z) " +
      s"FROM $mainTable  with error $DEFAULT_ERROR confidence .95")

    val schema1 = result.schema
    assert("x" === schema1.fieldNames(0))
    assert("y" === schema1.fieldNames(1))
    assert("z" === schema1.fieldNames(2))
    assert("Lower_Bound(x)" === schema1.fieldNames(3))
    assert("Upper_Bound(x)" === schema1.fieldNames(4))
    assert("Absolute_Error(x)" === schema1.fieldNames(5))
    assert("Relative_Error(x)" === schema1.fieldNames(6))
    assert("Lower_Bound(y)" === schema1.fieldNames(7))
    assert("Upper_Bound(y)" === schema1.fieldNames(8))
    assert("Absolute_Error(y)" === schema1.fieldNames(9))
    assert("Relative_Error(y)" === schema1.fieldNames(10))
    assert("Lower_Bound(z)" === schema1.fieldNames(11))
    assert("Upper_Bound(z)" === schema1.fieldNames(12))
    assert("Absolute_Error(z)" === schema1.fieldNames(13))
    assert("Relative_Error(z)" === schema1.fieldNames(14))

    this.assertAnalysis()
    val rows = result.collect()
    val schema = rows(0).schema
    assert(schema.length == 15)
    assert(rows.length == 1)

    var result1 = snc.sql(s"SELECT avg(l_quantity) as x , lower_bound(x)," +
      s"upper_bound(x), absolute_error(x), " +
      s"relative_error(x)" +
      s"  FROM $mainTable  with error $DEFAULT_ERROR confidence .95 ")
    var rows1 = result1.collect()
    this.assertAnalysis()
    assert(rows(0).getDouble(0) == rows1(0).getDouble(0))
    assert(rows(0).getDouble(3) == rows1(0).getDouble(1))
    assert(rows(0).getDouble(4) == rows1(0).getDouble(2))
    assert(rows(0).getDouble(5) == rows1(0).getDouble(3))
    assert(rows(0).getDouble(6) == rows1(0).getDouble(4))
    // because we know that individually the bootstrap bounds are calculated correctly,
    // we can compare it with those to see if the mixed results are correct.

    result1 = snc.sql(s"SELECT sum(l_quantity) as x , lower_bound(x)," +
      s"upper_bound(x), absolute_error(x), " +
      s"relative_error(x)" +
      s"  FROM $mainTable  with error $DEFAULT_ERROR confidence .95 ")
    rows1 = result1.collect()
    this.assertAnalysis()
    assert(rows(0).getDouble(1) == rows1(0).getDouble(0))
    assert(rows(0).getDouble(7) == rows1(0).getDouble(1))
    assert(rows(0).getDouble(8) == rows1(0).getDouble(2))
    assert(rows(0).getDouble(9) == rows1(0).getDouble(3))
    assert(rows(0).getDouble(10) == rows1(0).getDouble(4))


    result1 = snc.sql(s"SELECT count(l_quantity) as x , lower_bound(x)," +
      s"upper_bound(x), absolute_error(x), " +
      s"relative_error(x)" +
      s"  FROM $mainTable  with error $DEFAULT_ERROR confidence .95 ")
    rows1 = result1.collect()
    this.assertAnalysis()
    assert(rows(0).getLong(2) == rows1(0).getLong(0))
    assert(rows(0).getDouble(11) == rows1(0).getDouble(1))
    assert(rows(0).getDouble(12) == rows1(0).getDouble(2))
    assert(rows(0).getDouble(13) == rows1(0).getDouble(3))
    assert(rows(0).getDouble(14) == rows1(0).getDouble(4))
  }

  test("Sample Table Query on avg aggregate with error estimates") {
    val result = snc.sql(s"SELECT avg(l_quantity) as x, lower_bound(x) , " +
      s"upper_bound(x), absolute_error(x), " +
      s"relative_error(x) FROM $mainTable  with error $DEFAULT_ERROR confidence .95 ")

    this.assertAnalysis()
    val rows2 = result.collect()
    val estimate = rows2(0).getDouble(0)
    msg("estimate=" + estimate)

    assert(Math.abs(estimate -
      (17 + 36 + 8 + 28 + 24 + 32 + 38 + 45 + 49 + 27 + 2 + 28 + 26) / 13) < .8)
    msg("lower bound=" + rows2(0).getDouble(1) + ", upper bound" +
      rows2(0).getDouble(2) + ", absolute error =" + rows2(0).getDouble(3) +
      ", relative error =" + rows2(0).getDouble(4))
  }

  test("Sample Table Query on avg aggregate on integer column with error estimates ") {
    val result = snc.sql(s"SELECT avg(l_linenumber) as x, lower_bound(x) , " +
      s"upper_bound(x), absolute_error(x), " +
      s"relative_error(x) FROM $mainTable  with error $DEFAULT_ERROR confidence .95 ")

    result.collect()
    this.assertAnalysis()
  }


  test("Sample Table Query on Sum aggregate with error estimates should be correct") {
    val result = snc.sql(s"SELECT sum(l_quantity) as x, lower_bound(x) , " +
      s"upper_bound(x), absolute_error(x), " +
      s"relative_error(x) FROM $mainTable  with error $DEFAULT_ERROR confidence .95 ")

    this.assertAnalysis()
    val rows2 = result.collect()
    val estimate = rows2(0).getDouble(0)
    msg("estimate=" + estimate)

    assert(estimate === (17 + 36 + 8 + 28 + 24 + 32 + 38 + 45 + 49 + 27 + 2 + 28 + 26))
    msg("lower bound=" + rows2(0).getDouble(1) + ", upper bound" + rows2(0).getDouble(2)
      + ", absolute error =" + rows2(0).getDouble(3) +
      ", relative error =" + rows2(0).getDouble(4))

  }

  test("Sample Table Query on Sum aggregate  should be correct") {
    val result = snc.sql(s"SELECT sum(l_quantity) as T, lower_bound(T), " +
      s"upper_bound(T)  FROM $mainTable with error $DEFAULT_ERROR confidence 0.95")

    this.assertAnalysis()
    val rows2 = result.collect()
    val estimate = rows2(0).getDouble(0)
    msg("estimate=" + estimate)

    assert(estimate === (17 + 36 + 8 + 28 + 24 + 32 + 38 + 45 + 49 + 27 + 2 + 28 + 26))
    msg("lower bound=" + rows2(0).getDouble(1) + ", upper bound" + rows2(0).getDouble(2))
  }

  test("Sample Table Query on Sum aggregate with alias should be correct") {
    val result = snc.sql(s"SELECT sum(l_quantity) as T, lower_bound(T) lower, " +
      s"upper_bound(T) upper, absolute_error(T) as abs_err, " +
      s"relative_error(T) as rel_err FROM $mainTable " +
      s"with error $DEFAULT_ERROR confidence 0.95")

    this.assertAnalysis()
    val rows2 = result.collect()
    val estimate = rows2(0).getAs[Double]("t")
    msg("estimate=" + estimate)

    assert(estimate === (17 + 36 + 8 + 28 + 24 + 32 + 38 + 45 + 49 + 27 + 2 + 28 + 26))
    msg("lower bound=" + rows2(0).getAs[Double]("lower") + ", upper bound"
      + rows2(0).getAs[Double]("upper") + ", abs err" + rows2(0).getAs[Double]("abs_err")
      + ", rel err" + rows2(0).getAs[Double]("rel_err"))
  }

  test("Sample Table Query alias on Sum aggregate  should be correct") {
    val result = snc.sql(s"SELECT sum(l_quantity) as T, relative_error(T) FROM $mainTable " +
      s"with error $DEFAULT_ERROR confidence 0.95")

    this.assertAnalysis()
    val rows2 = result.collect()
    // val estimate = rows2(0).getDouble(0)
    assert(rows2(0).schema.head.name === "t")
  }

  test("Sample Table Query alias on Sum aggregate with group by clause " +
    "should be correct") {
    val result = snc.sql(s"SELECT sum(l_quantity) as T, l_orderkey, relative_error(T) " +
      s"FROM $mainTable group by l_orderkey order by l_orderkey " +
      s"with error $DEFAULT_ERROR confidence 0.95")
    this.assertAnalysis()
    val rows = result.collect()
    assert(rows.length === 3)
    val row1 = rows(0)
    val col11 = row1.getInt(1)
    val col12 = row1.getDouble(0)
    assert(col12 == row1.getAs[Double]("t"))
    assert(col11 === 1)
    assert(col12 === 145)

    val row2 = rows(1)
    val col21 = row2.getInt(1)
    val col22 = row2.getDouble(0)
    assert(col21 === 2)
    assert(col22 === 38)

    val row3 = rows(2)
    val col31 = row3.getInt(1)
    val col32 = row3.getDouble(0)
    assert(col31 === 3)
    assert(col32 === 177)
  }


  ignore("Bug SNAP-334 Sample Table Query alias on Sum aggregate with " +
    "group by clause should be correct") {
    val result = snc.sql(s"SELECT sum(l_quantity) as T, l_orderkey as orderKey " +
      s" FROM $mainTable group by orderKey with error $DEFAULT_ERROR confidence 0.95")

    this.assertAnalysis()
    val rows = result.collect()
    assert(rows.length === 3)
    val row1 = rows(0)
    val col11 = row1.getInt(1)
    val col12 = row1.getDouble(0)
    assert(col12 == row1.getAs[Double]("t"))
    assert(col11 === 1)
    assert(col12 === 145)

    val row2 = rows(1)
    val col21 = row2.getInt(1)
    val col22 = row2.getDouble(0)
    assert(col21 === 2)
    assert(col22 === 38)

    val row3 = rows(2)
    val col31 = row3.getInt(1)
    val col32 = row3.getDouble(0)
    assert(col31 === 3)
    assert(col32 === 177)
  }


  test("Sample Table Query with  alias for lower bound and upper bound   should  work correctly ") {
    val result = snc.sql(s"SELECT sum(l_quantity) as col1, lower_bound(col1) " +
      s"as col2 , upper_bound(col1) as col3 FROM $mainTable" +
      s" WITH ERROR " + 0.13 + " confidence " + 0.85)

    this.assertAnalysis()
    val rows = result.collect()
    val col1 = rows(0).getDouble(0)
    val col2 = rows(0).getDouble(1)
    val col3 = rows(0).getDouble(2)

    assert(col1 === rows(0).getAs[Double]("col1"))
    assert(col2 === rows(0).getAs[Double]("col2"))
    assert(col3 === rows(0).getAs[Double]("col3"))
  }

  test("Sample Table Query with multiple aggregate  on Sum aggregate " +
    "with group by clause  should be correct") {
    val result = snc.sql(s"SELECT sum(l_quantity) as T, l_orderkey, " +
      s"sum(l_linenumber), relative_error(T) FROM $mainTable group by l_orderkey " +
      s"order by l_orderkey with error $DEFAULT_ERROR confidence 0.95")

    this.assertAnalysis()
    val rows = result.collect()

    assert(rows.length === 3)
    val row1 = rows(0)
    val col11 = row1.getInt(1)
    val col12 = row1.getDouble(0)
    val col13 = row1.getLong(2)
    assert(col11 === 1)
    assert(col12 === 145)
    assert(col13 === 21)

    val row2 = rows(1)
    val col21 = row2.getInt(1)
    val col22 = row2.getDouble(0)
    val col23 = row2.getLong(2)
    assert(col21 === 2)
    assert(col22 === 38)
    assert(col23 === 1)

    val row3 = rows(2)
    val col31 = row3.getInt(1)
    val col32 = row3.getDouble(0)
    val col33 = row3.getLong(2)
    assert(col31 === 3)
    assert(col32 === 177)
    assert(col33 === 21)

  }


  test("Sample Table Query on Sum aggregate with hidden column should be correct") {
    val result = snc.sql(s"SELECT sum(l_quantity) as x, lower_bound(x) , " +
      s"upper_bound(x), absolute_error(x), " +
      s"relative_error(x) FROM $mainTable  with error $DEFAULT_ERROR " +
      s"confidence .95 ")

    this.assertAnalysis()
    val rows2 = result.collect()
    val estimate = rows2(0).getDouble(0)
    msg("estimate=" + estimate)

    assert(estimate ===
      (17 + 36 + 8 + 28 + 24 + 32 + 38 + 45 + 49 + 27 + 2 + 28 + 26))
    msg("lower bound=" + rows2(0).getDouble(1) + ", upper bound" +
      rows2(0).getDouble(2) + ", absolute error =" + rows2(0).getDouble(3) +
      ", relative error =" + rows2(0).getDouble(4))
  }

  test("Sample Table Query on Sum aggregate with aliases should be correct") {
    val result = snc.sql(s"SELECT l_orderkey as orderKey, sum(l_quantity) as x, lower_bound(x) , " +
      "upper_bound(x), absolute_error(x), " +
      s"relative_error(x) FROM $mainTable  group by l_orderkey with error $DEFAULT_ERROR " +
      s"confidence .95 ")

    this.assertAnalysis()
    val rows2 = result.collect()
    val estimate = rows2(0).getDouble(1)
    msg("estimate=" + estimate)
    msg("lower bound=" + rows2(0).getDouble(2) + ", upper bound=" + rows2(0).getDouble(3) +
      ", absolute error =" + rows2(0).getDouble(4) +
      ", relative error =" + rows2(0).getDouble(5))

  }

  test("query directly fired on sample table should not compute " +
    "error estimates if no error clause present") {
    val result = snc.sql(s"SELECT sum(l_quantity) as x , relative_error(x) FROM $sampleTable ")
    this.assertAnalysis()
    val rows2 = result.collect()
    val estimate = rows2(0).getDouble(0)
    msg("estimate=" + estimate)
    assert(estimate ===
      (17 + 36 + 8 + 28 + 24 + 32 + 38 + 45 + 49 + 27 + 2 + 28 + 26))
  }

  test("test correct setting of error fraction on queries fired on " +
    "base table & sample table") {
    var df = snc.sql("SELECT l_orderkey as orderKey, sum(l_quantity) as x, " +
      "lower_bound(x), upper_bound(x), absolute_error(x), relative_error(x) " +
      s"FROM $mainTable  group by l_orderkey  with error .5 confidence .95")
    var error = AQPTestUtils.getError(df.queryExecution.sparkPlan)
    assert(.5d === error)

    df = snc.sql(s"SELECT l_orderkey as orderKey, sum(l_quantity) as x, lower_bound(x) , " +
      "upper_bound(x), absolute_error(x), " +
      s"relative_error(x) FROM $mainTable  group by l_orderkey with error $DEFAULT_ERROR " +
      s"confidence .95 ")
    error = AQPTestUtils.getError(df.queryExecution.sparkPlan)
    assert(Constants.DEFAULT_ERROR === error)

    df = snc.sql(s"SELECT sum(l_quantity) as x, relative_error(x) FROM $sampleTable")
    error = AQPTestUtils.getError(df.queryExecution.sparkPlan)
    assert(ReplaceWithSampleTable.INIFINITE_ERROR_TOLERANCE === error)

    df = snc.sql(s"SELECT sum(l_quantity) as x, relative_error(x) FROM $sampleTable with error .7")
    error = AQPTestUtils.getError(df.queryExecution.sparkPlan)
    assert(0.7d === error)
  }

  test("test group by queries with limit not throwing exception if " +
    "the relative error for valid rows is within limit", Retryable) {
    val rs = snc.sql("SELECT SUM(distance) as sum, relative_error(sum), " +
      "upper_bound(sum) ,lower_bound(sum), UniqueCarrier FROM airline " +
      "group by UniqueCarrier  order by sum desc limit 5 with error .05 " +
      "confidence .95 behavior 'strict'")

    val rows = rs.collect()
    rows.foreach(row => assert(!row.isNullAt(1) && !row.isNullAt(2)))
  }

  test("test bug null error for bootstrap") {
    val rs = snc.sql("SELECT SUM(ArrDelay) as sum, relative_error(sum), " +
      "upper_bound(sum), UniqueCarrier FROM airline group by UniqueCarrier " +
      "order by sum with error .3 confidence .95 behavior 'do_nothing'")
    val rows = rs.collect()
    rows.foreach(row => assert(!row.isNullAt(1) && !row.isNullAt(2)))
  }

  test("test count accuracy for qcs with function") {
    val rs1 = snc.sql("SELECT count(*) as cnt, relative_error(cnt), " +
      "round(tan(ActualElapsedTime)) FROM airline group by round(tan(ActualElapsedTime)) " +
      "order by round(tan(ActualElapsedTime)) with error .3 behavior 'do_nothing'")
    val rows1 = rs1.collect()
    this.assertAnalysis()


    val rs2 = snc.sql("SELECT count(*) as cnt, round(tan(ActualElapsedTime)) FROM airline " +
      "group by round(tan(ActualElapsedTime)) order by round(tan(ActualElapsedTime))")

    val rows2 = rs2.collect()
    assert(rows1.length > 0)
    rows1.zip(rows2).foreach {
      case (row1, row2) => assert(Math.abs(row1.getLong(0) - row2.getLong(0)) < 2)
    }

  }

  test("bypass error calculation for no error estimates") {
    def checkResults(bypass: Array[Row], nobypass: Array[Row]): Unit = {
      bypass.zip(nobypass).foreach {
        case (row1, row2) =>
          for (i <- 0 until row1.length) {
            val schema = row1.schema
            schema.fields(i).dataType match {
              case IntegerType => assert(Math.abs(row1.getInt(i) - row2.getInt(i)) < 2)
              case StringType => assert(row1.getString(i) === row2.getString(i))
              case LongType => assert(Math.abs(row1.getLong(i) - row2.getLong(i)) < 2)
              case FloatType => assert(Math.abs(row1.getFloat(i) - row2.getFloat(i)) < 2)
              case DoubleType => assert(Math.abs(row1.getDouble(i) - row2.getDouble(i)) < 2)
              case _: DecimalType => assert(Math.abs(row1.getDecimal(i).
                subtract(row2.getDecimal(i)).doubleValue()) < 2)
            }
          }
      }
    }

    val rs1 = snc.sql("SELECT UniqueCarrier, count(*) as cnt FROM airline group by " +
      " UniqueCarrier order by UniqueCarrier with error .3 behavior 'do_nothing' ")
    val rows1 = rs1.collect()

    AssertAQPAnalysis.bypassErrorCalc(snc)
    val rs10 = snc.sql("SELECT UniqueCarrier, count(*) as cnt, absolute_error(cnt) " +
      " FROM airline group by " +
      " UniqueCarrier order by UniqueCarrier " +
      " with error .3 behavior 'do_nothing' ")
    val rows10 = rs10.collect()
    this.assertAnalysis()
    checkResults(rows1, rows10)

    val rs2 = snc.sql("SELECT UniqueCarrier, sum(ArrDelay) as summ FROM airline group by " +
      " UniqueCarrier order by UniqueCarrier with error .3 behavior 'do_nothing' ")
    val rows2 = rs2.collect()
    AssertAQPAnalysis.bypassErrorCalc(snc)
    val rs20 = snc.sql("SELECT UniqueCarrier, sum(ArrDelay) as summ, absolute_error(summ) " +
      " FROM airline group by " +
      " UniqueCarrier order by UniqueCarrier " +
      " with error .3 behavior 'do_nothing' ")
    val rows20 = rs20.collect()

    this.assertAnalysis()
    checkResults(rows2, rows20)


    val rs3 = snc.sql("SELECT UniqueCarrier, avg(ArrDelay) as avgg FROM airline group by " +
      " UniqueCarrier order by UniqueCarrier with error .3 behavior 'do_nothing' ")
    val rows3 = rs3.collect()
    AssertAQPAnalysis.bypassErrorCalc(snc)
    val rs30 = snc.sql("SELECT UniqueCarrier, avg(ArrDelay) as avgg, absolute_error(avgg) " +
      " FROM airline group by " +
      " UniqueCarrier order by UniqueCarrier " +
      " with error .3 behavior 'do_nothing' ")
    val rows30 = rs30.collect()
    this.assertAnalysis()
    checkResults(rows3, rows30)


    val rs4 = snc.sql("SELECT UniqueCarrier, avg(ArrDelay) as avgg, " +
      "sum(ArrDelay) as summ, " +
      "count(*) as countt  FROM airline group by " +
      " UniqueCarrier order by UniqueCarrier " +
      "with error .3 behavior 'do_nothing' ")

    val rows4 = rs4.collect()
    AssertAQPAnalysis.bypassErrorCalc(snc)

    val rs40 = snc.sql("SELECT UniqueCarrier, avg(ArrDelay) as avgg, " +
      "sum(ArrDelay) as summ, " +
      "count(*) as countt, absolute_error(summ)  FROM airline group by " +
      " UniqueCarrier order by UniqueCarrier " +
      "with error .3 behavior 'do_nothing' ")
    val rows40 = rs40.collect()
    this.assertAnalysis()
    checkResults(rows4, rows40)

  }

  test("ENT-57. Max Min functions on literal should allow aqp to be used") {
    val result = snc.sql(s"SELECT l_orderkey as orderKey, sum(l_quantity) as x, lower_bound(x) , " +
      "upper_bound(x), absolute_error(x), " +
      s"relative_error(x), max(100), max(true), min(true), min(101) FROM $mainTable  " +
      s"group by l_orderkey with error $DEFAULT_ERROR " +
      s"confidence .95 ")

    this.assertAnalysis()
    var rows2 = result.collect()
    var estimate = rows2(0).getDouble(1)
    msg("estimate=" + estimate)
    msg("lower bound=" + rows2(0).getDouble(2) + ", upper bound=" + rows2(0).getDouble(3) +
      ", absolute error =" + rows2(0).getDouble(4) +
      ", relative error =" + rows2(0).getDouble(5))

    rows2.foreach(row => {
      assertEquals(100, row.getInt(6))
      assertEquals(true, row.getBoolean(7))
      assertEquals(true, row.getBoolean(8))
      assertEquals(101, row.getInt(9))
    })


    val result1 = snc.sql(s"SELECT l_orderkey as orderKey, sum(l_quantity) as x," +
      s" lower_bound(x) , upper_bound(x), absolute_error(x), " +
      s"relative_error(x), max(100 + 123), MAX(CAST(1 AS BOOLEAN)), max(true), min(true), " +
      s"min(101) FROM $mainTable  " +
      s"group by l_orderkey with error $DEFAULT_ERROR " +
      s"confidence .95 ")

    this.assertAnalysis()
    rows2 = result1.collect()
    estimate = rows2(0).getDouble(1)
    msg("estimate=" + estimate)
    msg("lower bound=" + rows2(0).getDouble(2) + ", upper bound=" + rows2(0).getDouble(3) +
      ", absolute error =" + rows2(0).getDouble(4) +
      ", relative error =" + rows2(0).getDouble(5))

    rows2.foreach(row => {
      assertEquals(223, row.getInt(6))
      assertEquals(true, row.getBoolean(7))
      assertEquals(true, row.getBoolean(8))
      assertEquals(true, row.getBoolean(9))
      assertEquals(101, row.getInt(10))
    })
  }

  test("ENT-57. Max Min functions on qcs column should allow aqp to be used") {
    val result = snc.sql(s"SELECT sum(l_quantity) as x, lower_bound(x) , " +
      "upper_bound(x), absolute_error(x), " +
      s"relative_error(x), max(l_quantity), max(true), min(true), min(101) FROM $mainTable  " +
      s" with error $DEFAULT_ERROR " +
      s"confidence .95 ")

    this.assertAnalysis()
    val row = result.collect()(0)
    val estimate = row.getDouble(0)
    msg("estimate=" + estimate)
    msg("lower bound=" + row.getDouble(1) + ", upper bound=" + row.getDouble(2) +
      ", absolute error =" + row.getDouble(3) +
      ", relative error =" + row.getDouble(4))

    val result1 = snc.sql(s"SELECT max(l_quantity)  FROM $mainTable").collect()(0)

    assertEquals(result1.getFloat(0), row.getFloat(5), 0)
    assertEquals(true, row.getBoolean(6))
    assertEquals(true, row.getBoolean(7))
    assertEquals(101, row.getInt(8))


  }


  test("ENT-57. Sample table containing the right qcs should be used if max/min is on a qcs") {
    val rs = snc.sql("SELECT SUM(distance) as sum, lower_bound(sum), " +
      "upper_bound(sum) , relative_error(sum), Max(YearI) FROM airline " +
      " with error .05 confidence .95 ")

    this.assertAnalysis()
    val row = rs.collect()(0)
    val estimate = row.getLong(0)
    msg("estimate=" + estimate)
    msg("lower bound=" + row.getDouble(1) + ", upper bound=" + row.getDouble(2) +
      ", relative error =" + row.getDouble(3))

    val result1 = snc.sql(s"SELECT max(yeari)  FROM airline").collect()(0)

    assertEquals(result1.getInt(0), row.getInt(4))

    val rs1 = snc.sql("SELECT SUM(distance) as sum, lower_bound(sum), " +
      "upper_bound(sum) , relative_error(sum), Max(ActualElapsedTime) FROM airline " +
      " with error .05 confidence .95 ")

    assertTrue(DebugUtils.getAnalysisApplied(rs1.queryExecution.sparkPlan).isEmpty)

    val rs2 = snc.sql("SELECT SUM(distance) as sum, lower_bound(sum), " +
      "upper_bound(sum) , relative_error(sum), Max(round(tan(ActualElapsedTime))) FROM airline " +
      " with error .05 confidence .95 ")

    this.assertAnalysis()

    val row2 = rs2.collect()(0)
    val estimate2 = row2.getLong(0)
    msg("estimate=" + estimate2)
    msg("lower bound=" + row2.getDouble(1) + ", upper bound=" + row2.getDouble(2) +
      ", relative error =" + row2.getDouble(3))

    val result2 = snc.sql(s"SELECT Max(round(tan(ActualElapsedTime)))  FROM airline").collect()(0)
    assertEquals(result2.getDouble(0), row2.getDouble(4), 0)
  }

  test("Hashjoin Bug - 2") {
    snc.setConf(Property.DisableHashJoin.name, "false")

    var rs1 = snc.sql(s"select count(*) couunt, count(*) sample_count " +
      s" from $airlineMainTable t1, $airlineRefTable t2 where  t1.uniquecarrier = t2.code" +
      s" with error")

    AssertAQPAnalysis.bypassErrorCalc(snc)

    snc.setConf(Property.DisableHashJoin.name, "true")

    var rs2 = snc.sql(s"select count(*) couunt, count(*) sample_count " +
      s" from $airlineMainTable t1, $airlineRefTable t2 where  t1.uniquecarrier = t2.code" +
      s" with error")

    AssertAQPAnalysis.bypassErrorCalc(snc)

    var row1 = rs1.collect()(0)
    var row2 = rs2.collect()(0)
    assertEquals(row1.getLong(1), row2.getLong(1))
    assertTrue(Math.abs(row1.getLong(0) - row2.getLong(0)) < 2)
  }

  test("Hashjoin Bug-3") {
    snc.setConf(Property.DisableHashJoin.name, "false")

    var rs1 = snc.sql(s"select count(*) couunt, count(*) sample_count," +
      s" sum(${org.apache.spark.sql.collection.Utils.WEIGHTAGE_COLUMN_NAME}) sample_sum_weight " +
      s" from $airlineSampleTable t1, $airlineRefTable t2 where  t1.uniquecarrier = t2.code" +
      s" with error")

    AssertAQPAnalysis.bypassErrorCalc(snc)

    snc.setConf(Property.DisableHashJoin.name, "true")

    var rs2 = snc.sql(s"select count(*) couunt, count(*) sample_count, " +
      s" sum(${org.apache.spark.sql.collection.Utils.WEIGHTAGE_COLUMN_NAME}) sample_sum_weight " +
      s" from $airlineSampleTable t1, $airlineRefTable t2 where  t1.uniquecarrier = t2.code" +
      s" with error")

    AssertAQPAnalysis.bypassErrorCalc(snc)

    var row1 = rs1.collect()(0)
    var row2 = rs2.collect()(0)
    assertEquals(row1.getLong(1), row2.getLong(1))
    assertEquals(row1.getLong(2), row2.getLong(2))
    assertTrue(Math.abs(row1.getLong(0) - row2.getLong(0)) < 2)
  }


  ignore("Hashjoin: With closedform analysis sample table should be on buildside for queries " +
    "without error functions") {
    snc.setConf(Property.DisableHashJoin.name, "false")

    var rs1 = snc.sql(s"select count(*) couunt, count(*) sample_count," +
      s" sum(${org.apache.spark.sql.collection.Utils.WEIGHTAGE_COLUMN_NAME}) sample_sum_weight " +
      s" from $airlineSampleTable t1, $airlineRefTable t2 where  t1.uniquecarrier = t2.code" +
      s" with error")

    AssertAQPAnalysis.bypassErrorCalc(snc)
    val bhash = rs1.queryExecution.sparkPlan.collect {
      case x: BroadcastHashJoinExec => x
    }.head

    if (bhash.buildSide == BuildLeft) {
      val dscan = bhash.left.collect {
        case x: DataSourceScanExec => x
      }.head
      assertTrue(dscan.relation.isInstanceOf[ColumnFormatSamplingRelation])
    } else {
      assertTrue(bhash.buildSide == BuildRight)
      val dscan = bhash.right.collect {
        case x: DataSourceScanExec => x
      }.head
      assertTrue(dscan.relation.isInstanceOf[ColumnFormatSamplingRelation])
    }
  }

  ignore("Hashjoin: With bootstrap analysis sample table should be on the build side") {
    snc.setConf(Property.DisableHashJoin.name, "false")
    snc.setConf("spark.sql.autoBroadcastJoinThreshold", (8 * 1024).toString)
    var rs1 = snc.sql(s"select count(*) couunt, absolute_error(couunt)," +
      s" count(*) sample_count " +
      s" from $airlineMainTable t1, $airlineRefTable t2 where  t1.uniquecarrier = t2.code" +
      s" with error")

    AssertAQPAnalysis.bootStrapAnalysis(snc)
    val bhash = rs1.queryExecution.sparkPlan.collect {
      case x: BroadcastHashJoinExec => x
    }.head

    if (bhash.buildSide == BuildLeft) {
      val dscan = bhash.left.collect {
        case x: DataSourceScanExec => x
      }.head
      assertTrue(dscan.relation.isInstanceOf[ColumnFormatSamplingRelation])
    } else {
      assertTrue(bhash.buildSide == BuildRight)
      val dscan = bhash.right.collect {
        case x: DataSourceScanExec => x
      }.head
      assertTrue(dscan.relation.isInstanceOf[ColumnFormatSamplingRelation])
    }

  }

  test("support issue  SDENT-63") {
    val rs = snc.sql("SELECT uniquecarrier z," +
      " (SUM(depdelay)/ COUNT(depdelay)) as x, MAX(FALSE) y" +
      " FROM airline GROUP BY uniquecarrier with error .95 behavior 'LOCAL_OMIT'")
    rs.collect()
    assertAnalysis()
  }

  test("ENT-57. aqp query only containing max function should use sample table if possible") {
    val rs = snc.sql("SELECT  Max(YearI), Min(yearI) FROM airline  with error ")
    assertTrue(DebugUtils.getAnalysisApplied(rs.queryExecution.sparkPlan).
      map(_ == AnalysisType.ByPassErrorCalc).getOrElse(false))
    var row = rs.collect()(0)
    val result1 = snc.sql(s"SELECT max(yeari), min(yeari)  FROM airline").collect()(0)
    assertEquals(result1.getInt(0), row.getInt(0))
    assertEquals(result1.getInt(1), row.getInt(1))

    row = snc.sql(s"SELECT max(yeari) as maxx, min(yeari) as minn, " +
      s"lower_bound(maxx), upper_bound(maxx), absolute_error(maxx)," +
      s"relative_error(maxx), absolute_error(minn)  FROM airline with error").collect()(0)
    assertTrue(DebugUtils.getAnalysisApplied(rs.queryExecution.sparkPlan).
      map(_ == AnalysisType.ByPassErrorCalc).getOrElse(false))
    assertEquals(result1.getInt(0), row.getInt(0))
    assertEquals(result1.getInt(1), row.getInt(1))
    assertTrue(row.isNullAt(2))
    assertTrue(row.isNullAt(3))
    assertEquals(0, row.getDouble(4), 0)
    assertEquals(0, row.getDouble(5), 0)
    assertEquals(0, row.getDouble(6), 0)


    row = snc.sql(s"SELECT  max(yeari) as maxx, min(yeari) as minn, " +
      s"lower_bound(maxx), upper_bound(maxx), absolute_error(maxx)," +
      s"relative_error(maxx), absolute_error(minn), sum(yeari) as summ, " +
      s" lower_bound(summ) as lower, upper_bound(summ) as upper  FROM " +
      s" airline with error").collect()(0)
    assertAnalysis()
    assertEquals(result1.getInt(0), row.getInt(0))
    assertEquals(result1.getInt(1), row.getInt(1))
    assertTrue(row.isNullAt(2))
    assertTrue(row.isNullAt(3))
    assertEquals(0, row.getDouble(4), 0)
    assertEquals(0, row.getDouble(5), 0)
    assertEquals(0, row.getDouble(6), 0)
    assertFalse(row.isNullAt(7))
    assertTrue(row.getDouble(8) != 0)
    assertTrue(row.getDouble(9) != 0)
  }

  def msg(m: String): Unit = DebugUtils.msg(m)

  def createLineitemTable(sqlContext: SQLContext,
    tableName: String, isSample: Boolean = false): DataFrame = {


    val schema = StructType(Seq(
      StructField("l_orderkey", IntegerType, nullable = false),
      StructField("l_partkey", IntegerType, nullable = false),
      StructField("l_suppkey", IntegerType, nullable = false),
      StructField("l_linenumber", IntegerType, nullable = false),
      StructField("l_quantity", FloatType, nullable = false),
      StructField("l_extendedprice", FloatType, nullable = false),
      StructField("l_discount", FloatType, nullable = false),
      StructField("l_tax", FloatType, nullable = false),
      StructField("l_returnflag", StringType, nullable = false),
      StructField("l_linestatus", StringType, nullable = false),
      StructField("l_shipdate", DateType, nullable = false),
      StructField("l_commitdate", DateType, nullable = false),
      StructField("l_receiptdate", DateType, nullable = false),
      StructField("l_shipinstruct", StringType, nullable = false),
      StructField("l_shipmode", StringType, nullable = false),
      StructField("l_comment", StringType, nullable = false),
      StructField("scale", IntegerType, nullable = false)
    ))
    sqlContext.sql("DROP TABLE IF EXISTS " + tableName + "_sampled")
    sqlContext.sql("DROP TABLE IF EXISTS " + tableName)


    val people = sqlContext.sparkContext.textFile(LINEITEM_DATA_FILE)
      .map(_.split('|')).map(p => Row(p(0).trim.toInt, p(1).trim.toInt,
      p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toFloat, p(5).trim.toFloat,
      p(6).trim.toFloat, p(7).trim.toFloat,
      p(8).trim, p(9).trim, java.sql.Date.valueOf(p(10).trim),
      java.sql.Date.valueOf(p(11).trim), java.sql.Date.valueOf(p(12).trim),
      p(13).trim, p(14).trim, p(15).trim, p(16).trim.toInt))

    logInfo("total rows in table =" + people.collect().length)
    val df = if (isSample) {
      sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row],
        schema)

    } else {
      val dfx = sqlContext.createDataFrame(people, schema)
      dfx.write.format("column").mode(SaveMode.Append).saveAsTable(tableName)
      dfx
    }
    df
  }

  def assertAnalysis()

}

/**
 * Debuggin Utilities.
 *
 * To get the di"..." string interpolator to work, you'll need to add this
 * import:
 * import io.snappydata.util.DebugUtils._
 */
object DebugUtils extends Logging {
  val format = new SimpleDateFormat("mm:ss:SSS")

  // Apparently, its pretty hard to get the PID of the current process in Java?!
  // Anyway, here is one method that depends on /proc, but I think we are all
  // running on platforms that have /proc.  If not, we'll have to redo this on to
  // use the Java ManagementFactory.getRuntimemMXBean() method?  See
  // http://stackoverflow.com/questions/35842/how-can-a-java-program-get-its-own-process-id
  //
  // This should probably be in a OS-specific class?
  // lazy val myPid: Int = Integer.parseInt(new File("/proc/self")
  //   .getCanonicalFile().getName())

  lazy val myInfo: String = ManagementFactory.getRuntimeMXBean.getName

  /**
   * Print a message on stdout but prefix with thread info and timestamp info
   */
  def msg(m: String): Unit = {
    logInfo(di"$m")
  }

  /**
   * Get the PID for this JVM
   */
  def getPidInfo: String = myInfo

  implicit class DebugInterpolator(val sc: StringContext) extends AnyVal {
    def di(args: Any*): String = {
      val ts = new Date(System.currentTimeMillis())
      s"==== [($myInfo) ${Thread.currentThread().getName}: " +
        s"(${format.format(ts)})]:  ${sc.s(args: _*)}"
    }
  }

  def getAnalysisApplied(plan: SparkPlan): Option[AnalysisType.Type] = {
    val aqpInfo = if (AssertAQPAnalysis.aqpInfoArray.length < 1) None
    else Some(AssertAQPAnalysis.aqpInfoArray(0))

    plan.find {
      case _: SnappyHashAggregate => true
      case _: SnappySortAggregate => true
      case _ => false
    } match {
      case None => aqpInfo.map(_.analysisType).getOrElse(None)
      case Some(x) =>
        if (filter(x.expressions, { case y: ClosedFormErrorEstimate => y }
        ).nonEmpty) {
          assertTrue(aqpInfo.map(_.analysisType.map(_ == AnalysisType.Closedform).
            getOrElse(false)).getOrElse(true))
          Some(AnalysisType.Closedform)
        } else if (filter(x.expressions, {
          case y: DeclarativeBootstrapAggregateFunction => y
        }).nonEmpty) {
          assertTrue(aqpInfo.map(_.analysisType.map(_ == AnalysisType.Bootstrap).
            getOrElse(false)).getOrElse(true))
          Some(AnalysisType.Bootstrap)
        } else {
          None
        }
    }
  }

  def getAnyErrorAggregateFunction(plan: SparkPlan): Option[ErrorAggregateFunction] = {
    plan.find {
      case _: SnappyHashAggregate => true
      case _: SnappySortAggregate => true
      case _ => false
    } match {
      case None => None
      case Some(x) =>
        val exps = x.expressions.flatMap {
          _.collect {
            case x: ErrorAggregateFunction => x
          }
        }
        exps.headOption
    }
  }

  def filter(expressions: Seq[Expression],
    criteria: PartialFunction[Expression, Expression]): Seq[Expression] = {
    expressions.flatMap(_.collect(criteria))
  }
}

