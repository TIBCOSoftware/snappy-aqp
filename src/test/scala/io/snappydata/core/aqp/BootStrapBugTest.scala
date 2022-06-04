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

import io.snappydata.Property
import org.junit.Assert._
import org.apache.spark.sql.execution.common.{AQPInfo, AQPRules, AnalysisType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{AssertAQPAnalysis, SparkConf}

class BootStrapBugTest extends CommonBugTest {

  val suppressTestAsserts: Set[Int] = Set[Int]()
  val lineTable: String = getClass.getSimpleName + "_" + "line"
  val mainTable: String = getClass.getSimpleName + "_" + "mainTable"
  val sampleTable: String = getClass.getSimpleName + "_" + "mainTable_sampled"

  override val bugAQP214Asserter : ((Row, Row)) => Unit = {
    case (row1, row2) =>
      val totalCols = row1.length
      assert(Math.abs(row1.getLong(totalCols - 2) - row2.getLong(totalCols -2)) < 2)
  }

  // Set up sample & Main table
  private val LINEITEM_DATA_FILE = getClass.getResource("/datafile.tbl").getPath

  override def beforeAll(): Unit = {
    super.beforeAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    initTestTables()

  }


  override def afterAll(): Unit = {
    val snc = this.snc
    snc.sql("drop table if exists " + lineTable)
    snc.sql("drop table if exists " + sampleTable)
    snc.sql("drop table if exists " + mainTable)
    // cleanup metastore
    super.afterAll()
  }

 /*
 protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    val conf = new org.apache.spark.SparkConf()
        .setAppName("BootStrapBugTest")
        .setMaster("local[6]")
        .set("spark.sql.hive.metastore.sharedPrefixes",
          "com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc," +
              "com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity," +
              "com.mapr.fs.jni,org.apache.commons")
        .set("master.ui.port", "" + AvailablePort.getRandomAvailablePort(0))

    addSpecificProps(conf)
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }
  */

  protected def addSpecificProps(conf: SparkConf): Unit = {
    conf.setAppName("BootStrapAggregateFunctionTest")
      .set(Property.ClosedFormEstimates.name, "false").set(Property.NumBootStrapTrials.name, "100")
        .set(Property.AqpDebugFixedSeed.name, "true")
        .set("spark.master", "local[6]")
  }


  def assertAnalysis(position: Int = 0): Unit = {
    AssertAQPAnalysis.bootStrapAnalysis(this.snc, position)
  }


  test("Sample Table Query on Count aggregate with error estimates") {
    this.testSampleTableQueryCountAggWithErrorEstimates()
  }

  def testSampleTableQueryCountAggWithErrorEstimates(): Unit = {
    val result = snc.sql(s"SELECT count(l_quantity) as x, lower_bound(x) , " +
        s"upper_bound(x), absolute_error(x), " +
        s"relative_error(x) FROM $mainTable with error $DEFAULT_ERROR confidence .95 ")

    this.assertAnalysis()
    val rows2 = result.collect()
    val estimate = rows2(0).getLong(0)
    msg("estimate=" + estimate)
    assert(Math.abs(estimate - 13) < .8)
    msg("lower bound=" + rows2(0).getDouble(1) + ", upper bound" + rows2(0).getDouble(2)
        + ", absolute error =" + rows2(0).getDouble(3) +
        ", relative error =" + rows2(0).getDouble(4))
  }


  def initTestTables(): Unit = {

    createLineitemTable(snc, lineTable)
    val mainTableDF = createLineitemTable(snc, mainTable)
    snc.createSampleTable(sampleTable, Some(mainTable),
      Map(
        "qcs" -> "l_quantity",
        "fraction" -> "0.01",
        "strataReservoirSize" -> "50"), allowExisting = false)

  }


  test("Test sample table query on  aggregates with having clause containing aggregate function") {
    this.testSampleQueryWithHavingClauseAsAggregateFunc()
  }

  def testSampleQueryWithHavingClauseAsAggregateFunc(): Unit = {
    val resultX = snc.sql(s"SELECT avg(l_quantity) as x, l_orderkey, " +
        s" absolute_error(x) FROM $mainTable  group by l_orderkey " +
        s" having avg(l_quantity) > 25 " +
        s" with error $DEFAULT_ERROR confidence .95  ")
    /*
    val resultX = snc.sql(s"SELECT avg(l_quantity) as x, l_orderkey
      FROM $mainTable  group by l_orderkey having avg(l_quantity) > 25")
    */
    resultX.collect()
    this.assertAnalysis()
  }


  test("Test sample table query on mixed aggregates with group by") {
    this.testMixedAggregatesWithGroupBy()
  }


  def testMixedAggregatesWithGroupBy(): Unit = {
    val result = snc.sql(s"SELECT sum(l_quantity) as x , avg(l_quantity) as y, " +
        s"count(l_quantity) as z  , l_orderkey , absolute_error(x) FROM $mainTable " +
        s"group by l_orderkey with error $DEFAULT_ERROR confidence 0.95")

    this.assertAnalysis()
    val rows = result.collect()
    assert(rows.length === 3)
    rows(0).getDouble(1)

    val result1 = snc.sql(s"SELECT sum(l_quantity) as x , l_orderkey, " +
        s" absolute_error(x) FROM $mainTable group by l_orderkey " +
      s" with error $DEFAULT_ERROR confidence 0.95")
    val rows1 = result1.collect()
    this.assertAnalysis()
    val result2 = snc.sql(s"SELECT avg(l_quantity) as x , l_orderkey, " +
        s"  absolute_error(x) FROM $mainTable group by l_orderkey " +
      s"with error $DEFAULT_ERROR confidence 0.95")
    val rows2 = result2.collect()
    this.assertAnalysis()
    val result3 = snc.sql(s"SELECT count(l_quantity) as x , l_orderkey, " +
        s" absolute_error(x) FROM $mainTable group by l_orderkey " +
      s" with error $DEFAULT_ERROR confidence 0.95")
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

  test("Bug AQP211: A table with weight column should be treated as a sample table") {
    // val testNo = 31
    val nameTestTable = "TestSample"
    val scaledUpCount = snc.sql(s"select count(*) from $sampleAirlineUniqueCarrier").collect()(0)
      .getLong(0)
    val  sampleResult = snc.sql(s"select * from $sampleAirlineUniqueCarrier")
    snc.createTable(nameTestTable, "column", sampleResult.schema,
      Map.empty[String, String])
    sampleResult.write.mode(SaveMode.Append).saveAsTable(nameTestTable)
    var testTableCount = snc.sql(s"select count(*) from $nameTestTable ").collect()(0)
      .getLong(0)
    assert(Math.abs(scaledUpCount - testTableCount) < 2)

    val result1 = snc.sql(s"select count(*) as x, absolute_error(x) from $nameTestTable ")
      .collect()
    testTableCount = result1(0).getLong(0)
    var absError = result1(0).getDouble(1)
    assert(Math.abs(scaledUpCount - testTableCount) < 2)
    assert(absError > 0)

    val scaledUpSum = snc.sql(s"select Sum(ArrDelay) from $sampleAirlineUniqueCarrier").collect()(0)
      .getLong(0)
    val result2 = snc.sql(s"select sum(ArrDelay) as x, absolute_error(x) from $nameTestTable ")
      .collect()
    val testTableSum = result2(0).getLong(0)
    absError = result2(0).getDouble(1)
    assert(Math.abs(scaledUpSum - testTableSum) < 2)
    assert(absError > 0)


    val scaledUpAvg = snc.sql(s"select Avg(ArrDelay) from $sampleAirlineUniqueCarrier").collect()(0)
      .getDouble(0)
    val result3 = snc.sql(s"select Avg(ArrDelay) as x, absolute_error(x) from $nameTestTable ")
      .collect()
    val testTableAvg = result3(0).getDouble(0)
    absError = result3(0).getDouble(1)
    assert(Math.abs(scaledUpAvg - testTableAvg) < 2)
    assert(absError > 0)

  }


  test("Sample Table Query on avg aggregate on integer column with error estimates ") {
    this.testSampleQueryOnAvgWithIntegerColWithErrorEstimates()
  }

  def testSampleQueryOnAvgWithIntegerColWithErrorEstimates(): Unit = {
    val result = snc.sql(s"SELECT avg(l_linenumber) as x, lower_bound(x) , " +
        s"upper_bound(x), absolute_error(x), " +
        s"relative_error(x) FROM $mainTable  with error $DEFAULT_ERROR confidence .95 ")

    result.collect()
    this.assertAnalysis()
  }


  test("Sample Table Query alias on Sum aggregate  should be correct") {
    this.testSampleQueryWithAliasOnSum()
  }

  def testSampleQueryWithAliasOnSum(): Unit = {
    val result = snc.sql(s"SELECT sum(l_quantity) as T,  absolute_error(T) FROM $mainTable" +
      s" with error $DEFAULT_ERROR " +
        s"confidence 0.95")

    this.assertAnalysis()
    val rows2 = result.collect()
    // val estimate = rows2(0).getDouble(0)
    assert(rows2(0).schema.head.name === "t")
  }

  // This test takes a lot of time on newyork taxi data & takes lot of time. However it does test
  // important bugs, so periodiccally it needs to be enabled & run.
  // Currently this test is being run on a very small subset of total data.
  // periodically run it on full data
  // Because of the size, the plan generated is a little special case
  // which showed up multiple issues,
  // namely related to code of identifying query rerouting & shuffling of output attributes.
  test("Bug IndexOutofBoundsException") {
    this.testBugIndexOutOfBoundsException()
  }

  def testBugIndexOutOfBoundsException(): Unit = {
    // val testNo = 15

    val hfile: String = getClass.getResource("/2015.parquet").getPath
    // val hfile: String = getClass.getResource("/1995-2015_ParquetData").getPath
    // val hfile: String = getClass.getResource("/sample.csv").getPath
    val snContext = this.snc
    snc.dropTable("airline1", ifExists = true)
    snc.dropTable("airlineSampled", ifExists = true)
    snContext.sql("set spark.sql.shuffle.partitions=6")
    logInfo("about to load parquet")

    val airlineDataFrame = snContext.read.parquet(hfile).toDF("YEARI", "MonthI",
      "DayOfMonth", "DayOfWeek", "UniqueCarrier", "TailNum", "FlightNum",
      "Origin", "Dest", "CRSDepTime", "DepTime", "DepDelay", "TaxiOut",
      "TaxiIn", "CRSArrTime", "ArrTime", "ArrDelay", "Cancelled",
      "CancellationCode", "Diverted", "CRSElapsedTime", "ActualElapsedTime",
      "AirTime", "Distance", "CarrierDelay", "WeatherDelay",
      "NASDelay", "SecurityDelay", "LateAircraftDelay", "ArrDelaySlot")

    logInfo("airline data frame created")

    snContext.createTable("airline1", "column", airlineDataFrame.schema, Map("buckets" -> "8"))

    airlineDataFrame.write.insertInto("airline1")

    /**
     * Total number of executors (or partitions) = Total number of buckets +
     * Total number of reservoir
     */
    /**
     * Partition by influences whether a strata is handled in totality on a
     * single executor or not, and thus an important factor.
     */
    snc.sql(s"CREATE TABLE airlineSampled " +
        "USING column_sample " +
        "options " +
        "(" +
        s"qcs 'UniqueCarrier'," +
        "BUCKETS '8'," +
        s"fraction '.0001'," +
        s"strataReservoirSize '50', " +
        "baseTable 'airline1')")

    val bootstrapSum = snc.sql(s"select avg(ArrDelay) avg_arrival_delay, " +
        s"absolute_error(avg_arrival_delay) abs_err, YearI from airline1 " +
        s" where ArrDelay > 0  group by YEARI order by YEARI " +
        s"with error 0.1 behavior 'do_nothing'")

    val rs = bootstrapSum.collect()
    rs.foreach(row => assert(row.getDouble(1) < 1994))

    AssertAQPAnalysis.bootStrapAnalysis(snc)

    snc.sql("drop table if exists airlineSampled")
    snc.sql("drop table if exists airline1 ")
  }

  test("SNAP-3218 sample table population issue with explicit schema") {
    ddlStr = "(YearI INT," + // NOT NULL
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

    val hfile: String = getClass.getResource("/2015.parquet").getPath
    // val hfile: String = getClass.getResource("/1995-2015_ParquetData").getPath
    // val hfile: String = getClass.getResource("/sample.csv").getPath
    val snContext = this.snc
    snc.dropTable("airline1", ifExists = true)
    snc.dropTable("airlineSampled", ifExists = true)
    snContext.sql("set spark.sql.shuffle.partitions=6")

    val airlineDataFrame = snContext.read.parquet(hfile)

    snContext.createTable("airline1", "column", airlineDataFrame.schema, Map("buckets" -> "8"))

    airlineDataFrame.write.insertInto("airline1")

    /**
     * Total number of executors (or partitions) = Total number of buckets +
     * Total number of reservoir
     */
    /**
     * Partition by influences whether a strata is handled in totality on a
     * single executor or not, and thus an important factor.
     */
    snc.sql(s"CREATE TABLE airlineSampled $ddlStr " +
      "USING column_sample " +
      "options " +
      "(" +
      s"qcs 'UniqueCarrier'," +
      "BUCKETS '8'," +
      s"fraction '.0001'," +
      s"strataReservoirSize '50', " +
      "baseTable 'airline1')")

    val bootstrapSum = snc.sql(s"select avg(ArrDelay) avg_arrival_delay, " +
      s"absolute_error(avg_arrival_delay) abs_err, YearI from airline1 " +
      s" where ArrDelay > 0  group by YEARI order by YEARI " +
      s"with error 0.1 behavior 'do_nothing'")

    val rs = bootstrapSum.collect()
    snc.dropTable("airlineSampled", true)
    snc.dropTable("airline1", true)
  }

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

    sqlContext.sql("DROP TABLE IF EXISTS " + tableName)
    sqlContext.sql("DROP TABLE IF EXISTS " + tableName + "_sampled")

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
      dfx.createOrReplaceTempView(tableName)
      dfx
    }
    df
  }

  def assertAqpInfo(buffer: scala.collection.mutable.ArrayBuffer[Array[AQPInfo]]): Unit = {
    assertTrue(buffer.exists(arr => arr.length > 0 && (arr(0).analysisType match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })))
  }
}
