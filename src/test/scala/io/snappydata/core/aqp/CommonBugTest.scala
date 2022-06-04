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
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.{Constant, Property, SnappyFunSuite}
import org.scalatest.tagobjects.Retryable
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow}
import org.apache.spark.sql.execution.SnappyContextAQPFunctions
import org.apache.spark.sql.execution.aggregate.{SnappyHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.execution.common.{AQPInfo, AQPInfoStoreTestHook, AQPRules, AnalysisType, MapColumnToWeight}
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.{AssertAQPAnalysis, SparkConf}

abstract class CommonBugTest extends SnappyFunSuite with Matchers
    with BeforeAndAfterAll with BeforeAndAfter {

  protected val createTableExtension: String = "  using column "
  val DEFAULT_ERROR = Constant.DEFAULT_ERROR
  val suppressTestAsserts: Set[Int]
  var orderLineDF: DataFrame = _
  val orderlineTable = "order_line_col"

  var orderLineNullDF: DataFrame = _
  val orderlineNullTable = "order_line_col_null"
  val airlineTable = "airline"
  var airlineDF: DataFrame = _
  var ddlStr: String = _
  val nyc = "nyc"
  var nycDF: DataFrame = _
  var tripFareDF: DataFrame = _
  val taxiFare = "TAXIFARE"
  val dailyTaxiFare = "DAILYTAXIFARE"
  val sampleOrderLineOlNumber = "sampled_order_line_ol_number"
  val sampleOrderLineNullOlNumber = "sampled_order_line_null_ol_number"
  val sampleAirlineUniqueCarrier = "sampled_airline_unique_carrier"

  def asserter(assertFunc: => Unit, testNumber: Int): Unit = {
    if (!suppressTestAsserts.contains(testNumber)) {
      assertFunc
    } else {
      // Do Nothing
      msg("************************TEST NO = " + testNumber + " IS DISABLED ." +
          " DON'T BE FOOLED BY GREEN TEST COLOUR****")
    }
  }

  // Set up sample & Main table

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    this.initTables()
    System.setProperty(Constant.defaultBehaviorAsDO_NOTHING, "true")
  }

  after {
    snc.sql("drop table if exists nycSample")
  }

  def initTables(): Unit = {
    snc.sql(s"drop table if exists $orderlineTable ")
    snc.sql(s"create table $orderlineTable(" +
      "ol_w_id         integer," +
      "ol_d_id         integer," +
      "ol_o_id         integer," +
      "ol_number       integer," +
      "ol_i_id         integer," +
      "ol_amount       decimal(6,2)," +
      "ol_supply_w_id  integer," +
      "ol_quantity     decimal(2,0)," +
      "ol_dist_info    varchar(24))")

    val orderLineCSV = getClass.getResource("/ORDER_LINE.csv").getPath
    orderLineDF = snc.read.format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("maxCharsPerColumn", "4096")
      .load(orderLineCSV)

    orderLineDF.write.insertInto(orderlineTable)
    orderLineDF.cache()

    snc.sql(s"CREATE SAMPLE TABLE $sampleOrderLineOlNumber" +
      s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
      s" baseTable '$orderlineTable')")

    snc.sql(s"drop table if exists $orderlineNullTable ")
    snc.sql(s"create table  $orderlineNullTable(" +
      "ol_w_id         integer," +
      "ol_d_id         integer," +
      "ol_o_id         integer," +
      "ol_number       integer," +
      "ol_i_id         integer," +
      "ol_amount       decimal(6,2)," +
      "ol_supply_w_id  integer," +
      "ol_quantity     decimal(2,0)," +
      "ol_quantity_mix decimal(2,0)," +
      "ol_dist_info    varchar(24))")

    val orderLineNullCSV = getClass.getResource("/ORDER_LINE_NULL_VALS.csv").getPath
    orderLineNullDF = snc.read.format("com.databricks.spark.csv")
      .option("inferSchema", "true").option("maxCharsPerColumn", "4096")
      .load(orderLineNullCSV)

    orderLineNullDF.write.insertInto(orderlineNullTable)
    orderLineNullDF.cache()

    snc.sql(s"CREATE SAMPLE TABLE $sampleOrderLineNullOlNumber" +
      s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
      s" baseTable '$orderlineNullTable')")



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
    val snContext = snc
    snContext.sql("set spark.sql.shuffle.partitions=6")

    airlineDF = snContext.read.load(hfile)

    airlineDF.registerTempTable(airlineTable)
    // airlineDF.cache()
    val qcsColumns: String = "UniqueCarrier"
    val strataReserviorSize = 50

    val sampleRatio: Double = 0.001
    snc.sql(s"CREATE TABLE $sampleAirlineUniqueCarrier $ddlStr" +
      "USING column_sample " +
      "options " +
      "(" +
      s"qcs '$qcsColumns'," +
      "BUCKETS '8'," +
      s"fraction '$sampleRatio'," +
      s"strataReservoirSize '$strataReserviorSize', " +
      s"baseTable '$airlineTable')")



    val hfileNYC: String = getClass.getResource("/NYC_trip_ParquetEmptyData").getPath
    // TO run on full data use the below path
    // val hfile: String = getClass.getResource("/NYC_trip_ParquetData").getPath

    nycDF = snc.read.parquet(hfileNYC)
    nycDF.cache()
    nycDF.registerTempTable(nyc)


    val hfileTaxi: String = getClass.getResource("/trip_fare_ParquetEmptyData").getPath
    // TODO: Use following with full data
    // val hfile: String = "../../data/NYC_trip_ParquetData"
    // val hfile1: String = "../../data/trip_fare_ParquetData"


    tripFareDF = snc.read.parquet(hfileTaxi)

    tripFareDF.registerTempTable(taxiFare)
    tripFareDF.cache()
    snc.sql(s"CREATE TABLE $dailyTaxiFare(hack_license String, " +
      s" pickup_date Date, daily_fare_amount Long, " +
      s" daily_tip_amount Long) USING COLUMN")
    // -- OPTIONS (buckets '8', PARTITION_BY 'hack_license')
    snc.sql(s"INSERT INTO TABLE $dailyTaxiFare SELECT hack_license, " +
      s" to_date(pickup_datetime) as pickup_date, " +
      s" sum(fare_amount) as daily_fare_amount, " +
      s" sum(tip_amount) as daily_tip_amount " +
      s" FROM $taxiFare group by hack_license, " +
      s" pickup_datetime")
  }

  def beforeTest(): Unit = {
    System.setProperty(Constant.defaultBehaviorAsDO_NOTHING, "true")
  }

  def afterTest(): Unit = {
    System.setProperty(Constant.defaultBehaviorAsDO_NOTHING, "false")
  }

  override def afterAll(): Unit = {
    val snc = this.snc
    snc.sql(s"drop table if exists $sampleAirlineUniqueCarrier ")
    snc.sql(s"drop table if exists $sampleOrderLineOlNumber ")
    snc.sql(s"drop table if exists $sampleOrderLineNullOlNumber ")
    snc.sql(s"drop table if exists $orderlineNullTable ")
    snc.sql(s"drop table if exists $orderlineTable ")
    snc.sql(s"drop table if exists $airlineTable ")
    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
    snc.sql(s"drop table if exists $nyc ")
    snc.sql(s"drop table if exists $taxiFare ")
    snc.sql("drop table if exists DAILYTAXIFARE_SAMPLEHACKLICENSE")
    snc.sql(s"drop table if exists $dailyTaxiFare ")
    // cleanup metastore
    if (snc != null) {
      snc.clearCache()
    }
    super.afterAll()
    stopAll()
    System.setProperty(Constant.defaultBehaviorAsDO_NOTHING, "false")
  }

  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    val random = new Random(731)
    var maxCodeGen: Int = 0
    while(maxCodeGen == 0) {
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
        .setAppName("CommonBugTest")
        .setMaster("local[6]")
        .set("spark.sql.hive.metastore.sharedPrefixes",
          "com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc," +
              "com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity," +
              "com.mapr.fs.jni,org.apache.commons")
        .set("master.ui.port", "" + AvailablePort.getRandomAvailablePort(0))
        .set("spark.sql.shuffle.partitions", "6")
        .set("spark.sql.codegen.maxFields", maxCodeGen.toString)
        .set(Property.TestDisableCodeGenFlag.name , "true")
        .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")

    addSpecificProps(conf)
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  protected def addSpecificProps(conf: SparkConf): Unit

  def assertAnalysis(position: Int = 0): Unit


  test("SNAP-806 test1 decimal column not handled correctly") {
    testSnap_806_1()
  }

  def testSnap_806_1(): Unit = {
    val testNo = 1
    val result1 = snc.sql(
      s"""SELECT *  FROM
       $orderlineTable """)
    result1.collect()

    val result = snc.sql(s"SELECT ol_number, sum(ol_quantity) AS sum_qty, sum(ol_amount) AS" +
        s" sum_amount, avg(ol_quantity) AS avg_qty, avg(ol_amount) AS avg_amount,  count(*) " +
        s"AS count_order, absolute_error(sum_qty) FROM $orderlineTable GROUP BY ol_number " +
      s" ORDER BY ol_number with error $DEFAULT_ERROR confidence 0.95")
    val rows = result.collect()
    rows.foreach(row => for (i <- 0 until row.length) {
      asserter(assert(!row.isNullAt(i)), testNo)
    })

    this.assertAnalysis()
  }

  test("SNAP-806 test2 decimal column not handled correctly") {
    this.testSnap_806_2()
  }

  def testSnap_806_2(): Unit = {
    val testNo = 2


    val result1 = snc.sql( s"""SELECT *  FROM $orderlineNullTable """)
    result1.collect()

    snc.sql(
      s"""SELECT ol_number as num_base_table,
        | sum(ol_quantity) AS sum_qty_null,
        | sum(ol_amount) AS sum_amount,
        | avg(ol_quantity) AS avg_qty_null,
        | avg(ol_amount) AS avg_amount,
        | count(*) AS count_star,
        | count(ol_quantity) AS cnt_qty_null,
        | count(ol_quantity_mix) AS cnt_qty_mix,
        | sum(ol_quantity_mix) AS sum_qty_mix,
        | avg(ol_quantity_mix) AS avg_qty_mix
        | FROM $orderlineNullTable
        | GROUP BY ol_number
        | ORDER BY ol_number""".stripMargin).collect()

    val result = snc.sql(
      s"""SELECT ol_number as num_sample_table,
        | sum(ol_quantity) AS sum_qty_null,
        | sum(ol_amount) AS sum_amount,
        | avg(ol_quantity) AS avg_qty_null,
        | avg(ol_amount) AS avg_amount,
        | count(*) AS count_star,
        | count(ol_quantity) AS cnt_qty_null,
        | count(ol_quantity_mix) AS cnt_qty_mix,
        | sum(ol_quantity_mix) AS sum_qty_mix,
        | avg(ol_quantity_mix) AS avg_qty_mix,
        | absolute_error(cnt_qty_mix)
        | FROM $orderlineNullTable
        | GROUP BY ol_number
        | ORDER BY ol_number
        | with error 0.2
        | confidence 0.95""".stripMargin)
    val rows = result.collect()
    var loop = 0
    rows.foreach(row => {
      asserter(assert(row.isNullAt(1)), testNo)
      asserter(assert(row.isNullAt(3)), testNo)
      asserter(assert(!row.isNullAt(0)), testNo)
      asserter(assert(!row.isNullAt(2)), testNo)
      asserter(assert(!row.isNullAt(4)), testNo)
      asserter(assert(!row.isNullAt(5)), testNo)
      asserter(assert(!row.isNullAt(6)), testNo)
      asserter(assert(!row.isNullAt(7)), testNo)
      if (loop == 0) {
        asserter(assert(row.isNullAt(8)), testNo)
        asserter(assert(row.isNullAt(9)), testNo)
      } else {
        asserter(assert(!row.isNullAt(8)), testNo)
        asserter(assert(!row.isNullAt(9)), testNo)
      }
      loop = loop + 1
    })

    this.assertAnalysis()
    snc.sql("drop table if exists sampled_order_line2_col")

  }

  test("SNAP-822 test1 select query on sample table throws exception") {
    testSnap822_1()
  }

  def testSnap822_1(): Unit = {
    val testNo = 3

    val result1 = snc.sql(
      s"""SELECT *  FROM
       $sampleOrderLineOlNumber """)
    val schema = result1.schema
    asserter(assert(schema.length === 10), testNo)
    result1.collect()
  }

  test("SNAP-822 test2 select query on sample table throws exception") {
    testSnap822_2()
  }


  def testSnap822_2(): Unit = {
    val testNo = 4
    val result1 = snc.sql(
      s"""SELECT *  FROM
       $sampleOrderLineNullOlNumber """)
    result1.collect()
    val schema = result1.schema
    asserter(assert(schema.length === 11), testNo)
  }

  test(" select query with aggregate on sample table  with where clause & error clause") {
    testQueryWithErrorAndWhereClauseOnSampleTable()
  }

  def testQueryWithErrorAndWhereClauseOnSampleTable(): Unit = {
    val testNo = 5
    snc.sql("drop table if exists sampled_order_line4_col")
    snc.sql("drop table if exists order_line4_col")
    snc.sql("create table order_line4_col(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       decimal(6,2)," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_dist_info    varchar(24))")

    snc.sql("CREATE SAMPLE TABLE sampled_order_line4_col" +
        " OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        " baseTable 'order_line4_col')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE.csv").getPath
    var orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto("order_line4_col")


    val result1 = snc.sql(
      """SELECT count(*) as x, absolute_error(x) FROM
       sampled_order_line4_col where ol_o_id > 0 with error 0.5 """)
    result1.collect()

    this.assertAnalysis()
    snc.sql("drop table if exists sampled_order_line4_col")
    snc.sql("drop table if exists order_line4_col")
  }

  test("count aggregate query with column name") {
    testCountQueryOnColumn()
  }

  def testCountQueryOnColumn(): Unit = {
    val testNo = 6


    val result1 = snc.sql(
      s"""SELECT count(ol_o_id) as x, absolute_error(x)  FROM
       $sampleOrderLineOlNumber where ol_o_id > 0 with error 0.5 """)
    result1.collect()

    this.assertAnalysis()


  }

  test("null columns for aggregates : test 1") {
    testAggregatesOnNullColumns_1()
  }


  def testAggregatesOnNullColumns_1(): Unit = {
    val testNo = 7
    snc.sql("drop table if exists sampled_order_line6_col")

    snc.sql("create table order_line6_col(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       decimal(6,2)," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))")

    snc.sql("CREATE SAMPLE TABLE sampled_order_line6_col" +
        " OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        " baseTable 'order_line6_col')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_NULL_VALS.csv").getPath
    var orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)
    orderLineDF.write.insertInto("order_line6_col")

    val resultBaseTable = snc.sql("select count(*) as count_star, avg(ol_quantity) as average, " +
        " sum(ol_quantity) as sum, count(ol_quantity) as count_col from order_line6_col ")
    val rowsBaseTable = resultBaseTable.collect()
    asserter(assert(rowsBaseTable(0).getLong(0) > 0), testNo)
    asserter(assert(rowsBaseTable(0).isNullAt(1)), testNo)
    asserter(assert(rowsBaseTable(0).isNullAt(2)), testNo)
    asserter(assert(rowsBaseTable(0).getLong(3) === 0), testNo)
    logInfo("base table results=")
    logInfo(rowsBaseTable.mkString("\n"))

    val resultSampleTableTable = snc.sql("select count(*) as count_star," +
        " avg(ol_quantity) as average, " +
        " sum(ol_quantity) as sum, count(ol_quantity) as count_col, absolute_error(count_col) " +
      " from sampled_order_line6_col ")

    logInfo("sample table results=")
    logInfo(resultSampleTableTable.collect().mkString("\n"))

    val rowsSampleTable = resultSampleTableTable.collect()
    asserter(assert(rowsSampleTable(0).getLong(0) > 0), testNo)
    asserter(assert(rowsSampleTable(0).isNullAt(1)), testNo)
    asserter(assert(rowsSampleTable(0).isNullAt(2)), testNo)
    asserter(assert(rowsSampleTable(0).getLong(3) === 0), testNo)

    this.assertAnalysis()
    snc.sql("drop table if exists sampled_order_line6_col")
    snc.sql("drop table if exists order_line6_col")
  }

  test("null columns for aggregates : test 2") {
    testAggregateOnNullColumn_2()
  }

  def testAggregateOnNullColumn_2(): Unit = {
    val testNo = 8
    snc.sql(s"TRUNCATE TABLE $sampleAirlineUniqueCarrier")
    airlineDF.write.insertInto(sampleAirlineUniqueCarrier)
    val resultSample = snc.sql(s"Select sum(ArrDelay) as x ," +
      s"count(arrDelay) as y, count(*) as z,  avg(ArrDelay) as p from $airlineTable " +
      s"where Origin = 'DL' with error $DEFAULT_ERROR confidence 0.95")
    val rowsSample = resultSample.collect()

    val resultBase = snc.sql("Select sum(ArrDelay) as x ,count(arrDelay) as y, count(*) as z," +
        s" avg(ArrDelay) as p from $airlineTable where Origin = 'DL' ")
    val rowsBase = resultBase.collect()

    asserter(assert(rowsBase(0).isNullAt(0)), testNo)
    asserter(assert(rowsBase(0).getLong(1) === 0), testNo)
    asserter(assert(rowsBase(0).getLong(2) === 0), testNo)
    asserter(assert(rowsBase(0).isNullAt(3)), testNo)

    asserter(assert(rowsSample(0).isNullAt(0)), testNo)
    asserter(assert(rowsSample(0).getLong(1) === 0), testNo)
    asserter(assert(rowsSample(0).getLong(2) === 0), testNo)
    asserter(assert(rowsSample(0).isNullAt(3)), testNo)
  }

  test("NPE in bootstrap error bounds when sum or avg is 0") {

    val schema = StructType(Array(StructField("name", StringType, false),
      StructField("value", IntegerType, false)))

    val encoder = RowEncoder(schema)
    val df = snc.range(0, 100).map(row => { val id = row.getLong(0)
      Row("name_" + id.toInt, id.toInt)
    })(encoder)

    df.registerTempTable("base")
    snc.sql(s"CREATE SAMPLE TABLE sample_base" +
        s" OPTIONS(qcs 'name', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'base')")
    var query = s"select avg(value) avgg, lower_bound(avgg), upper_bound(avgg), " +
        s"absolute_error(avgg), relative_error(avgg) from sample_base group by name"
    var results = snc.sql(query).collect()
    results.foreach(row => 0 until 5 foreach(i => assert(!row.isNullAt(i)))

    )

    query = s"select sum(value) summ, lower_bound(summ), upper_bound(summ), " +
        s"absolute_error(summ), relative_error(summ) from sample_base group by name"
    results = snc.sql(query).collect()
    results.foreach(row => 0 until 5 foreach(i => assert(!row.isNullAt(i))))

    snc.sql("drop table if exists sample_base")
    snc.sql("drop table if exists base")

  }

  test("SNAP-806 test null for empty table and null row") {
    testSnap806()
  }

  def testSnap806(): Unit = {
    val testNo = 9
    snc.sql("drop table if exists sampled_order_line9_col")
    snc.sql("drop table if exists order_line9_col")
    snc.sql("create table order_line9_col(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       decimal(6,2)," +
        "ol_supply_w_id  integer," +
        "ol_quantity     decimal(2,0)," +
        "ol_quantity_mix decimal(2,0)," +
        "ol_dist_info    varchar(24))")

    snc.sql("CREATE SAMPLE TABLE sampled_order_line9_col" +
        " OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        " baseTable 'order_line9_col')")

    val orderLineEmptyCSV = getClass.getResource("/ORDER_LINE_NULL_SINGLE_ROW_VALS.csv").getPath
    var orderLineEmptyDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineEmptyCSV)

    snc.sql(
      """SELECT count(*) as count_star_on_empty_base_table
        | FROM order_line9_col """.stripMargin).collect()
    val result_count_star_empty = snc.sql(
      """SELECT count(*) as count_star_on_empty_sample_table
        | FROM sampled_order_line9_col """.stripMargin)
    val rows_count_star_empty = result_count_star_empty.collect()
    rows_count_star_empty.foreach(row => {
      asserter(assert(!row.isNullAt(0)), testNo)
    })

    snc.sql(
      """SELECT count(ol_quantity) as count_col_on_empty_base_table
        | FROM order_line9_col """.stripMargin).collect()
    val result_count_col_empty = snc.sql(
      """SELECT count(ol_quantity) as count_col_on_empty_sample_table,
        | absolute_error(count_col_on_empty_sample_table)
        | FROM sampled_order_line9_col """.stripMargin)
    val rows_count_col_empty = result_count_col_empty.collect()
    rows_count_col_empty.foreach(row => {
      asserter(assert(!row.isNullAt(0)), testNo)
    })

    // TODO: SNAP-826 NPE if we force an empty row in sample table
    //    orderLineEmptyDF.write.insertInto("order_line9_col")
    //    snc.sql(
    //      """SELECT count(*) as count_star_on_null_row_base_table
    //        | FROM order_line9_col """.stripMargin).collect()
    //    snc.sql(
    //      """SELECT count(ol_quantity) as count_col_on_null_row_base_table
    //        | FROM order_line9_col """.stripMargin).collect()
    //    orderLineEmptyDF.write.insertInto("sampled_order_line9_col")
    //    snc.sql(
    //      """SELECT count(*) as count_star_on_null_row_sample_table
    //        | FROM sampled_order_line9_col """.stripMargin).collect()

    this.assertAnalysis()
    snc.sql("drop table if exists sampled_order_line9_col")
    snc.sql("drop table if exists order_line9_col")
  }

  test("SNAP-823 test null for error stats") {
    testSnap823()
  }


  def testSnap823(): Unit = {
    val testNo = 10

    val result1 = snc.sql( s"""SELECT *  FROM $orderlineNullTable """)
    result1.collect()

    val result_cnt_null = snc.sql(s"SELECT count(ol_quantity) AS cnt_qty_null, " +
        s" absolute_error(cnt_qty_null) AS cnt_qty_null_ae," +
        s" relative_error(cnt_qty_null) AS cnt_qty_null_re," +
        s" lower_bound(cnt_qty_null) AS cnt_qty_null_lb," +
        s" upper_bound(cnt_qty_null) AS cnt_qty_null_up" +
        s" FROM $orderlineNullTable " +
        s" with error $DEFAULT_ERROR " +
        s" confidence .95")
    val rows_cnt_null = result_cnt_null.collect()
    rows_cnt_null.foreach(row => {
      asserter(assert(!row.isNullAt(0)), testNo)
      asserter(assert(row.isNullAt(1)), testNo)
      asserter(assert(row.isNullAt(2)), testNo)
      asserter(assert(row.isNullAt(3)), testNo)
      asserter(assert(row.isNullAt(4)), testNo)
    })

    val result_cnt_mix = snc.sql(s"SELECT count(ol_quantity_mix) AS cnt_qty_mix, " +
        s" absolute_error(cnt_qty_mix) AS cnt_qty_mix_ae," +
        s" relative_error(cnt_qty_mix) AS cnt_qty_mix_re," +
        s" lower_bound(cnt_qty_mix) AS cnt_qty_mix_lb," +
        s" upper_bound(cnt_qty_mix) AS cnt_qty_mix_ub" +
        s" FROM $orderlineNullTable " +
        s" with error $DEFAULT_ERROR " +
        s" confidence .95")
    val rows_cnt_mix = result_cnt_mix.collect()
    rows_cnt_mix.foreach(row => {
      asserter(assert(!row.isNullAt(0)), testNo)
      asserter(assert(!row.isNullAt(1)), testNo)
      asserter(assert(!row.isNullAt(2)), testNo)
      asserter(assert(!row.isNullAt(3)), testNo)
      asserter(assert(!row.isNullAt(4)), testNo)
    })

    val result_sum_null = snc.sql(s"SELECT sum(ol_quantity) AS sum_qty_null, " +
        s" absolute_error(sum_qty_null) AS sum_qty_null_ae," +
        s" relative_error(sum_qty_null) AS sum_qty_null_re," +
        s" lower_bound(sum_qty_null) AS sum_qty_null_lb," +
        s" upper_bound(sum_qty_null) AS sum_qty_null_up" +
        s" FROM $orderlineNullTable " +
        s" with error $DEFAULT_ERROR " +
        s" confidence .95")
    val rows_sum_null = result_sum_null.collect()
    rows_sum_null.foreach(row => {
      asserter(assert(row.isNullAt(0)), testNo)
      asserter(assert(row.isNullAt(1)), testNo)
      asserter(assert(row.isNullAt(2)), testNo)
      asserter(assert(row.isNullAt(3)), testNo)
      asserter(assert(row.isNullAt(4)), testNo)
    })

    val result_sum_mix = snc.sql(s"SELECT sum(ol_quantity_mix) AS sum_qty_mix, " +
        s" absolute_error(sum_qty_mix) AS sum_qty_mix_ae," +
        s" relative_error(sum_qty_mix) AS sum_qty_mix_re," +
        s" lower_bound(sum_qty_mix) AS sum_qty_mix_lb," +
        s" upper_bound(sum_qty_mix) AS sum_qty_mix_ub" +
        s" FROM $orderlineNullTable " +
        s" with error $DEFAULT_ERROR " +
        s" confidence .95")
    val rows_sum_mix = result_sum_mix.collect()
    rows_sum_mix.foreach(row => {
      asserter(assert(!row.isNullAt(0)), testNo)
      asserter(assert(!row.isNullAt(1)), testNo)
      asserter(assert(!row.isNullAt(2)), testNo)
      asserter(assert(!row.isNullAt(3)), testNo)
      asserter(assert(!row.isNullAt(4)), testNo)
    })

    val result_avg_null = snc.sql(s"SELECT avg(ol_quantity) AS avg_qty_null, " +
        s" absolute_error(avg_qty_null) AS avg_qty_null_ae," +
        s" relative_error(avg_qty_null) AS avg_qty_null_re," +
        s" lower_bound(avg_qty_null) AS avg_qty_null_lb," +
        s" upper_bound(avg_qty_null) AS avg_qty_null_up" +
        s" FROM $orderlineNullTable " +
        s" with error $DEFAULT_ERROR " +
        s" confidence .95")
    val rows_avg_null = result_avg_null.collect()
    rows_avg_null.foreach(row => {
      asserter(assert(row.isNullAt(0)), testNo)
      asserter(assert(row.isNullAt(1)), testNo)
      asserter(assert(row.isNullAt(2)), testNo)
      asserter(assert(row.isNullAt(3)), testNo)
      asserter(assert(row.isNullAt(4)), testNo)
    })

    val result_avg_mix = snc.sql(s"SELECT avg(ol_quantity_mix) AS avg_qty_mix, " +
        s" absolute_error(avg_qty_mix) AS avg_qty_mix_ae," +
        s" relative_error(avg_qty_mix) AS avg_qty_mix_re," +
        s" lower_bound(avg_qty_mix) AS avg_qty_mix_lb," +
        s" upper_bound(avg_qty_mix) AS avg_qty_mix_ub" +
        s" FROM $orderlineNullTable " +
        s" with error $DEFAULT_ERROR " +
        s" confidence .95")
    val rows_avg_mix = result_avg_mix.collect()
    rows_avg_mix.foreach(row => {
      asserter(assert(!row.isNullAt(0)), testNo)
      asserter(assert(!row.isNullAt(1)), testNo)
      asserter(assert(!row.isNullAt(2)), testNo)
      asserter(assert(!row.isNullAt(3)), testNo)
      asserter(assert(!row.isNullAt(4)), testNo)
    })

    this.assertAnalysis()


  }

  test("Test conistency of sum , count & average queries") {
    testConsistencyOfSumCountAvg()
  }


  def testConsistencyOfSumCountAvg(): Unit = {
    val testNo = 11
    val rs1 = snc.sql(s"SELECT SUM(ArrDelay) as sum, lower_bound(sum), " +
      s" upper_bound(sum) FROM $airlineTable  with error $DEFAULT_ERROR confidence .95")


    val sum1 = rs1.collect()(0).getLong(0)
    this.assertAnalysis()

    val rs2 = snc.sql(s"SELECT SUM(ArrDelay) as sum, lower_bound(sum), " +
      s"upper_bound(sum) FROM $airlineTable with error $DEFAULT_ERROR confidence .95")


    val sum2 = rs2.collect()(0).getLong(0)
    this.assertAnalysis()

    assert(sum1 === sum2)

    val rs3 = snc.sql(s"SELECT AVG(ArrDelay) as sum, lower_bound(sum), upper_bound(sum)" +
      s" FROM $airlineTable with error $DEFAULT_ERROR confidence .95")


    val avg1 = rs3.collect()(0).getDouble(0)
    this.assertAnalysis()

    val rs4 = snc.sql(s"SELECT AVG(ArrDelay) as sum, lower_bound(sum), upper_bound(sum) " +
      s" FROM $airlineTable with error $DEFAULT_ERROR confidence .95")


    val avg2 = rs4.collect()(0).getDouble(0)
    this.assertAnalysis()
    assert(Math.abs(avg1 - avg2) < .1)

    val rs5 = snc.sql(s"SELECT Count(ArrDelay) as sum, lower_bound(sum), upper_bound(sum) " +
      s"FROM $airlineTable with error $DEFAULT_ERROR confidence .95")


    val count1 = rs5.collect()(0).getLong(0)
    this.assertAnalysis()
    val rs6 = snc.sql(s"SELECT Count(ArrDelay) as sum, lower_bound(sum), upper_bound(sum) " +
      s"FROM $airlineTable with error $DEFAULT_ERROR confidence .95")


    val count2 = rs6.collect()(0).getLong(0)
    this.assertAnalysis()
    assert(count1 === count2)

  }

  test("Test Bug AQP-128 and AQP 96 and AQP -206_AQP77", Retryable) {
    testAQP128_AQP96_AQP206_AQP77()
  }


  def testAQP128_AQP96_AQP206_AQP77(): Unit = {
    val testNo = 12

    val rs1 = snc.sql(s"SELECT uniqueCarrier, avg(ArrDelay) as COUNT,avg(ArrDelay) as COUNT1 ," +
      s" absolute_error(count1) FROM $sampleAirlineUniqueCarrier where  MonthI < 2 " +
      s" group by uniqueCarrier order by uniqueCarrier desc")

    val rows = rs1.collect()
    assert(rows.length > 0)
    AssertAQPAnalysis.bootStrapAnalysis(snc)
    for (row <- rows) {
      assert(Math.abs(row.getDouble(1) - row.getDouble(2)) < .0001)
    }

    var avgGT10: Int = 0
    for (row <- rows) {
      if (row.getDouble(1) > 10) {
        avgGT10 = avgGT10 + 1
      }
    }

    val rs2 = snc.sql(s"SELECT uniqueCarrier, avg(ArrDelay) as COUNT,avg(ArrDelay) as COUNT1 ," +
      s"absolute_error(count1) FROM $sampleAirlineUniqueCarrier where MonthI < 2" +
      s" group by uniqueCarrier " +
      s" having avg(ArrDelay) > 10 behavior 'do_nothing'")

    val rows2 = rs2.collect()
    assert(rows2.length === avgGT10)
    for (row <- rows2) {
      assert(row.getDouble(1) > 10)
    }

    val rs3 = snc.sql(s"Select uniqueCarrier, sum(ArrDelay) as x , " +
      s"absolute_error( x ),relative_error( x ) from $airlineTable " +
      s"group by uniqueCarrier having relative_error( x ) < 0.9 " +
      s"order by uniqueCarrier  desc with error")

    val roows = rs3.collect()
    assert(roows.length > 0)
    roows.foreach(row => assert(row.getDouble(3) < .9))

    val sdf = snc.sql(s"select avg(DepDelay) delay, Origin, absolute_error(delay) " +
      s"from $sampleAirlineUniqueCarrier group by Origin order by delay desc limit 100")
    logInfo(sdf.count().toString)
    this.assertAnalysis()

    val sdf1 = snc.sql(s"select avg(DepDelay) delay, Origin, absolute_error(delay) " +
      s"from $airlineTable  group by Origin order by delay desc limit 100 with error")
    logInfo(sdf1.count().toString)
    this.assertAnalysis()
  }

  test("Bug NPE in getting sample_count") {
    val testNo = 13
    snc.sql("drop table if exists sampled_order_line13_col")


    snc.sql("CREATE SAMPLE TABLE sampled_order_line13_col" +
        " OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable '$orderlineNullTable')")

    snc.sql(
      """SELECT count(*) as sample_cnt FROM sampled_order_line13_col """.stripMargin).collect()

    snc.sql("drop table if exists sampled_order_line13_col")

  }


  test("AvgQueryWithWhereClauseUsesBootstrapAnalysis") {
    val testNo = 14

    val rs1 = snc.sql(s"SELECT uniqueCarrier, avg(ArrDelay) avg, absolute_error(avg)" +
      s" FROM $sampleAirlineUniqueCarrier where" +
        s"  MonthI < 2 group by uniqueCarrier ")
    rs1.collect()

    AssertAQPAnalysis.bootStrapAnalysis(snc)

    val rs2 = snc.sql(s"SELECT AVG(ArrDelay) as AVG, lower_bound(AVG),upper_bound(AVG) FROM" +
        s" $airlineTable where MonthI > 0 with error 0.5 confidence .95")
    rs2.collect()
    AssertAQPAnalysis.bootStrapAnalysis(snc)
  }



  test("incorrect sort in show method") {
    val testNo = 15

    snc.sql("set spark.sql.shuffle.partitions=6")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       decimal(6,2)," +
        "ol_supply_w_id  integer," +
        "ol_quantity     decimal(2,0)," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension)

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_5000.csv").getPath
    val customSchema = StructType(Array(
      StructField("ol_w_id", IntegerType, true),
      StructField("ol_d_id", IntegerType, true),
      StructField("ol_o_id", IntegerType, true),
      StructField("ol_number", IntegerType, true),
      StructField("ol_i_id", IntegerType, true),
      StructField("ol_amount", DecimalType(10, 4), true),
      StructField("ol_supply_w_id", IntegerType, true),
      StructField("ol_quantity", DecimalType(4, 2), true),
      StructField("ol_dist_info", StringType, true)))
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("maxCharsPerColumn", "4096")
        .schema(customSchema)
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")


    val pop_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s"")
    //  pop_result_avg_0.collect()
    val pop_value_avg0 = pop_result_avg_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](pop_result_avg_0, pop_value_avg0, _.getDouble(0))
    pop_value_avg0.foreach(r => doPrint(r.toString()))

    val sam_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 0.9 confidence .95 behavior 'DO_NOTHING'")
    val sam_row_avg0 = sam_result_avg_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_0, sam_row_avg0, _.getDouble(0))
    sam_row_avg0.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg0.length == pop_value_avg0.length)
    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo ")
  }



  // This test takes a lot of time on newyork taxi data & takes lot of time. However it does test
  // important bugs, so periodiccally it needs to be enabled & run.
  // Currently this test is being run on a very small subset of total data.
  // periodically run it on full data
  test("Bug AQP-154, AQP-204, AQP-205, AQP-94, AQP-207") {
    val testNo = 17


    val fraction = .08
    val sampleDataFrame = snc.createSampleTable("nycSample",
      Some(nyc), Map("qcs" -> "vendor_id",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "70"),
      allowExisting = false)

    val rs1 = snc.sql(s"SELECT count(*) as sample_ FROM nycSample ")
    this.assertAnalysis()
    val sampleCount = rs1.collect()(0).getLong(0)
    logInfo("actual sample count=" + sampleCount)

    val rs3 = snc.sql(s"SELECT count(*) as countfromsample , absolute_error(countfromsample)" +
      s" FROM nycSample ")
    this.assertAnalysis()
    val estimatedCount = rs3.collect()(0).getLong(0)
    logInfo("estimated count from sample =" + estimatedCount)

    val rs2 = snc.sql(s"SELECT count(*) as  actual FROM $nyc ")
    val actualCount = rs2.collect()(0).getLong(0)
    logInfo("actual count  =" + actualCount)
    assert(Math.abs(actualCount - estimatedCount) < 3)

    logInfo(" expected sample count  = " + fraction * actualCount)
    val actualSampleFraction = sampleCount.toDouble / actualCount.toDouble
    // This assertion was previously < .2 which holds if the full newyork taxi table data
    // is considererd
    // but with small amount of data the fraction sampled breaches the limit & deficit is around .35
    // why?
    assert(Math.abs(fraction - actualSampleFraction) / fraction < .4)


    val rsx = snc.sql(s"select avg(trip_time_in_secs/60) tripTime, hour(pickup_datetime), " +
        s" count(*) howManyTrips, absolute_error(tripTime) from nycSample " +
      s" where pickup_latitude < 40.767588 and pickup_latitude > 40.749775 " +
        s" and pickup_longitude > -74.001632 and  " +
      s" pickup_longitude < -73.974595 and dropoff_latitude > 40.716800    " +
        s" and  dropoff_latitude <  40.717776 and dropoff_longitude >  -74.017682 " +
      s" and dropoff_longitude < -74.000945  " +
        s" group by hour(pickup_datetime) with error")
    rsx.collect()

    val res1 = snc.sql(s"select avg(trip_distance) as avgTripDist," +
      s" medallion, absolute_error(avgTripDist) from nycSample where trip_distance > 1 " +
        s"group by medallion order by medallion, avgTripDist desc limit 10 " +
      s"with error")
    AssertAQPAnalysis.bootStrapAnalysis(snc)

    val res2 = snc.sql("select avg(trip_distance) as avgTripDist," +
      "medallion from nycSample where trip_distance > 1 " +
      "group by medallion order by medallion, avgTripDist desc limit 10 " +
      "with error")

    val res3 = snc.sql("select avg(trip_distance) as avgTripDist,medallion " +
      "from nycSample where trip_distance > 1 " +
        "group by medallion order by medallion, avgTripDist desc limit 10 " +
      "with error")

    val rows1 = res1.collect()
    val rows2 = res2.collect()
    val rows3 = res3.collect()

    val validator: PartialFunction[(Row, Row), Unit] = {
      case (x, y) => assert(x.getDouble(0) === y.getDouble(0))
        assert(x.getString(1) === y.getString(1))
    }
    rows1.zip(rows2).foreach {
      validator
    }
    rows1.zip(rows3).foreach {
      validator
    }



    val rsAvg = snc.sql(s"select avg(trip_time_in_secs/60) tripTime, hour(pickup_datetime), " +
      s" count(*) howManyTrips,  absolute_error(tripTime) from $nyc" +
      s" where pickup_latitude < 40.767588 and pickup_latitude > 40.749775 " +
      s"and pickup_longitude > -74.001632 and  pickup_longitude < -73.974595 " +
      s" and dropoff_latitude > 40.716800  and  dropoff_latitude <  40.717776 " +
      s" and dropoff_longitude >  -74.017682 and dropoff_longitude < -74.000945 " +
        s" group by hour(pickup_datetime) with error")
    val rowsAvg = rsAvg.collect()
    rowsAvg.foreach(row => {
      logInfo("avg = " + row.getDouble(0) + "; hour = " + row.getInt(1) + "; " +
          "count = " + row.getLong(2))
    })

    snc.sql("drop table if exists nycSample")

  }

  // This test is currently being run on very small subset of newyork taxi data.
  // Periodically run it on full data
  // by using local data files.
  test("Bug AQP-204") {
    this.testBugAQP_204()
  }

  def testBugAQP_204(): Unit = {
    val testNo = 18


    val fraction = .09
    val sampleDataFrame = snc.createSampleTable("nycSample",
      Some(nyc), Map("qcs" -> "medallion",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "70"),
      allowExisting = false)
      val res1 = snc.sql(s"select avg(trip_distance) as avgTripDist," +
      s" medallion, sum(trip_distance) as totalDistance, " +
        s" count(*) as count, absolute_error(count) from " +
        s" $nyc where (trip_distance  >= 0  or trip_distance  <= 0  ) " +
        s"group by medallion order by avgTripDist desc limit 10 " +
      s"with error")
    AssertAQPAnalysis.bootStrapAnalysis(snc)

    val res2 = snc.sql(s"select avg(trip_distance) as avgTripDist," +
      s" medallion, sum(trip_distance) as totalDistance," +
        s" count(*) as count, absolute_error(count)" +
        s" from $nyc where (trip_distance  >= 0  or trip_distance  <= 0  ) " +
        s"group by medallion order by avgTripDist desc limit 10" +
      s" with error")
    AssertAQPAnalysis.bootStrapAnalysis(snc)
    val res3 = snc.sql(s"select avg(trip_distance) as avgTripDist," +
      s"medallion, sum(trip_distance) as totalDistance," +
        s" count(*) as count, absolute_error(count) " +
        s"from $nyc where (trip_distance  >= 0  or" +
      s" trip_distance  <= 0  ) " +
        s"group by medallion order by avgTripDist desc limit 10 " +
      s"with error")
    AssertAQPAnalysis.bootStrapAnalysis(snc)
    val rows1 = res1.collect()
    val rows2 = res2.collect()
    val rows3 = res3.collect()

    val validator: PartialFunction[(Row, Row), Unit] = {
      case (x, y) => assert(x.getDouble(0).toLong === y.getDouble(0).toLong)
        assert(x.getString(1) === y.getString(1))


    }
    rows1.zip(rows2).foreach {
      validator
    }
    rows1.zip(rows3).foreach {
      validator
    }

    val res4 = snc.sql(s"select avg(trip_distance) as avgTripDist," +
      s"medallion, sum(trip_distance) as totalDistance, " +
       s" count(*) as count, absolute_error(count) from " +
        s" $nyc group by medallion " +
      s" order by avgTripDist " +
      s" desc limit 10 with error")
    this.assertAnalysis()

    val res5 = snc.sql(s"select avg(trip_distance) as avgTripDist, " +
      s"medallion, sum(trip_distance) as totalDistance, " +
        s" count(*) as count, absolute_error(count)" +
        s" from $nyc group by medallion " +
      s"order by avgTripDist desc limit 10 " +
      s"with error")
    this.assertAnalysis()
    val res6 = snc.sql(s"select avg(trip_distance) as avgTripDist," +
      s"medallion, sum(trip_distance) as totalDistance ," +
        s" count(*) as count, absolute_error(count)" +
        s" from $nyc group by medallion " +
      s"order by avgTripDist desc limit 10" +
      s" with error")
    this.assertAnalysis()
    val rows4 = res4.collect()
    val rows5 = res5.collect()
    val rows6 = res6.collect()

    rows4.zip(rows5).foreach {
      validator
    }
    rows4.zip(rows6).foreach {
      validator
    }

    rows1.zip(rows4).foreach {
      validator
    }

    snc.sql("drop table if exists nycSample")

  }

  test("Bug AQP-210 and AQP-227") {
    val testNo = 19

    val rs1 = snc.sql(s"SELECT uniqueCarrier, sum(ArrDelay) as x, lower_bound(x), upper_bound(x)," +
        s"  lower_bound(x) + upper_bound(x)   FROM $airlineTable where " +
        s"  MonthI < 2 group by uniqueCarrier  with error")
    this.assertAnalysis()
    val rows = rs1.collect()
    rows.foreach(row => assert((row.getDouble(2) + row.getDouble(3)) == row.getDouble(4)))

    val rs2 = snc.sql(s"SELECT uniqueCarrier, sum(ArrDelay) as x, lower_bound(x), upper_bound(x)," +
        s"  lower_bound(x) + upper_bound(x)   FROM $airlineTable where " +
        s"  MonthI < 2 group by uniqueCarrier " +
        s"having lower_bound(x) + upper_bound(x) > 0 with error")
    this.assertAnalysis()
    val rows1 = rs2.collect()
    rows1.foreach(row => {
      assert((row.getDouble(2) + row.getDouble(3)) == row.getDouble(4))
      assert(row.getDouble(4) > 0)

    })

    val rs3 = snc.sql(s"SELECT uniqueCarrier, sum(ArrDelay) as x, avg(ArrDelay) as y ," +
      s" lower_bound(x), lower_bound(x) + upper_bound(x) , " +
      s" relative_error(y) + absolute_error(y), relative_error(y), " +
      s" absolute_error(y) ,upper_bound(x)  FROM $airlineTable where " +
        s"  MonthI < 2 group by uniqueCarrier  with error")
    AssertAQPAnalysis.bootStrapAnalysis(snc)
    val rows2 = rs3.collect()
    rows2.foreach(row => {
      assert((row.getDouble(3) + row.getDouble(8)) == row.getDouble(4))
      assert((row.getDouble(6) + row.getDouble(7)) == row.getDouble(5))
    })

    val rs4 = snc.sql(s"SELECT uniqueCarrier, sum(ArrDelay) as x, lower_bound(x), upper_bound(x)," +
        s"  lower_bound(x) + upper_bound(x)   FROM $airlineTable where " +
        s"  MonthI < 2 group by uniqueCarrier order by uniqueCarrier with error")
    this.assertAnalysis()
    val rowX = rs4.collect()
    val rowsY = rowX.filter(row => row.getDouble(2) < 0)
    // there exists negative rows
    assert(rowsY.length > 0)
    rowX.foreach(row => assert( (row.getDouble(2) + row.getDouble(3)) ===  row.getDouble(4)))
    val rows3 = rowX.filter(row => row.getDouble(4) < 0)

    val rs5 = snc.sql(s"SELECT uniqueCarrier, sum(ArrDelay) as x, lower_bound(x), upper_bound(x)," +
        s"  lower_bound(x) + upper_bound(x)   FROM $airlineTable where " +
        s"  MonthI < 2 group by uniqueCarrier  having (-1)*(lower_bound(x) + upper_bound(x)) > 0 " +
        s" order by uniqueCarrier with error")
    this.assertAnalysis()
    val rows4 = rs5.collect()

    assert(rows4.length == rows3.length)
    rows4.foreach(row => {
      assert(row.getDouble(4) < 0)
    })

    rows4.zip(rows3).foreach {
      case (row1, row2) => {
        assert(row1.getString(0) == row2.getString(0))
        assert(row1.getLong(1) == row2.getLong(1))
        assert(row1.getDouble(2) == row2.getDouble(2))
        assert(row1.getDouble(3) == row2.getDouble(3))
        assert(row1.getDouble(4) == row2.getDouble(4))
      }
    }

    // Test AQP-227
    val rs6 = snc.sql("select avg(taxiin + taxiout) avgTaxiTime, count( * ) numFlights, " +
      s"dest, avg(arrDelay) arrivalDelay, absolute_error(arrivalDelay) from $airlineTable " +
      s" where (taxiin > 60 or taxiout > 60) and dest in  (select dest from $airlineTable " +
      s" group by dest having count ( * ) >1000000) group by dest order " +
          s" by avgTaxiTime desc with error")

    val rows6 = rs6.collect()
    AssertAQPAnalysis.bootStrapAnalysis(snc)
    if(rows6.isEmpty) {
      val rs7 = snc.sql("select avg(taxiin + taxiout) avgTaxiTime, count( * ) numFlights," +
        s" dest, avg(arrDelay) arrivalDelay,  absolute_error(arrivalDelay) from $airlineTable " +
        s"where (taxiin > 60 or taxiout > 60) and dest in  (select dest from $airlineTable " +
        s"group by dest having count ( * ) >1000000) group by dest order " +
          " by avgTaxiTime desc ")

      val rows7 = rs7.collect()
      assert(snc.sessionState.contextFunctions.asInstanceOf[SnappyContextAQPFunctions]
          .aqpInfo.isEmpty)
      assert(rows7.isEmpty)
    }
  }

  // This test is currently being run on very small subset of newyork taxi data.
  // Periodically run it on full data
  // by using local data files.
  test("AQP-214") {
    this.testAQP214()
  }

  def testAQP214(): Unit = {
    val testNo = 20
   try {
     val fraction = .04
      val sampleDataFrame = snc.createSampleTable("nycSample",
        Some(nyc), Map("qcs" -> "hour(pickup_datetime)",
          "fraction" -> s"$fraction", "strataReservoirSize" -> "50",
          ExternalStoreUtils.COLUMN_BATCH_SIZE -> "2000"),
        allowExisting = false)

     // Verify that passing of batch column size as -1 in the cache batch creator does not cause
     // system to have only single entry for all stratum flushed
     // Get the total number of disticnt weights. The GFE region should contain atleast that many
     // stratums as each distinct weight should result in atleast 1 stratum ( or more than 1)

     val numWeights = snc.sql(s"select distinct" +
       s" ${org.apache.spark.sql.collection.Utils.WEIGHTAGE_COLUMN_NAME} " +
       s" from nycSample ").collect().length
     assert(numWeights > 0)
     // Get total GFE region entry count
     val entryCount = Misc.getRegionForTable(
       ColumnFormatRelation.columnBatchTableName("APP.NYCSAMPLE"), true).size()

      logInfo(" number of weights = " + numWeights + ", number of entries =" + entryCount)
     assert(entryCount <= numWeights)

      val res1 = snc.sql(s"select  hour(pickup_datetime)," +
        s" count(*) howManyTrips, absolute_error(howManyTrips) " +
        s"from $nyc  group by hour(pickup_datetime) order by " +
          s" hour(pickup_datetime) with error")
      val rows1 = res1.collect()
      this.assertAnalysis()

      val res2 = snc.sql(s"select  hour(pickup_datetime)," +
        s"count(*) howManyTrips from $nyc  " +
        s"group by hour(pickup_datetime) " +
        s"order by  hour(pickup_datetime) ")
      val rows2 = res2.collect()
      assert(rows1.length > 0)
      rows1.zip(rows2).foreach(bugAQP214Asserter)

      val res3 = snc.sql(s"select count(*) howManyTrips, absolute_error(howManyTrips) " +
        s"from $nyc  with error")
      val rows3 = res3.collect()
      this.assertAnalysis()

      val res4 = snc.sql(s"select  count(*) howManyTrips from $nyc ")
      val rows4 = res4.collect()
      assert(rows3.length > 0)
      rows3.zip(rows4).foreach(bugAQP214Asserter)

    } catch {
      case th: Throwable => {
        th.printStackTrace()
        throw th
      }
    }
    snc.sql("drop table if exists nycSample")
  }

  val bugAQP214Asserter : ((Row, Row)) => Unit

  // NYCTaxi Zeppelin Bug
  test("Bug AQP224: Sample Table Query in subquery with Join, AQP-247"){
    this.testAQP224_AQP247()
  }

  def testAQP224_AQP247() {
    val testNo = 22
    val fraction = .01
    val sampleDataFrame = snc.createSampleTable("NYCTAXI_SAMPLEHACKLICENSE",
      Some(nyc),
      // TODO - AQP-232: Takes long time with two columns, and one column having a function
      // Map("qcs" -> "hack_license, to_date(pickup_datetime)",
      Map("qcs" -> "hack_license",
        "buckets" -> "8",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "50"),
      allowExisting = false)


    snc.sql(s"CREATE SAMPLE TABLE TAXIFARE_SAMPLEHACKLICENSE on $taxiFare " +
      s"options  (buckets '8', qcs 'hack_license, year(pickup_datetime), month(pickup_datetime)'," +
      s" fraction '0.01', strataReservoirSize '50') AS (SELECT * FROM $taxiFare)")

    val baseQuery = snc.sql(s"Select Trips.hack_license, " +
        s" sum(Earning.daily_fare_amount) as monthly_income " +
        s" from (Select hack_license, " +
        s" to_date(pickup_datetime) as pickup_date, " +
        s" sum(trip_distance) as daily_trips " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9 " +
        s" group by hack_license, " +
        s" to_date(pickup_datetime)) Trips " +
        s" join " +
        s" $dailyTaxiFare Earning " +
        s" ON Trips.hack_license = Earning.hack_license " +
        s" AND Trips.pickup_date = Earning.pickup_date " +
        s" where Trips.daily_trips > 10 AND " +
        s" year(Earning.pickup_date) = 2013 and " +
        s" month(Earning.pickup_date) = 9 " +
        s" group by Trips.hack_license " +
        s" order by monthly_income desc")
    val baseQueryCollect = baseQuery.collect()
    baseQueryCollect.foreach(row => {
      doPrint(row.getString(0) + "; " + row.getLong(1))
    })

    //  AQP-224 fails here
    val sampleQuery = snc.sql(s"Select Trips.hack_license, " +
        s" sum(Earning.daily_fare_amount) as monthly_income " +
        s" from (Select hack_license, " +
        s" to_date(pickup_datetime) as pickup_date, " +
        s" sum(trip_distance) as daily_trips " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9 " +
        s" group by hack_license, " +
        s" to_date(pickup_datetime)) Trips " +
        s" join " +
        s" $dailyTaxiFare Earning " +
        s" ON Trips.hack_license = Earning.hack_license " +
        s" AND Trips.pickup_date = Earning.pickup_date " +
        s" where Trips.daily_trips > 10 AND " +
        s" year(Earning.pickup_date) = 2013 and " +
        s" month(Earning.pickup_date) = 9 " +
        s" group by Trips.hack_license " +
        s" order by monthly_income desc" +
        s" with error")

    val sampleQueryCollect = sampleQuery.collect()
    sampleQueryCollect.foreach(row => {
      doPrint(row.getString(0) + "; " + row.getLong(1))
    })



    // AQP-247
    val rs1 = snc.sql(s"Select Trips.hack_license, Trips.trips, Earning.earn from (" +
        s" select hack_license, count(trip_distance) as trips from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9" +
        s" group by hack_license order by trips desc limit 100 ) Trips join " +
        s" (select hack_license, avg(fare_amount) as earn from $taxiFare " +
        s" where year(pickup_datetime) = 2013 and month(pickup_datetime) = 9 " +
        s" group by hack_license order by earn desc limit 100) " +
        s" Earning ON Trips.hack_license = Earning.hack_license order by earn desc with error")
    rs1.collect()

    val rs2 = snc.sql(s"Select Trips.hack_license, Trips.trips, Earning.earn from (" +
      s"select hack_license, count(trip_distance) as trips,  absolute_error(trips) " +
      s"from $nyc where year(pickup_datetime) = 2013  and month(pickup_datetime) = 9" +
        s" group by hack_license order by trips desc limit 100 with error) Trips " +
      s" join (select hack_license, avg(fare_amount) as earn,  absolute_error(earn) " +
      s" from $taxiFare where year(pickup_datetime) = 2013 and month(pickup_datetime) = 9 " +
        s" group by hack_license order by earn desc limit 100 with error) Earning " +
        s" ON Trips.hack_license = Earning.hack_license  order by earn desc ")
    rs2.collect()
    this.assertAnalysis(0)
    AssertAQPAnalysis.bootStrapAnalysis(snc, 1)

    val rs3 = snc.sql(s"Select Trips.hack_license, Trips.trips, Earning.earn from (" +
      s"select hack_license, count(trip_distance) as trips,  absolute_error(trips) " +
      s" from $nyc where year(pickup_datetime) = 2013  and month(pickup_datetime) = 9" +
        s" group by hack_license order by trips desc limit 100 ) Trips join (select hack_license," +
        s" avg(fare_amount) as earn,  absolute_error(earn) from $taxiFare" +
      s" where year(pickup_datetime) = 2013 and month(pickup_datetime) = 9 " +
        s" group by hack_license order by earn desc limit 100 ) Earning " +
        s" ON Trips.hack_license = Earning.hack_license  order by earn desc with error ")

    rs3.collect()
    this.assertAnalysis(0)
    AssertAQPAnalysis.bootStrapAnalysis(snc, 1)
    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
    snc.sql("drop table if exists TAXIFARE_SAMPLEHACKLICENSE")

  }


  // This test is currently being run on very small subset of newyork taxi data. Periodically run
  // it on full data by using local data files.
  test("Bug AQP-229, AQP-239, AQP-246, AQP-230") {
      val fraction = .04
      val sampleDataFrame = snc.createSampleTable("nycSample",
        Some(nyc), Map("qcs" -> "hour(pickup_datetime)",
          "fraction" -> s"$fraction", "strataReservoirSize" -> "50"),
        allowExisting = false)


    val res1 = snc.sql(s"select hack_license,avg(trip_distance) from $nyc " +
      s" where trip_distance < 10 group by hack_license order by hack_license desc limit 100 " +
      s"with error 0.03 behavior 'local_omit'")
    val rows1 = res1.collect()
    var foundANull = false
    rows1.foreach(row => if (row.isNullAt(1)) {
      foundANull = true
    } )
    assert(foundANull)

    AssertAQPAnalysis.bootStrapAnalysis(snc)

    val rs1 = snc.sql(s"select sum(trip_distance) numOfRides,relative_error(numOFRides), " +
        s" CASE WHEN month(pickup_datetime) = 1 THEN 'Jan' " +
      s" WHEN month(pickup_datetime) = 2 THEN 'Feb' " +
      s" WHEN month(pickup_datetime) = 3 THEN 'Mar' " +
      s" WHEN month(pickup_datetime) = 4 THEN 'Apr' " +
      s" WHEN month(pickup_datetime) = 5 THEN 'May' " +
      s" WHEN month(pickup_datetime) = 6 THEN 'Jun'" +
      s" WHEN month(pickup_datetime) = 7 THEN 'Jul' " +
      s" WHEN month(pickup_datetime) = 8 THEN 'Aug' " +
      s" WHEN month(pickup_datetime) = 9 THEN 'Sep' " +
      s" WHEN month(pickup_datetime) = 10 THEN 'Oct' " +
      s" WHEN month(pickup_datetime) = 11 THEN 'Nov' " +
      s" WHEN month(pickup_datetime) = 12 THEN 'Dec'" +
      s" END AS mnt from $nyc group by month(pickup_datetime) order by month(pickup_datetime)" +
      s"  with error 0.9 behavior 'strict'")
  val rows_1 = rs1.collect()

  val asserter : Array[Row] => Unit = (rows: Array[Row]) => {
     assert(rows.length > 0)
     rows.foreach(row => {
       val mnt = row.getString(2)
       assert( mnt == "Jan" || mnt == "Feb" || mnt == "Mar" || mnt == "Apr" || mnt == "May"
         || mnt == "Jun" || mnt == "Jul" || mnt == "Aug"|| mnt == "Sep"|| mnt == "Oct"||
         mnt == "Nov"|| mnt == "Dec")
     })
     }

    asserter(rows_1)

    val rs2 = snc.sql(s"select sum(trip_distance) numOfRides,relative_error(numOFRides), " +
      s" CASE WHEN month(pickup_datetime) = 1 THEN 'Jan' " +
      s" WHEN month(pickup_datetime) = 2 THEN 'Feb' " +
      s" WHEN month(pickup_datetime) = 3 THEN 'Mar' " +
      s" WHEN month(pickup_datetime) = 4 THEN 'Apr' " +
      s" WHEN month(pickup_datetime) = 5 THEN 'May' " +
      s" WHEN month(pickup_datetime) = 6 THEN 'Jun'" +
      s" WHEN month(pickup_datetime) = 7 THEN 'Jul' " +
      s" WHEN month(pickup_datetime) = 8 THEN 'Aug' " +
      s" WHEN month(pickup_datetime) = 9 THEN 'Sep' " +
      s" WHEN month(pickup_datetime) = 10 THEN 'Oct' " +
      s" WHEN month(pickup_datetime) = 11 THEN 'Nov' " +
      s" WHEN month(pickup_datetime) = 12 THEN 'Dec'" +
      s" END AS mnt from $nyc group by month(pickup_datetime) order by month(pickup_datetime)" +
      s"  with error 0.0000001 behavior 'do_nothing'")
    val rows_2 = rs2.collect()

    asserter(rows_2)

    val rs3 = snc.sql(s"select sum(trip_distance) numOfRides,relative_error(numOFRides), " +
      s" CASE WHEN month(pickup_datetime) = 1 THEN 'Jan' " +
      s" WHEN month(pickup_datetime) = 2 THEN 'Feb' " +
      s" WHEN month(pickup_datetime) = 3 THEN 'Mar' " +
      s" WHEN month(pickup_datetime) = 4 THEN 'Apr' " +
      s" WHEN month(pickup_datetime) = 5 THEN 'May' " +
      s" WHEN month(pickup_datetime) = 6 THEN 'Jun'" +
      s" WHEN month(pickup_datetime) = 7 THEN 'Jul' " +
      s" WHEN month(pickup_datetime) = 8 THEN 'Aug' " +
      s" WHEN month(pickup_datetime) = 9 THEN 'Sep' " +
      s" WHEN month(pickup_datetime) = 10 THEN 'Oct' " +
      s" WHEN month(pickup_datetime) = 11 THEN 'Nov' " +
      s" WHEN month(pickup_datetime) = 12 THEN 'Dec'" +
      s" END AS mnt from $nyc group by month(pickup_datetime) order by month(pickup_datetime)" +
      s"  with error 0.0000001 behavior 'local_omit'")
    val rows_3 = rs3.collect()

    asserter(rows_3)

   // AQP-246 disabled for now
   val rs4 = snc.sql("select sum(trip_distance) numOfRides,relative_error(numOFRides), " +
        " CASE WHEN month(pickup_datetime) = 1 THEN 'Jan' " +
        " WHEN month(pickup_datetime) = 2 THEN 'Feb' " +
        " WHEN month(pickup_datetime) = 3 THEN 'Mar' " +
        " WHEN month(pickup_datetime) = 4 THEN 'Apr' " +
        " WHEN month(pickup_datetime) = 5 THEN 'May' " +
        " WHEN month(pickup_datetime) = 6 THEN 'Jun'" +
        " WHEN month(pickup_datetime) = 7 THEN 'Jul' " +
        " WHEN month(pickup_datetime) = 8 THEN 'Aug' " +
        " WHEN month(pickup_datetime) = 9 THEN 'Sep' " +
        " WHEN month(pickup_datetime) = 10 THEN 'Oct' " +
        " WHEN month(pickup_datetime) = 11 THEN 'Nov' " +
        " WHEN month(pickup_datetime) = 12 THEN 'Dec'" +
        " END AS mnt from nyc group by month(pickup_datetime) order by month(pickup_datetime)" +
        "  with error 0.0000001 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")
    val rows_4 = rs4.collect()

    asserter(rows_4)


    val rs5 = snc.sql(s"select sum(trip_distance) numOfRides,relative_error(numOFRides), " +
        s" CASE WHEN month(pickup_datetime) = 1 THEN 'Jan' " +
      s" WHEN month(pickup_datetime) = 2 THEN 'Feb' " +
      s" WHEN month(pickup_datetime) = 3 THEN 'Mar' " +
      s" WHEN month(pickup_datetime) = 4 THEN 'Apr' " +
      s" WHEN month(pickup_datetime) = 5 THEN 'May' " +
      s" WHEN month(pickup_datetime) = 6 THEN 'Jun'" +
      s" WHEN month(pickup_datetime) = 7 THEN 'Jul' " +
      s" WHEN month(pickup_datetime) = 8 THEN 'Aug' " +
      s" WHEN month(pickup_datetime) = 9 THEN 'Sep' " +
      s" WHEN month(pickup_datetime) = 10 THEN 'Oct' " +
      s" WHEN month(pickup_datetime) = 11 THEN 'Nov' " +
      s" WHEN month(pickup_datetime) = 12 THEN 'Dec'" +
      s" END AS mnt from $nyc group by month(pickup_datetime) " +
      s"  with error 0.0000001 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")
    val rows_5 = rs5.collect()

    asserter(rows_5)


    val rs6 = snc.sql(s"select sum(trip_distance) numOfRides,relative_error(numOFRides), " +
        s" CASE WHEN month(pickup_datetime) = 1 THEN 'Jan' " +
      s" WHEN month(pickup_datetime) = 2 THEN 'Feb' " +
      s" WHEN month(pickup_datetime) = 3 THEN 'Mar' " +
      s" WHEN month(pickup_datetime) = 4 THEN 'Apr' " +
      s" WHEN month(pickup_datetime) = 5 THEN 'May' " +
      s" WHEN month(pickup_datetime) = 6 THEN 'Jun'" +
      s" WHEN month(pickup_datetime) = 7 THEN 'Jul' " +
      s" WHEN month(pickup_datetime) = 8 THEN 'Aug' " +
      s" WHEN month(pickup_datetime) = 9 THEN 'Sep' " +
      s" WHEN month(pickup_datetime) = 10 THEN 'Oct' " +
      s" WHEN month(pickup_datetime) = 11 THEN 'Nov' " +
      s" WHEN month(pickup_datetime) = 12 THEN 'Dec'" +
      s" END AS mnt from $nyc group by month(pickup_datetime) " +
      s"  with error 0.0000001 behavior 'RUN_ON_FULL_TABLE'")
    val rows_6 = rs6.collect()

    asserter(rows_6)


    // test

    val rs7 = snc.sql(s"select count(trip_distance) as avgTripDist, medallion, " +
      s"absolute_error(avgTripDist), relative_error(avgTripDist),lower_bound(avgTripDist)," +
      s"upper_bound(avgTripDist) from $nyc where trip_distance > 1 group by medallion " +
      s"order by avgTripDist desc limit 10 with error 0.01 behavior 'local_omit'")
    rs7.collect()

    snc.sql("drop table if exists nycsample")



  }


  ignore("query sample table taking less time than base table with error.fixed with " +
    "caching of catalog lookup ") {
    val hfile: String = getClass.getResource("/NYC_trip_ParquetData").getPath
    // val hfile: String = getClass.getResource("/NYC_trip_ParquetData").getPath
    val nycDF = snc.read.parquet(hfile)

    nycDF.registerTempTable("nyc")



    val fraction = .04
    val sampleDataFrame = snc.createSampleTable("nycSample",
      Some("nyc"), Map("qcs" -> "hour(pickup_datetime)",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "50"),
      allowExisting = false)

    val t1 = System.currentTimeMillis()
    val res1 = snc.sql(s"select sum(trip_distance) from NYC  " +
      s" with error ")
    res1.collect()
    val t2 = System.currentTimeMillis()
    logInfo("Time taken in millisec = " + (t2 - t1))
    val rows1 = res1.collect()

    AssertAQPAnalysis.bootStrapAnalysis(snc)

    var sum : Long = 0
    for(i <- 0 until 10) {
      val t1 = System.currentTimeMillis()
      val res1 = snc.sql(s"select sum(trip_distance) from NYC  " +
        s" with error ")
      res1.collect()
      val t2 = System.currentTimeMillis()
      logInfo("Time taken in millisec = " + (t2 - t1))
      sum += (t2 - t1)
    }

    logInfo("avg time for 10 queries =" + sum/10d)
  }

  // NYCTaxi Zeppelin Bug
  test("Bug AQP231: Sample Table subquery in a temp table and then joined"){
    this.testAQP231_1()
  }

  def testAQP231_1(): Unit = {
    val testNo = 23
    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
    val fraction = .01
    val sampleDataFrame = snc.createSampleTable("NYCTAXI_SAMPLEHACKLICENSE",
      Some(nyc),
      // TODO - AQP-232: Takes long time with two columns, and one column having a function
      // Map("qcs" -> "hack_license, to_date(pickup_datetime)",
      Map("qcs" -> "hack_license",
        "buckets" -> "8",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "50"),
      allowExisting = false)

    val start1 = System.currentTimeMillis
    val subQuery = snc.sql(s"Select hack_license, " +
        s" to_date(pickup_datetime) as pickup_date, " +
        s" sum(trip_distance) as daily_trips " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9 " +
        s" group by hack_license, " +
        s" to_date(pickup_datetime)" +
        s" order by daily_trips desc")
    subQuery.createOrReplaceTempView("TEMPTRIPS")
    subQuery.collect()

    val baseQuery = snc.sql(s"Select Trips.hack_license, " +
        s" sum(Earning.daily_fare_amount) as monthly_income " +
        s" from TEMPTRIPS Trips " +
        s" join " +
        s" $dailyTaxiFare Earning " +
        s" ON Trips.hack_license = Earning.hack_license " +
        s" AND Trips.pickup_date = Earning.pickup_date " +
        s" where Trips.daily_trips > 10 AND " +
        s" year(Earning.pickup_date) = 2013 and " +
        s" month(Earning.pickup_date) = 9 " +
        s" group by Trips.hack_license " +
        s" order by monthly_income desc")
    baseQuery.collect()
    val end1 = System.currentTimeMillis
    logWarning("Time taken for base: " + (end1 - start1) + "ms")
    val baseQueryCollect = baseQuery.collect()
    baseQueryCollect.foreach(row => {
      logWarning(row.getString(0) + "; " + row.getLong(1))
    })

    val start2 = System.currentTimeMillis
    val aqpSubQuery = snc.sql(s"Select hack_license, " +
        s" to_date(pickup_datetime) as pickup_date, " +
        s" sum(trip_distance) as daily_trips " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9 " +
        s" group by hack_license, " +
        s" to_date(pickup_datetime)" +
        s" order by daily_trips desc" +
        s" with error")
    aqpSubQuery.registerTempTable("AQPTEMPTRIPS")
    aqpSubQuery.collect()

    val sampleQuery = snc.sql(s"Select Trips.hack_license, " +
        s" sum(Earning.daily_fare_amount) as monthly_income " +
        s" from AQPTEMPTRIPS Trips " +
        s" join " +
        s" $dailyTaxiFare Earning " +
        s" ON Trips.hack_license = Earning.hack_license " +
        s" AND Trips.pickup_date = Earning.pickup_date " +
        s" where Trips.daily_trips > 10 AND " +
        s" year(Earning.pickup_date) = 2013 and " +
        s" month(Earning.pickup_date) = 9 " +
        s" group by Trips.hack_license " +
        s" order by monthly_income desc")
    sampleQuery.collect()
    val end2 = System.currentTimeMillis
    logWarning("Time taken for sample: " + (end2 - start2) + "ms")
    val sampleQueryCollect = sampleQuery.collect()
    sampleQueryCollect.foreach(row => {
      logWarning(row.getString(0) + "; " + row.getLong(1))
    })

    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
  }

  // NYCTaxi Zeppelin Bug
  test("AQP-233: Sample Table Query in subquery dataframe and join, Bug AQP-249") {
    testAQP233_AQP249()
  }

  def testAQP233_AQP249(): Unit = {
    val testNo = 24
    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")

    val fraction = .01
    val sampleDataFrame = snc.createSampleTable("NYCTAXI_SAMPLEHACKLICENSE",
      Some(nyc),
      // TODO - AQP-232: Takes long time with two columns, and one column having a function
      // Map("qcs" -> "hack_license, to_date(pickup_datetime)",
      Map("qcs" -> "hack_license",
        "buckets" -> "8",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "50"),
      allowExisting = false)

    val start1 = System.currentTimeMillis
    val tripsDf = snc.sql("Select hack_license as t_hack_license, " +
        s" to_date(pickup_datetime) as t_pickup_date, " +
        s" sum(trip_distance) as t_daily_trips " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9 " +
        s" group by hack_license, " +
        s" pickup_datetime")
    tripsDf.collect()
    val earningDf = snc.sql(s"Select hack_license, " +
        s" pickup_date, " +
        s" daily_fare_amount " +
        s" from $dailyTaxiFare ")
    var joinDf = tripsDf.join(earningDf,
      tripsDf("t_hack_license") === earningDf("hack_license") &&
          tripsDf("t_pickup_date") === earningDf("pickup_date"), "inner").
        filter("t_daily_trips > 10").
        filter(functions.year(new org.apache.spark.sql.Column("pickup_date")) === 2013).
        filter(functions.month(new org.apache.spark.sql.Column("pickup_date")) === 9).
        groupBy("t_hack_license").
        agg(functions.sum("daily_fare_amount").alias("monthly_income")).
        select("t_hack_license", "monthly_income").
        sort(functions.desc("monthly_income"))
    joinDf.collect()
    val end1 = System.currentTimeMillis
    logWarning("Time taken for base: " + (end1 - start1) + "ms")
    val baseQueryCollect = joinDf.collect()
    baseQueryCollect.foreach(row => {
      logWarning(row.getString(0) + "; " + row.getLong(1))
    })

    val start2 = System.currentTimeMillis
    // To use withError, import org.apache.spark.sql.snappy._ for implicit
    val tripsDf2 = snc.sql("Select hack_license as t_hack_license, " +
        s" to_date(pickup_datetime) as t_pickup_date, " +
        s" sum(trip_distance) as t_daily_trips " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9 " +
        s" group by hack_license, " +
        s" pickup_datetime").withError(.5, .5)
    tripsDf2.collect()
    val earningDf2 = snc.sql(s"Select hack_license, " +
        s" pickup_date, " +
        s" daily_fare_amount " +
        s" from $dailyTaxiFare ")
    val joinDf2 = tripsDf2.join(earningDf2,
      tripsDf2("t_hack_license") === earningDf2("hack_license") &&
          tripsDf2("t_pickup_date") === earningDf2("pickup_date"), "inner").
        filter("t_daily_trips > 10").
        filter(functions.year(new org.apache.spark.sql.Column("pickup_date")) === 2013).
        filter(functions.month(new org.apache.spark.sql.Column("pickup_date")) === 9).
        groupBy("t_hack_license").
        agg(functions.sum("daily_fare_amount").alias("monthly_income")).
        select("t_hack_license", "monthly_income").
        sort(functions.desc("monthly_income"))
    joinDf2.collect()
    val end2 = System.currentTimeMillis
    logWarning("Time taken for sample: " + (end2 - start2) + "ms")
    val sampleQueryCollect = joinDf2.collect()
    sampleQueryCollect.foreach(row => {
      logWarning(row.getString(0) + "; " + row.getLong(1))
    })


    snc.sql(s"CREATE SAMPLE TABLE DAILYTAXIFARE_SAMPLEHACKLICENSE on $dailyTaxiFare " +
      s" options (buckets '8',  qcs 'hack_license, year(pickup_date), month(pickup_date)', " +
      s"fraction '0.01', strataReservoirSize '50') AS (SELECT * FROM $dailyTaxiFare)")

    // Test bug AQP-249
    val rs1 = snc.sql(s"Select Trips.hack_license, sum(Earning.daily_fare_amount) " +
      s"as monthly_income,  relative_error(monthly_income), lower_bound(monthly_income) from " +
      s"(Select hack_license,  to_date(pickup_datetime) as pickup_date, sum(trip_distance) as" +
      s" daily_trips from $nyc where year(pickup_datetime) = 2013 and month(pickup_datetime) = 9 " +
      s"group by hack_license, to_date(pickup_datetime)) Trips join $dailyTaxiFare Earning ON " +
      s"Trips.hack_license = Earning.hack_license  AND Trips.pickup_date = Earning.pickup_date " +
      s"where Trips.daily_trips > 10 AND year(Earning.pickup_date) = 2013 and " +
      s"month(Earning.pickup_date) = 9 group by Trips.hack_license " +
      s"order by monthly_income desc with error 0.1 behavior 'do_nothing'")

    rs1.collect()
    AssertAQPAnalysis.bootStrapAnalysis(snc)


    val rs2 = snc.sql(s"Select Trips.hack_license, " +
      s" sum(Earning.daily_fare_amount) as monthly_income, " +
        s" relative_error(monthly_income), lower_bound(monthly_income) " +
      s" from (Select hack_license, " +
        s" to_date(pickup_datetime) as pickup_date, sum(trip_distance) as daily_trips," +
      s" absolute_error(daily_trips) from $nyc where" +
        s" year(pickup_datetime) = 2013 and month(pickup_datetime) = 9 " +
      s" group by hack_license," +
        s" to_date(pickup_datetime) with error) Trips " +
      s" join $dailyTaxiFare Earning ON Trips.hack_license = Earning.hack_license " +
        s" AND Trips.pickup_date = Earning.pickup_date where Trips.daily_trips > 10" +
        s" AND year(Earning.pickup_date) = 2013 and month(Earning.pickup_date) = 9" +
        s" group by Trips.hack_license order by monthly_income desc" +
      s"  with error 0.1 behavior 'do_nothing'")

    rs2.collect()
    AssertAQPAnalysis.bootStrapAnalysis(snc)
    this.assertAnalysis(1)

    val rs3 = snc.sql(s"Select Trips.hack_license, Trips.trips, Earning.earn from " +
        s" (select hack_license, count(trip_distance) as trips,  absolute_error(trips) from $nyc " +
      s" where year(pickup_datetime) = 2013" +
        s" and month(pickup_datetime) = 9 group by hack_license" +
      s" order by trips desc limit 10  with error) Trips join" +
        s" (select hack_license, sum(daily_fare_amount) as earn,  absolute_error(earn) " +
      s" from $dailyTaxiFare where year(pickup_date) = 2013 and" +
        s" month(pickup_date) = 9 group by hack_license " +
      s"order by earn desc limit 10 with error) Earning" +
        s" ON Trips.hack_license = Earning.hack_license order by earn desc ")

    rs3.collect()
    this.assertAnalysis(0)
    this.assertAnalysis(1)


    var rs4 = snc.sql(s"Select Trips.hack_license," +
      s" sum(Earning.fare_amount) as monthly_income,  absolute_error(monthly_income) " +
      s" from $nyc Trips join" +
        s" $taxiFare Earning ON Trips.hack_license = Earning.hack_license AND" +
        s" Trips.pickup_datetime = Earning.pickup_datetime where Trips.trip_distance < 5 AND" +
        s" year(Trips.pickup_datetime) = 2013 and month(Trips.pickup_datetime) = 9 " +
      s"group by Trips.hack_license order by monthly_income desc " +
      s"with error 0.5 behavior 'do_nothing'")

    rs4.collect()
    AssertAQPAnalysis.bootStrapAnalysis(snc)

    var rs5 = snc.sql(s"Select Trips.hack_license, " +
      s"sum(trips.trip_distance) as monthly_income, absolute_error(monthly_income)" +
      s" from $nyc Trips " +
        s" where Trips.trip_distance > 0 AND" +
        s" year(Trips.pickup_datetime) = 2013 " +
      s"and month(Trips.pickup_datetime) = 9 group by Trips.hack_license" +
        s" order by monthly_income desc with error 0.5 behavior 'do_nothing'")

    rs5.collect()
    this.assertAnalysis(0)

    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
    snc.sql("drop table if exists DAILYTAXIFARE_SAMPLEHACKLICENSE")
  }

  // NYCTaxi Zeppelin Bug
  test("Bug AQP231: Simplified test for data mismatch") {
    this.testAQP231_2()
  }

  def testAQP231_2(): Unit = {
    val testNo = 25

    val fraction = .01
    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
    snc.sql("drop table if exists DAILYTAXIFARE_SAMPLEHACKLICENSE")
    val sampleDataFrame = snc.createSampleTable("NYCTAXI_SAMPLEHACKLICENSE",
      Some(nyc),
      // TODO - AQP-232: Takes long time with two columns, and one column having a function
      // Map("qcs" -> "hack_license, to_date(pickup_datetime)",
      Map("qcs" -> "hack_license",
        "buckets" -> "8",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "50"),
      allowExisting = false)

    val subQuery = snc.sql(s"Select hack_license, " +
        s" sum(trip_distance) as daily_trips " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9 " +
        s" group by hack_license " +
        s" order by daily_trips desc")
    logWarning("base logical " + subQuery.qe.logical)
    logWarning("base analyzed " + subQuery.qe.analyzed)
    logWarning("base optimized " + subQuery.qe.optimizedPlan)
    logWarning("base spark " + subQuery.qe.sparkPlan)
    val subQueryCollect = subQuery.collect()
    logWarning("subQueryCollect - start")
    subQueryCollect.foreach(row => {
      logWarning(row.getString(0) + "; " + row.getDouble(1))
    })
    logWarning("subQueryCollect - end")

    val aqpSubQuery = snc.sql(s"Select hack_license, " +
        s" sum(trip_distance) as daily_trips " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9 " +
        s" group by hack_license " +
        s" order by daily_trips desc" +
        s" with error")
    logWarning("sample logical " + aqpSubQuery.qe.logical)
    logWarning("sample analyzed " + aqpSubQuery.qe.analyzed)
    logWarning("sample optimized " + aqpSubQuery.qe.optimizedPlan)
    logWarning("sample spark " + aqpSubQuery.qe.sparkPlan)
    val aqpSubQueryCollect = aqpSubQuery.collect()
    logWarning("aqpSubQueryCollect - start")
    aqpSubQueryCollect.foreach(row => {
      logWarning(row.getString(0) + "; " + row.getDouble(1))
    })
    logWarning("aqpSubQueryCollect - end")

    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")

  }

  // NYCTaxi Zeppelin Bug
  test("Bug AQP225: Join of two subquery") {
    this.testAQP225()
  }
  def testAQP225(): Unit = {
    val testNo = 26

    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")

    val fraction = .01
    val sampleDataFrame = snc.createSampleTable("NYCTAXI_SAMPLEHACKLICENSE",
      Some(nyc),
      // TODO - AQP-232: Takes long time with two columns, and one column having a function
      // Map("qcs" -> "hack_license, to_date(pickup_datetime)",
      Map("qcs" -> "hack_license",
        "buckets" -> "8",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "50"),
      allowExisting = false)

    val sumQuery = snc.sql(s"Select Trips.hack_license, " +
        s" Trips.trips, " +
        s" Earning.earn from " +
        s" (" +
        s" select hack_license, " +
        s" count(trip_distance) as trips " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9 " +
        s" group by hack_license " +
        s" order by trips desc " +
        s" limit 100" +
        s" ) Trips " +
        s" join " +
        s" (" +
        s" select hack_license," +
        s" sum(fare_amount) as earn " +
        s" from $taxiFare " +
        s" where year(pickup_datetime) = 2013" +
        s" and month(pickup_datetime) = 9" +
        s" group by hack_license" +
        s" order by earn desc" +
        s" limit 100" +
        s" ) Earning " +
        s" ON Trips.hack_license = Earning.hack_license" +
        s" order by earn desc")
    val sumQueryCollect = sumQuery.collect()
    logWarning("sumQueryCollect - start")
    sumQueryCollect.foreach(row => {
      logWarning(row.getString(0) + "; " + row.getLong(1) + "; " + row.getDouble(2))
    })
    logWarning("sumQueryCollect - end")

    val avgQuery = snc.sql(s"Select Trips.hack_license, " +
        s" Trips.trips, " +
        s" Earning.earn from " +
        s" (" +
        s" select hack_license, " +
        s" count(trip_distance) as trips " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9 " +
        s" group by hack_license " +
        s" order by trips desc " +
        s" limit 100" +
        s" ) Trips " +
        s" join " +
        s" (" +
        s" select hack_license," +
        s" avg(fare_amount) as earn " +
        s" from $taxiFare " +
        s" where year(pickup_datetime) = 2013" +
        s" and month(pickup_datetime) = 9" +
        s" group by hack_license" +
        s" order by earn desc" +
        s" limit 100" +
        s" ) Earning " +
        s" ON Trips.hack_license = Earning.hack_license" +
        s" order by earn desc")
    val avgQueryCollect = avgQuery.collect()
    logWarning("avgQueryCollect - start")
    avgQueryCollect.foreach(row => {
      logWarning(row.getString(0) + "; " + row.getLong(1) + "; " + row.getLong(2))
    })
    logWarning("avgQueryCollect - end")

    val aqpSumQuery = snc.sql(s"Select Trips.hack_license, " +
        s" Trips.trips, " +
        s" Earning.earn from " +
        s" (" +
        s" select hack_license, " +
        s" count(trip_distance) as trips " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9 " +
        s" group by hack_license " +
        s" order by trips desc " +
        s" limit 100" +
        s" ) Trips " +
        s" join " +
        s" (" +
        s" select hack_license," +
        s" sum(fare_amount) as earn " +
        s" from $taxiFare " +
        s" where year(pickup_datetime) = 2013" +
        s" and month(pickup_datetime) = 9" +
        s" group by hack_license" +
        s" order by earn desc" +
        s" limit 100" +
        s" ) Earning " +
        s" ON Trips.hack_license = Earning.hack_license" +
        s" order by earn desc" +
        s" with error")
    val aqpSumQueryCollect = aqpSumQuery.collect()
    logWarning("aqpSumQueryCollect - start")
    aqpSumQueryCollect.foreach(row => {
      logWarning(row.getString(0) + "; " + row.getLong(1) + "; " + row.getDouble(2))
    })
    logWarning("aqpSumQueryCollect - end")

    val aqpAvgQuery = snc.sql(s"Select Trips.hack_license, " +
        s" Trips.trips, " +
        s" Earning.earn from " +
        s" (" +
        s" select hack_license, " +
        s" count(trip_distance) as trips " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" and month(pickup_datetime) = 9 " +
        s" group by hack_license " +
        s" order by trips desc " +
        s" limit 100" +
        s" ) Trips " +
        s" join " +
        s" (" +
        s" select hack_license," +
        s" avg(fare_amount) as earn " +
        s" from $taxiFare " +
        s" where year(pickup_datetime) = 2013" +
        s" and month(pickup_datetime) = 9" +
        s" group by hack_license" +
        s" order by earn desc" +
        s" limit 100" +
        s" ) Earning " +
        s" ON Trips.hack_license = Earning.hack_license" +
        s" order by earn desc" +
        s" with error")
    val aqpAvgQueryCollect = aqpAvgQuery.collect()
    logWarning("aqpAvgQueryCollect - start")
    aqpAvgQueryCollect.foreach(row => {
      logWarning(row.getString(0) + "; " + row.getLong(1) + "; " + row.getDouble(2))
    })
    logWarning("aqpAvgQueryCollect - end")

    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")

  }

  test("Test repeat aggregates") {

    def assertResults[T](rows: Array[Row]): Unit = {
      rows.foreach(row => {
        assert(row.getAs[T](1) === row.getAs[T](2))
        assert(row.getAs[T](2) === row.getAs[T](3))
      })
    }
    val rs1 = snc.sql(s"SELECT uniqueCarrier, avg(ArrDelay) as avg1, avg(ArrDelay) as avg2 ," +
      s" avg(ArrDelay) as avg3 , absolute_error(avg1) FROM $sampleAirlineUniqueCarrier where " +
      s"MonthI < 13" +
      s" group by uniqueCarrier  behavior 'do_nothing'")
    assertResults[Double](rs1.collect())

    val rs2 = snc.sql(s"SELECT uniqueCarrier, sum(ArrDelay) as sum1, sum(ArrDelay) as sum2 ," +
      s" sum(ArrDelay) as sum3 , absolute_error(sum1) FROM $sampleAirlineUniqueCarrier where " +
      s"MonthI < 13" +
      s" group by uniqueCarrier  behavior 'do_nothing'")
    assertResults[Long](rs2.collect())

    val rs3 = snc.sql(s"SELECT uniqueCarrier, count(*) as count1, count(*) as count2 ," +
      s" count(*) as count3 , absolute_error(count1) FROM $sampleAirlineUniqueCarrier where " +
      s"MonthI < 13" +
      s" group by uniqueCarrier  behavior 'do_nothing'")
    assertResults[Long](rs3.collect())

    val rs4 = snc.sql(s"SELECT uniqueCarrier, avg(ArrDelay) as avg1, avg(ArrDelay) as avg2 ," +
      s" avg(ArrDelay) as avg3, absolute_error(avg1) FROM $sampleAirlineUniqueCarrier where " +
      s" MonthI < 2" +
      s" group by uniqueCarrier " +
      s" having avg(ArrDelay) > 5 behavior 'do_nothing'")

    assertResults[Double](rs4.collect())
    rs4.collect.foreach(row => assert(row.getDouble(2) > 5) )

  }

  test("Bug AQP-130 order by on error functions not supported") {

    val results1 = snc.sql("Select uniqueCarrier, sum(ArrDelay) as sum , absolute_error(sum) from" +
      " airline group by  uniqueCarrier order by absolute_error(sum) with error  ").collect()
    for(i <- 0 until results1.length - 1) {
      val row1 = results1(i)
      val row2 = results1(i + 1)
      assert(row1.getDouble(2) <= row2.getDouble(2))
    }

    val results2 = snc.sql("Select uniqueCarrier, avg(ArrDelay) as avgg , relative_error(avgg) " +
      "from airline group by  uniqueCarrier order by relative_error(avgg) with error  ").collect()
    for(i <- 0 until results2.length - 1) {
      val row1 = results2(i)
      val row2 = results2(i + 1)
      assert(row1.getDouble(2) <= row2.getDouble(2))
    }

    val results3 = snc.sql("Select uniqueCarrier, avg(ArrDelay) as avgg , relative_error(avgg) " +
      " from airline group by  uniqueCarrier order by relative_error(avgg) with error  ") .collect()
    for(i <- 0 until results3.length - 1) {
      val row1 = results3(i)
      val row2 = results3(i + 1)
      assert(row1.getDouble(2) <= row2.getDouble(2))
    }


    val results4 = snc.sql("Select uniqueCarrier, avg(ArrDelay) as avgg ,relative_error(avgg) err" +
      " from airline group by  uniqueCarrier order by err with error ") .collect()
    for(i <- 0 until results4.length - 1) {
      val row1 = results4(i)
      val row2 = results4(i + 1)
      assert(row1.getDouble(2) <= row2.getDouble(2))
    }

    val results5 = snc.sql("Select uniqueCarrier, avg(ArrDelay) as avgg ,relative_error(avgg) err" +
      " from airline group by  uniqueCarrier order by relative_error(avgg) with error ") .collect()
    for(i <- 0 until results5.length - 1) {
      val row1 = results5(i)
      val row2 = results5(i + 1)
      assert(row1.getDouble(2) <= row2.getDouble(2))
    }

  }

  test("Bug AQP223: CASE Statement with different HAC behavior") {
    val testNo = 27
    val fraction = .01
    val sampleDataFrame = snc.createSampleTable("NYCTAXI_SAMPLEHACKLICENSE",
      Some(nyc),
      // TODO - AQP-232: Takes long time with two columns, and one column having a function
      // Map("qcs" -> "hack_license, to_date(pickup_datetime)",
      Map("qcs" -> "hack_license",
        "buckets" -> "8",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "50"),
      allowExisting = false)

    val query1 = snc.sql(s"select count(*) AS numOfRides," +
        s" CASE " +
        s" WHEN month(pickup_datetime) = 1 THEN 'Jan' " +
        s" WHEN month(pickup_datetime) = 2 THEN 'Feb' " +
        s" WHEN month(pickup_datetime) = 3 THEN 'Mar' " +
        s" WHEN month(pickup_datetime) = 4 THEN 'Apr' " +
        s" WHEN month(pickup_datetime) = 5 THEN 'May' " +
        s" WHEN month(pickup_datetime) = 6 THEN 'Jun' " +
        s" WHEN month(pickup_datetime) = 7 THEN 'Jul' " +
        s" WHEN month(pickup_datetime) = 8 THEN 'Aug' " +
        s" WHEN month(pickup_datetime) = 9 THEN 'Sep' " +
        s" WHEN month(pickup_datetime) = 10 THEN 'Oct' " +
        s" WHEN month(pickup_datetime) = 11 THEN 'Nov' " +
        s" WHEN month(pickup_datetime) = 12 THEN 'Dec' " +
        s" END AS mnt " +
        // s" month(pickup_datetime) " +
        s" from $nyc " +
        s" group by month(pickup_datetime) " +
        s" order by month(pickup_datetime) " +
        s" with error 0.1 " +
        s" behavior 'partial_run_on_base_table'" +
        // s" behavior 'run_on_full_table'" +
        s" ")
    query1.collect()
  }

  // TODO The test is disabled till the AQP-271 branch of spark is merged
  ignore("Bug AQP-271 repeat aggregates being evaluated multiple times") {

    val results1 = snc.sql("select uniqueCarrier, avg(ArrDelay) from airline group by " +
      " uniqueCarrier having avg(ArrDelay) > 0 ")
    assert(results1.queryExecution.executedPlan.find {
      case _: SnappyHashAggregateExec => true
      case _ => false
    }.map(plan => plan.asInstanceOf[SnappyHashAggregateExec].aggregateExpressions.size == 1 )
      .getOrElse(false))

    val results2 = snc.sql("select uniqueCarrier, avg(ArrDelay) as agg, absolute_error(agg) from " +
      "airline group by  uniqueCarrier having avg(ArrDelay) > 0 with error")

    val expectedNumAggs = AssertAQPAnalysis.getAnalysisType(snc).map(x => x match {
      case AnalysisType.Bootstrap => 2
      case AnalysisType.Closedform => 1
      case _ => -1
    }).getOrElse(-1)

    val y = results2.queryExecution.sparkPlan.find {
      case _: SnappyHashAggregateExec => true
      case _: SortAggregateExec => true
      case _ => false
    }

    assert(results2.queryExecution.sparkPlan.find {
      case _: SnappyHashAggregateExec => true
      case _: SortAggregateExec => true
      case _ => false
    }.map(plan => plan.asInstanceOf[SnappyHashAggregateExec].aggregateExpressions.size ==
      expectedNumAggs )
      .getOrElse(false))

  }


  // NYCTaxi Zeppelin Bug
  test("Bug AQP217: Having clause with partial routing") {
    this.testAQP217()
  }

  def testAQP217(): Unit = {
    val testNo = 28
    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
    val fraction = .01
    val sampleDataFrame = snc.createSampleTable("NYCTAXI_SAMPLEHACKLICENSE",
      Some(nyc),
      // TODO - AQP-232: Takes long time with two columns, and one column having a function
      // Map("qcs" -> "hack_license, to_date(pickup_datetime)",
      Map("qcs" -> "hack_license",
        "buckets" -> "8",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "50"),
      allowExisting = false)


    val sumSubQueryDN = snc.sql(s"select passenger_count," +
        s" sum(trip_distance) as sumTripDist," +
        s" absolute_error(sumTripDist)," +
        s" relative_error(sumTripDist)," +
        s" lower_bound(sumTripDist)," +
        s" upper_bound(sumTripDist) " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        // s" and month(pickup_datetime) = 2 " +
        //  s" and day(pickup_datetime) = 1 " +
        s" group by passenger_count " +
        s" HAVING sum(trip_distance) > 70000 " +
        s" order by sumTripDist desc " +
        s" with error 0.005 " +
        s" behavior 'do_nothing'" +
        s" ")
    sumSubQueryDN.collect()

    val sumSubQueryPR = snc.sql(s"select passenger_count," +
        s" sum(trip_distance) as sumTripDist," +
        s" absolute_error(sumTripDist)," +
        s" relative_error(sumTripDist)," +
        s" lower_bound(sumTripDist)," +
        s" upper_bound(sumTripDist) " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        // s" and month(pickup_datetime) = 2 " +
        //  s" and day(pickup_datetime) = 1 " +
        s" group by passenger_count " +
        s" HAVING sum(trip_distance) > 70000 " +
        s" order by sumTripDist desc " +
        s" with error 0.005 " +
        s" behavior 'partial_run_on_base_table'" +
        s" ")
    var count1 = 0
    sumSubQueryPR.collect().foreach(ir => {
      count1 = count1 + 1
    })
    assert(count1 > 4)

    val avgSubQueryDN = snc.sql(s"select passenger_count," +
        s" avg(trip_distance) as avgTripDist," +
        s" absolute_error(avgTripDist)," +
        s" relative_error(avgTripDist)," +
        s" lower_bound(avgTripDist)," +
        s" upper_bound(avgTripDist) " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" group by passenger_count " +
        s" HAVING avg(trip_distance) > 2 " +
        s" order by avgTripDist desc " +
        s" with error 0.01 " +
        s" behavior 'do_nothing'" +
        s" ")
    avgSubQueryDN.collect()

    val avgSubQueryPR = snc.sql(s"select passenger_count," +
        s" avg(trip_distance) as avgTripDist," +
        s" absolute_error(avgTripDist)," +
        s" relative_error(avgTripDist)," +
        s" lower_bound(avgTripDist)," +
        s" upper_bound(avgTripDist) " +
        s" from $nyc " +
        s" where year(pickup_datetime) = 2013 " +
        s" group by passenger_count " +
        s" HAVING avg(trip_distance) > 2 " +
        s" order by avgTripDist desc " +
        s" with error 0.01 " +
        s" behavior 'partial_run_on_base_table'" +
        s" ")
    avgSubQueryPR.collect()

    val avgSubQuery2dn = snc.sql(s"select medallion," +
        s" avg(trip_distance) as avgTripDist," +
        s" absolute_error(avgTripDist)," +
        s" relative_error(avgTripDist)," +
        s" lower_bound(avgTripDist)," +
        s" upper_bound(avgTripDist) " +
        s" from $nyc " +
        s" group by medallion " +
        s" HAVING avg(trip_distance) < 300 " +
        s" order by avgTripDist desc " +
        s" with error 0.2 " +
        s" behavior 'do_nothing'" +
        s" ")
    avgSubQuery2dn.collect()

    val avgSubQuery2pr = snc.sql(s"select medallion," +
        s" avg(trip_distance) as avgTripDist," +
        s" absolute_error(avgTripDist)," +
        s" relative_error(avgTripDist)," +
        s" lower_bound(avgTripDist)," +
        s" upper_bound(avgTripDist) " +
        s" from $nyc " +
        s" group by medallion " +
        s" HAVING avg(trip_distance) < 300 " +
        s" order by avgTripDist desc " +
        s" with error 0.2 " +
        s" behavior 'partial_run_on_base_table'" +
        s" ")
    avgSubQuery2pr.collect()

    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")

  }

  test("Sum , Count, Average returned from sample table is correct") {

    val fraction = .08
    val sampleDataFrame = snc.createSampleTable("nycSample",
      Some(nyc), Map("qcs" -> "vendor_id",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "70"),
      allowExisting = false)

    val result1 = snc.sql(s"select hour(pickup_datetime), avg(trip_time_in_secs) avgTripTime,  " +
      s" absolute_error(avgTripTime) from nycSample" +
      s" where pickup_latitude < 80.767588  " +
      s"and pickup_longitude > -74.001632  group by hour(pickup_datetime) with error").collect()
    AssertAQPAnalysis.bootStrapAnalysis(snc)
    val avgMap = result1.map(row => (row.getInt(0), row.getDouble(1))).toMap
    assert(avgMap.size > 1)
    val result2 = snc.sql(s"select hour(pickup_datetime), " +
      s" sum(trip_time_in_secs)  totalTripTime,  count(*) howManyTrips,  " +
      s" absolute_error(totalTripTime) from nycSample" +
      s" where pickup_latitude < 80.767588  " +
      s"and pickup_longitude > -74.001632  group by hour(pickup_datetime) with error").collect()
    val sumMap = result2.map(row => (row.getInt(0), row.getLong(1))).toMap
    val countMap = result2.map(row => (row.getInt(0), row.getLong(2))).toMap
    this.assertAnalysis()

    val manualCalcData = snc.sql(s"select hour(pickup_datetime), trip_time_in_secs, " +
      s"${org.apache.spark.sql.collection.Utils.WEIGHTAGE_COLUMN_NAME} from nycSample" +
      s" where pickup_latitude < 80.767588  " +
      s"and pickup_longitude > -74.001632 ").collect()
    val conditionedData = manualCalcData.map(row => (row.getInt(0), row.getInt(1), row.getLong(2)))
    val dummyBoundRef = BoundReference(0, LongType, false)
    val actualMap = conditionedData.groupBy[Int](tuple => tuple._1).map {
      case (key, arr) => {
        val(sum, count) = arr.foldLeft[ (Double, Double)]( (0d, 0d))((part, tuple) => {
          val dummyRow = new GenericInternalRow(Array[Any](tuple._3))
          val(weight, _, _, _) = MapColumnToWeight.detailedEval(dummyRow, dummyBoundRef, null)
          val summ = part._1 + tuple._2 * weight
          val countt = part._2 + weight
          (summ, countt)
        }
        )
        (key, (sum / count, sum.toLong, count.toLong))
      }
    }

    for( (key, (avg, sum, count)) <- actualMap) {
      assert(Math.abs(avg - avgMap.get(key).get) < 1 )
      assert(sum == sumMap.get(key).get)
      assert(count == countMap.get(key).get)
    }
    snc.sql("drop table if exists nycsample")

  }


  test("Bug-A transformation obtained on sampled relation should not behave further as " +
      "sampled relation") {
    val df = snc.sql(s"SELECT * from $sampleAirlineUniqueCarrier")
    val trainingTableSchema = StructType(df.schema.dropRight(1)).add(StructField("IsDelayed",
      BooleanType, false))

    val newdf = df.map(row => {
      val arrDelayFieldIndex = row.schema.fieldIndex("arrdelay")
      val isDelayed = !row.isNullAt(arrDelayFieldIndex) && row.getInt(arrDelayFieldIndex) > 0
      Row.merge(Row(row.toSeq.dropRight(1): _*), Row(isDelayed))
    }
    )(RowEncoder(trainingTableSchema))

    newdf.createOrReplaceTempView("tempTable1")
    val res = snc.sql(s"select Origin, Dest,  count(*) from tempTable1 group by Origin, Dest")
    res.collect()

    val df1 = snc.sql(s"SELECT origin, count(*) as cnt from $sampleAirlineUniqueCarrier " +
        s"group by origin")
    val results = df1.collect().map(row => (row.getString(0), row.getLong(1))).toMap

    df1.createOrReplaceTempView("tempTable2")
    val res1 = snc.sql(s"select * from tempTable2 ")
    val map1 = res1.collect().map(row => (row.getString(0), row.getLong(1))).toMap
    map1.foreach {
      case (key, value) => assert(value === results.get(key).get)
    }
  }

  test("Bug- A query on temp table with sample table created, should use sample table") {
    val df = snc.sql(s"select * from $airlineTable")
    val temp = "airlinetemp"
    df.createOrReplaceTempView(temp)
    // Now create a sample table on this view
    snc.sql(s" CREATE SAMPLE TABLE sample_airlinetemp ON $temp  OPTIONS (qcs 'UniqueCarrier', " +
        s"fraction '.01', persistent 'sync') AS (SELECT * FROM $temp)")

    val res = snc.sql(s"select count(*) as x, absolute_error(x) from $temp group by" +
        s" uniquecarrier with error")
    this.assertAnalysis()
    val options = Map[String, Any]("qcs" -> "UniqueCarrier, Origin", "fraction" -> 0.1,
      "strataReservoirSize" -> 50)

    val sampleDataFrame: SampleDataFrame = airlineDF.stratifiedSample(options)
    sampleDataFrame.registerTempTable("tempSample")
    val sampledDataFrame: Dataset[Row] = snc.table("tempSample")

    sampleDataFrame.collect()
    assert(sampledDataFrame.count > 0)
  }

  test("AQP282: Replacement of main table with sample table") {

    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
    val taxiTable = "NYCTAXI_1"
    val taxiFaree = "NYCTAXI_FARE_1"
    // Since data is big, use empty tables (only few rows)
    val hfile: String = getClass.getResource("/NYC_trip_ParquetEmptyData").getPath
    val hfileFare: String = getClass.getResource("/trip_fare_ParquetEmptyData").getPath
    // nycDF.registerTempTable("NYCTAXI")

    snc.sql(s"CREATE EXTERNAL TABLE $taxiTable " +
        s" USING parquet " +
        s" OPTIONS(path '$hfile')" +
        s" ")

    snc.sql(s"CREATE EXTERNAL TABLE $taxiFaree " +
        s" USING parquet " +
        s" OPTIONS(path '$hfileFare')" +
        s" ")


    val fraction = .01
    val sampleDataFrame = snc.createSampleTable("NYCTAXI_SAMPLEHACKLICENSE",
      Some(taxiTable),
      Map("qcs" -> "hour(pickup_datetime)",
        // "buckets" -> "8",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "50"),
      allowExisting = false)


    val sampleQuery1 = snc.sql(s"select sum(passenger_count) sum_, " +
        s"absolute_error(sum_) from $taxiTable with error ")
    this.assertAnalysis()

    snc.sql(s"CREATE SAMPLE TABLE TAXIFARE_SAMPLEHACKLICENSE on $taxiFaree " +
        s"options  (buckets '8', qcs 'hack_license, " +
        s"year(pickup_datetime), " +
        s"month(pickup_datetime)'," +
        s" fraction '0.01', strataReservoirSize '50') AS (SELECT * FROM $taxiFaree)")
    val sampleQuery2 = snc.sql(s"select taxi.medallion, taxi.hack_license, " +
        s"sum(rate_code) as sum_rc ," +
        s" relative_error(sum_rc) from $taxiTable taxi,  $taxiFaree tfare   where " +
        s"taxi.medallion = tfare.medallion group by   " +
        s"taxi.medallion, taxi.hack_license with error ")

    AssertAQPAnalysis.bootStrapAnalysis(snc)

    val sampleQuery3 = snc.sql(s"select taxi.medallion, taxi.hack_license, " +
        s"sum(rate_code) as sum_rc ," +
        s" relative_error(sum_rc) from $taxiTable taxi   where " +
        s"( select sum(fare_amount) as sumfare from $taxiFaree tfare where " +
        s"taxi.medallion = tfare.medallion ) < 36  group by   " +
        s"taxi.medallion, taxi.hack_license with error ")

    this.assertAnalysis()

    snc.sql("drop table if exists TAXIFARE_SAMPLEHACKLICENSE")
    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
    snc.sql(s"drop table if exists $taxiTable ")
    snc.sql(s"drop table if exists $taxiFaree ")
  }

  test("SNAP-3236.No plan for SubqueryAlias. Assert fail - 1") {
    try {
      snc.dropTable("NYCTAXI_SAMPLEHACKLICENSE", true)
      snc.sql(s"CREATE SAMPLE TABLE NYCTAXI_SAMPLEHACKLICENSE on $nyc " +
        s"options  (buckets '8', qcs 'hack_license, " +
        s"year(pickup_datetime), " +
        s"month(pickup_datetime)'," +
        s" fraction '0.01', strataReservoirSize '50') ")
      val buffer = scala.collection.mutable.ArrayBuffer[Array[AQPInfo]]()
      AQPRules.setTestHookStoreAQPInfo(new AQPInfoStoreTestHook {
        override def callbackBeforeClearing(aqpInfo: Array[AQPInfo]): Unit = {
          buffer.append(aqpInfo)
        }
      })
      val x = snc.table(nyc).groupBy("hack_license", "pickup_datetime").
        agg(Map("trip_distance" -> "sum")).alias("total_tips").
        // filter(month(col("pickup_datetime")).equalTo(9)).
        withError(0.01d, behavior = Constant.BEHAVIOR_PARTIAL_RUN_ON_BASE_TABLE).
        sort(col("sum(trip_distance)").desc).
        limit(10)

      x.show
      assertAqpInfo(buffer)
    } finally {
      snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
      AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    }
  }


  test("SNAP-3236.No plan for SubqueryAlias. Assert fail - 2") {
    try {
      snc.dropTable("NYCTAXI_SAMPLEHACKLICENSE", true)
      snc.sql(s"CREATE SAMPLE TABLE NYCTAXI_SAMPLEHACKLICENSE on $nyc " +
        s"options  (buckets '8', qcs 'hack_license, " +
        s"year(pickup_datetime), " +
        s"month(pickup_datetime)'," +
        s" fraction '0.01', strataReservoirSize '50') ")
      val buffer = scala.collection.mutable.ArrayBuffer[Array[AQPInfo]]()
      AQPRules.setTestHookStoreAQPInfo(new AQPInfoStoreTestHook {
        override def callbackBeforeClearing(aqpInfo: Array[AQPInfo]): Unit = {
          buffer.append(aqpInfo)
        }
      })
      val x = snc.table(nyc).groupBy("hack_license", "pickup_datetime").
        agg(Map("trip_distance" -> "sum")).alias("total_tips").
        withError(0.01d, behavior = Constant.BEHAVIOR_RUN_ON_FULL_TABLE).
        sort(col("sum(trip_distance)").desc).
        limit(10)

      x.show
      assertAqpInfo(buffer)
    } finally {
      snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
      AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    }
  }

  def assertAqpInfo(buffer: scala.collection.mutable.ArrayBuffer[Array[AQPInfo]])

  ignore("AQP-283. Wrong aggregate function being converted") {

    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
    val taxiTable = "NYCTAXI_1"
    val taxiFaree = "NYCTAXI_FARE_1"
    // Since data is big, use empty tables (only few rows)
    val hfile: String = getClass.getResource("/NYC_trip_ParquetEmptyData").getPath
    val hfileFare: String = getClass.getResource("/trip_fare_ParquetEmptyData").getPath
    // nycDF.registerTempTable("NYCTAXI")

    snc.sql(s"CREATE EXTERNAL TABLE $taxiTable " +
        s" USING parquet " +
        s" OPTIONS(path '$hfile')" +
        s" ")

    snc.sql(s"CREATE EXTERNAL TABLE $taxiFaree " +
        s" USING parquet " +
        s" OPTIONS(path '$hfileFare')" +
        s" ")


    val fraction = .01
    val sampleDataFrame = snc.createSampleTable("NYCTAXI_SAMPLEHACKLICENSE",
      Some(taxiTable),
      Map("qcs" -> "hour(pickup_datetime)",
        // "buckets" -> "8",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "50"),
      allowExisting = false)


    snc.sql(s"CREATE SAMPLE TABLE TAXIFARE_SAMPLEHACKLICENSE on $taxiFaree " +
        s"options  (buckets '8', qcs 'hack_license, " +
        s"year(pickup_datetime), " +
        s"month(pickup_datetime)'," +
        s" fraction '0.01', strataReservoirSize '50') AS (SELECT * FROM $taxiFaree)")

    val sampleQuery2 = snc.sql(s"select taxi.medallion, taxi.hack_license, " +
        s"sum(fare_amount) / sum(trip_time_in_secs) " +
        s" from $taxiTable taxi,  $taxiFaree tfare   where " +
        s"taxi.medallion = tfare.medallion group by   " +
        s"taxi.medallion, taxi.hack_license with error ")

    snc.sql("drop table if exists TAXIFARE_SAMPLEHACKLICENSE")
    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
    snc.sql(s"drop table if exists $taxiTable ")
    snc.sql(s"drop table if exists $taxiFaree ")
  }

  // NYCTaxi Zeppelin Bug
  ignore("AQP251: Timing for sample count") {
    val testNo = 29
    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")

    // Since data is big, use empty tables (only few rows)
    val hfile: String = getClass.getResource("/NYC_trip_ParquetEmptyData").getPath

    // nycDF.registerTempTable("NYCTAXI")

    snc.sql(s"CREATE EXTERNAL TABLE NYCTAXI " +
        s" USING parquet " +
        s" OPTIONS(path '$hfile')" +
        s" ")

    val fraction = .01
    val sampleDataFrame = snc.createSampleTable("NYCTAXI_SAMPLEHACKLICENSE",
      Some("NYCTAXI"),
      Map("qcs" -> "hour(pickup_datetime)",
        // "buckets" -> "8",
        "fraction" -> s"$fraction", "strataReservoirSize" -> "50"),
      allowExisting = false)

//    snc.sql(s"CREATE SAMPLE TABLE NYCTAXI_SAMPLEHACKLICENSE " +
//        s" ON NYCTAXI " +
//        s" OPTIONS (qcs 'hour(pickup_datetime)'" +
//        s" ,fraction '0.01' " +
//        // s" ,persistent 'sync'" +
//        s") AS " +
//        s" (SELECT * FROM NYCTAXI)")

    logInfo(snc.sparkSession.sql("SET -v").collect().mkString("\n"))

    val start1 = System.currentTimeMillis
    val baseQuery = snc.sql(s"select count(*) count " +
        s" from NYCTAXI ")
    val mid1 = System.currentTimeMillis
    baseQuery.collect()
    val end1 = System.currentTimeMillis
    logInfo("Time taken for base: " + (end1 - start1) + "ms")
    logInfo("Time taken for base - mid: " + (mid1 - start1) + "ms")
    logWarning("Time taken for base: " + (end1 - start1) + "ms")
    logWarning("Time taken for base - mid: " + (mid1 - start1) + "ms")
    logWarning("baseQuery logical " + baseQuery.qe.logical)
    logWarning("baseQuery analyzed " + baseQuery.qe.analyzed)
    logWarning("baseQuery optimized " + baseQuery.qe.optimizedPlan)
    logWarning("baseQuery spark " + baseQuery.qe.sparkPlan)

    val start3 = System.currentTimeMillis
    val sampleQueryDirect = snc.sql(s"select count(*) count " +
        s" from NYCTAXI_SAMPLEHACKLICENSE ")
    val mid3 = System.currentTimeMillis
    sampleQueryDirect.collect()
    val end3 = System.currentTimeMillis
    logInfo("Time taken for sample Direct: " + (end3 - start3) + "ms")
    logInfo("Time taken for sample Direct - mid: " + (mid3 - start3) + "ms")
    logWarning("Time taken for sample Direct: " + (end3 - start3) + "ms")
    logWarning("Time taken for sample Direct - mid: " + (mid3 - start3) + "ms")
    logWarning("sampleQueryDirect logical " + sampleQueryDirect.qe.logical)
    logWarning("sampleQueryDirect analyzed " + sampleQueryDirect.qe.analyzed)
    logWarning("sampleQueryDirect optimized " + sampleQueryDirect.qe.optimizedPlan)
    logWarning("sampleQueryDirect spark " + sampleQueryDirect.qe.sparkPlan)

    val start2 = System.currentTimeMillis
    val sampleQuery = snc.sql(s"select count(*) count " +
        s" from NYCTAXI " +
        s" with error ")
    val mid2 = System.currentTimeMillis
    sampleQuery.collect()
    val end2 = System.currentTimeMillis
    logInfo("Time taken for sample: " + (end2 - start2) + "ms")
    logInfo("Time taken for sample - mid: " + (mid2 - start2) + "ms")
    logWarning("Time taken for sample: " + (end2 - start2) + "ms")
    logWarning("Time taken for sample - mid: " + (mid2 - start2) + "ms")
    logWarning("sampleQuery logical " + sampleQuery.qe.logical)
    logWarning("sampleQuery analyzed " + sampleQuery.qe.analyzed)
    logWarning("sampleQuery optimized " + sampleQuery.qe.optimizedPlan)
    logWarning("sampleQuery spark " + sampleQuery.qe.sparkPlan)

    val start12 = System.currentTimeMillis
    val sampleQuery12 = snc.sql(s"select count(*) count " +
        s" from NYCTAXI ")
    val mid12 = System.currentTimeMillis
    sampleQuery12.collect()
    val end12 = System.currentTimeMillis
    logInfo("Time taken for base: " + (end12 - start12) + "ms")
    logInfo("Time taken for base - mid: " + (mid12 - start12) + "ms")

    val start32 = System.currentTimeMillis
    val sampleQueryDirect32 = snc.sql(s"select count(*) count " +
        s" from NYCTAXI_SAMPLEHACKLICENSE ")
    val mid32 = System.currentTimeMillis
    sampleQueryDirect32.collect()
    val end32 = System.currentTimeMillis
    logInfo("Time taken for sample Direct: " + (end32 - start32) + "ms")
    logInfo("Time taken for sample Direct - mid: " + (mid32 - start32) + "ms")

    val start22 = System.currentTimeMillis
    val sampleQuery22 = snc.sql(s"select count(*) count " +
        s" from NYCTAXI " +
        s" with error ")
    val mid22 = System.currentTimeMillis
    sampleQuery22.collect()
    val end22 = System.currentTimeMillis
    logInfo("Time taken for sample: " + (end22 - start22) + "ms")
    logInfo("Time taken for sample - mid: " + (mid22 - start22) + "ms")

    snc.sql("drop table if exists NYCTAXI_SAMPLEHACKLICENSE")
    snc.sql("drop table if exists NYCTAXI ")
  }

  private def doPrint(str: String): Unit = {
    logInfo(str)
  }

  def msg(m: String): Unit = DebugUtils.msg(m)
}
