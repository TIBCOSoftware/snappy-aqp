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

import scala.util.Random

import com.gemstone.gemfire.internal.AvailablePort
import io.snappydata.{Property, SnappyFunSuite}
import org.junit.Assert._
import org.scalatest.{BeforeAndAfterAll, Matchers}
import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.common.AQPRules

abstract class AbstractViewTest extends SnappyFunSuite
  with BeforeAndAfterAll with Matchers {
  val DEFAULT_ERROR: Double = Constants.DEFAULT_ERROR

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


  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    this.initAirlineTables()
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
      .setAppName("AbstractViewTest")
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

  test("SNAP-3131 : query on view which is formed on underlying table does not use aqp: test1") {
    snc.sql("drop view if exists airline_view ")
    snc.sql(s"create view airline_view as (select arrtime, arrdelay, depdelay, origin, " +
      s" dest, uniquecarrier from $airlineMainTable )")
    var rs1 = snc.sql("select count(*) couunt, count(*) sample_count, absolute_error(couunt)," +
      "relative_error(couunt) from airline_view with error")
    assertAnalysis()
    var rs2 = snc.sql("select count(*) couunt, count(*) sample_count, absolute_error(couunt)," +
      "relative_error(couunt) from airline with error")
    assertAnalysis()

    var row1 = rs1.collect()(0)
    var row2 = rs2.collect()(0)
    assertEquals(row1.getLong(0), row2.getLong(0))
    assertEquals(row1.getLong(1), row2.getLong(1))
    assertEquals(row1.getDouble(2), row2.getDouble(2), 0d)
    assertEquals(row1.getDouble(3), row2.getDouble(3), 0d)

    rs1 = snc.sql("select avg(depdelay) avgg, sum(arrdelay) suum, count(*) couunt," +
      " count(*) sample_count, absolute_error(suum), relative_error(suum), absolute_error(avgg)," +
      " relative_error(avgg), absolute_error(couunt), relative_error(couunt) from " +
      " airline_view with error")
    assertAnalysis()

    rs2 = snc.sql("select avg(depdelay) avgg, sum(arrdelay) suum, count(*) couunt," +
      " count(*) sample_count, absolute_error(suum), relative_error(suum), absolute_error(avgg)," +
      " relative_error(avgg), absolute_error(couunt), relative_error(couunt) from " +
      " airline with error")
    assertAnalysis()


    row1 = rs1.collect()(0)
    row2 = rs2.collect()(0)
    assertEquals(row1.getDouble(0), row2.getDouble(0), 0d)
    assertEquals(row1.getLong(1), row2.getLong(1))
    assertEquals(row1.getLong(2), row2.getLong(2))
    assertEquals(row1.getLong(3), row2.getLong(3))
    assertEquals(row1.getDouble(4), row2.getDouble(4), 0d)
    assertEquals(row1.getDouble(5), row2.getDouble(5), 0d)
    assertEquals(row1.getDouble(6), row2.getDouble(6), 0d)
    assertEquals(row1.getDouble(7), row2.getDouble(7), 0d)
    assertEquals(row1.getDouble(8), row2.getDouble(8), 0d)
    assertEquals(row1.getDouble(9), row2.getDouble(9), 0d)
    snc.sql("drop view if exists airline_view ")
  }

  test("SNAP-3131 : query on view which is formed on underlying table does not use aqp: test2") {
    snc.sql("drop view if exists airline_view ")
    snc.sql(s"create view airline_view as (select arrtime, arrdelay, depdelay, origin, " +
      s" dest, uniquecarrier from $airlineMainTable )")


    var rs1 = snc.sql("select avg(depdelay) avgg, sum(arrdelay) suum, count(*) couunt," +
      " count(*) sample_count, absolute_error(suum), relative_error(suum), absolute_error(avgg)," +
      " relative_error(avgg), absolute_error(couunt), relative_error(couunt), uniquecarrier from " +
      " airline_view group by uniquecarrier with error")
    assertAnalysis()

    var rs2 = snc.sql("select avg(depdelay) avgg, sum(arrdelay) suum, count(*) couunt," +
      " count(*) sample_count, absolute_error(suum), relative_error(suum), absolute_error(avgg)," +
      " relative_error(avgg), absolute_error(couunt), relative_error(couunt), uniquecarrier from " +
      " airline group by uniquecarrier with error")
    assertAnalysis()


    var map1 = rs1.collect().map(row => row.getString(10) -> row).toMap
    var map2 = rs2.collect().map(row => row.getString(10) -> row).toMap
    assertEquals(map1.size, map2.size)
    for ((key, row1) <- map1) {
      val row2 = map2.get(key).get
      assertEquals(row1.getDouble(0), row2.getDouble(0), 0d)
      assertEquals(row1.getLong(1), row2.getLong(1))
      assertEquals(row1.getLong(2), row2.getLong(2))
      assertEquals(row1.getLong(3), row2.getLong(3))
      assertEquals(row1.getDouble(4), row2.getDouble(4), 0d)
      assertEquals(row1.getDouble(5), row2.getDouble(5), 0d)
      assertEquals(row1.getDouble(6), row2.getDouble(6), 0d)
      assertEquals(row1.getDouble(7), row2.getDouble(7), 0d)
      assertEquals(row1.getDouble(8), row2.getDouble(8), 0d)
      assertEquals(row1.getDouble(9), row2.getDouble(9), 0d)
    }
    snc.sql("drop view if exists airline_view ")
  }

  test("SNAP-3211") {
    snc.sql("CREATE TABLE STORE_SALES (SS_ITEM_SK INTEGER " +
      " NOT NULL,SS_SALES_PRICE DECIMAL(7,2) NOT NULL) USING column")
    snc.sql("CREATE TABLE ITEM (I_ITEM_SK INTEGER NOT NULL," +
      " I_MANUFACT_ID INTEGER NOT NULL) USING column")
    snc.sql("CREATE OR REPLACE VIEW item_details AS SELECT * " +
      " FROM (SELECT i_manufact_id, sum(ss_sales_price) sum_sales, " +
      " avg(sum(ss_sales_price)) OVER (PARTITION BY i_manufact_id) " +
      " avg_quarterly_sales  FROM item, store_sales " +
      " WHERE ss_item_sk = i_item_sk  GROUP BY i_manufact_id) tmp1 " +
      "  WHERE avg_quarterly_sales > 0")

    snc.sql("drop view if exists item_details")
    snc.sql("drop table if exists item")
    snc.sql("drop table if exists STORE_SALES")
  }

  test("SNAP-3204_1") {

    snc.sql("drop view if exists airline_view ")
    snc.sql(s"create view airline_view as (select monthi, yeari, arrtime, " +
      s"arrdelay, depdelay, origin, " +
      s" dest, uniquecarrier from $airlineMainTable where monthi < 3 )")

    val q1 = "select count(*), count(*) sample_ from  airline_view  with error"
    val rs1 = snc.sql(q1)
    rs1.show()

    val q2 = "select count(*) , count(*) sample_ from (" +
      s"select arrtime, arrdelay, depdelay, origin, dest, " +
      s"uniquecarrier from $airlineMainTable where monthi < 3) with error"
    val rs2 = snc.sql(q2)
    rs2.show()
    snc.sql("drop view if exists airline_view ")
  }

  test("SNAP-3204_2") {
    val climateDataFile: String = getClass.getResource("/climatedata.csv").getPath
    snc.sql("CREATE EXTERNAL TABLE climateChange_staging USING " +
     s" com.databricks.spark.csv OPTIONS(path '$climateDataFile'," +
      " header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096') ")
    snc.sql(s"CREATE TABLE climateChange  USING column options(buckets '32',redundancy '1')" +
      s"  AS (SELECT * FROM climateChange_staging)")

    snc.sql("CREATE SAMPLE TABLE climateChangeSampleTable ON climateChange OPTIONS(buckets '7'," +
      "redundancy '1', qcs 'element',fraction '0.01',strataReservoirSize '50')")

    snc.sql("CREATE VIEW climateChange_View AS SELECT ID AS stationId, " +
      " IF( ELEMENT='TMAX', data_value, NULL ) AS tmax, IF( ELEMENT='TMIN', data_value, NULL )" +
      " AS tmin,CAST(substr(ymd, 0, 4) AS INT) AS year FROM CLIMATECHANGE " +
      " WHERE ELEMENT IN ('TMIN','TMAX')")

    val q1 = s"select count(*) x ,count(*) sample_, absolute_error(x) from " +
      s"climateChange_View with error"
    val rs1 = snc.sql(q1).collect()
    assertAnalysis()
    val q2 = s"select count(*) ,count(*) sample_ from (SELECT " +
      s"IF( ELEMENT='TMAX', data_value, NULL ) AS tmax, " +
      s" IF( ELEMENT='TMIN', data_value, NULL ) AS tmin, " +
      s"CAST(substr(ymd, 0, 4) AS INT) AS year FROM CLIMATECHANGE " +
      s" WHERE ELEMENT IN ('TMIN','TMAX')) with error"

    val rs2 = snc.sql(q2).collect()
    AssertAQPAnalysis.bypassErrorCalc(snc)
    rs1.zip(rs2).foreach(tup => {
      assertEquals(tup._1.getLong(1), tup._2.getLong(1))
      assertTrue(Math.abs(tup._1.getLong(0) - tup._2.getLong(0)) < 2)
    })
  }


  def assertAnalysis()

}
