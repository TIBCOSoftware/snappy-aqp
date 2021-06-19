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

import java.sql.DriverManager

import scala.util.Random

import com.gemstone.gemfire.internal.AvailablePort
import com.pivotal.gemfirexd.TestUtil
import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.junit.Assert.assertEquals
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{AssertAQPAnalysis, SparkConf}

abstract class AbstractSampleTableInsert extends SnappyFunSuite
  with Matchers
  with BeforeAndAfterAll {

  val DEFAULT_ERROR: Double = Constants.DEFAULT_ERROR

  val lineTable: String = getClass.getSimpleName + "_" + "line"
  var lineDf: DataFrame = _
  val sampleTable1: String = getClass.getSimpleName + "_" + "line_sampled_quantity"
  val sampleTable2: String = getClass.getSimpleName + "_" + "line_sampled_order"
  val LINEITEM_DATA_FILE: String = getClass.getResource("/datafile.tbl").getPath
  val LINEITEM_DATA_FILE_1: String = getClass.getResource("/datafile-1.tbl").getPath
  val tableType: String

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    this.dropSampleTables()
    initTestTables()
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
  }

  override def afterAll(): Unit = {
    val snc = this.snc
    // cleanup metastore
    this.dropSampleTables()
    if (snc != null) {
      snc.clearCache()
    }
    super.afterAll()
    stopAll()
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "false")
  }

  protected def addSpecificProps(conf: SparkConf): Unit = {
    conf.setAppName("ClosedFormErrorEstimateFunctionTest")
      .set(io.snappydata.Property.ClosedFormEstimates.name, "true")
  }

  def assertAnalysis(): Unit = {
    AssertAQPAnalysis.closedFormAnalysis(this.snc)
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
      .setAppName("SampleTableInsertTest")
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


  def initTestTables(): Unit = {
    val snc = this.snc
    this.createLineitemTable()
  }

  def initSampleTables(): Unit = {
    val snc = this.snc
    snc.sql(s"create sample table $sampleTable1 on $lineTable options(qcs 'l_quantity'," +
      s" fraction '0.01', strataReservoirSize '10') as (select * from $lineTable) ")

    snc.sql(s"create sample table $sampleTable2 on $lineTable options(qcs 'l_orderkey'," +
      s" fraction '0.0001', strataReservoirSize '50', buckets '1') as (select * from $lineTable) ")
  }

  def dropSampleTables(): Unit = {
    snc.sql(s"drop table if exists $sampleTable1 ")
    snc.sql(s"drop table if exists $sampleTable2 ")
  }


  def createLineitemTable(populate: Boolean = true): Unit = {
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
    snc.sql("DROP TABLE IF EXISTS " + lineTable)

    this.lineDf = snc.createTable(this.lineTable, tableType,
      schema, Map.empty[String, String], false)
    if (populate) {
      populateLineTable()
    }
  }

  def populateLineTable(): Unit = {
    val people = snc.sparkContext.textFile(LINEITEM_DATA_FILE)
      .map(_.split('|')).map(p => Row(p(0).trim.toInt, p(1).trim.toInt,
      p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toFloat, p(5).trim.toFloat,
      p(6).trim.toFloat, p(7).trim.toFloat,
      p(8).trim, p(9).trim, java.sql.Date.valueOf(p(10).trim),
      java.sql.Date.valueOf(p(11).trim), java.sql.Date.valueOf(p(12).trim),
      p(13).trim, p(14).trim, p(15).trim, p(16).trim.toInt))

    logInfo("total rows in table =" + people.collect().length)
    val dfx = snc.createDataFrame(people, lineDf.schema)
    dfx.write.format(tableType).mode(SaveMode.Append).saveAsTable(lineTable)
  }

  private def assertInsertion(): Unit = {
    var rsBase1 = snc.sql(s"select l_orderkey, count(*) countt " +
      s"from $lineTable group by l_orderkey order by countt").collect()
    var rAqp1 = snc.sql(s"select l_orderkey, count(*) countt, absolute_error(countt) " +
      s"from $lineTable group by l_orderkey order by countt with error").collect()
    assertAnalysis()
    assertEquals(rsBase1.length, rAqp1.length)
    rsBase1.zip(rAqp1).foreach {
      case (r1, r2) => assertEquals(r1.getLong(1), r2.getLong(1))
    }

    var rsBase2 = snc.sql(s"select l_quantity, count(*) countt " +
      s"from $lineTable group by l_quantity order by countt").collect()
    var rAqp2 = snc.sql(s"select l_quantity, count(*) countt, absolute_error(countt) " +
      s"from $lineTable group by l_quantity order by countt with error").collect()
    assertAnalysis()
    assertEquals(rsBase2.length, rAqp2.length)
    rsBase2.zip(rAqp2).foreach {
      case (r1, r2) => assertEquals(r1.getLong(1), r2.getLong(1))
    }

  }

  test("jdbc batch insert on base table reflects in aqp count") {
    this.initSampleTables()
    this.assertInsertion()

    // Now do a batch insert in the main table using data frame
    val part1 = snc.sparkContext.textFile(LINEITEM_DATA_FILE_1)
      .map(_.split('|')).map(p => Row(p(0).trim.toInt, p(1).trim.toInt,
      p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toFloat, p(5).trim.toFloat,
      p(6).trim.toFloat, p(7).trim.toFloat,
      p(8).trim, p(9).trim, java.sql.Date.valueOf(p(10).trim),
      java.sql.Date.valueOf(p(11).trim), java.sql.Date.valueOf(p(12).trim),
      p(13).trim, p(14).trim, p(15).trim, p(16).trim.toInt))

    val df1 = snc.createDataFrame(part1, lineDf.schema)

    var serverHostPort2 = TestUtil.startNetServer()
    var conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    val ps = conn.prepareStatement(s"insert into $lineTable values" +
      s" (?,?,?, ?,?,?, ?,?,?, ?,?,?, ?,?,?,?,?)")
    df1.collect().foreach(row => {
      ps.setInt(1, row.getInt(0))
      ps.setInt(2, row.getInt(1))
      ps.setInt(3, row.getInt(2))
      ps.setInt(4, row.getInt(3))
      ps.setFloat(5, row.getFloat(4))
      ps.setFloat(6, row.getFloat(5))
      ps.setFloat(7, row.getFloat(6))
      ps.setFloat(8, row.getFloat(7))
      ps.setString(9, row.getString(8))
      ps.setString(10, row.getString(9))
      ps.setDate(11, row.getDate(10))
      ps.setDate(12, row.getDate(11))
      ps.setDate(13, row.getDate(12))
      ps.setString(14, row.getString(13))
      ps.setString(15, row.getString(14))
      ps.setString(16, row.getString(15))
      ps.setInt(17, row.getInt(16))
      ps.addBatch()
    })
    ps.executeBatch()
    this.assertInsertion()
    this.dropSampleTables()
    conn.close()
  }

  test("insert into val select syntax on base table reflects in aqp count") {
    this.initSampleTables()
    this.assertInsertion()
    // Now do a batch insert in the main table using data frame
    val part1 = snc.sparkContext.textFile(LINEITEM_DATA_FILE_1)
      .map(_.split('|')).map(p => Row(p(0).trim.toInt, p(1).trim.toInt,
      p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toFloat, p(5).trim.toFloat,
      p(6).trim.toFloat, p(7).trim.toFloat,
      p(8).trim, p(9).trim, java.sql.Date.valueOf(p(10).trim),
      java.sql.Date.valueOf(p(11).trim), java.sql.Date.valueOf(p(12).trim),
      p(13).trim, p(14).trim, p(15).trim, p(16).trim.toInt))

    val df1 = snc.createDataFrame(part1, lineDf.schema)
    df1.write.format(tableType).mode(SaveMode.Append).saveAsTable("temp")
    snc.sql(s"insert into $lineTable select * from temp ")

    // re execute base & aqp query
    this.assertInsertion()
    snc.dropTable("temp", true)
    this.dropSampleTables()
  }

  test("dataframe write to base table reflects in aqp count") {
    this.initSampleTables()
    this.assertInsertion()

    // Now do a batch insert in the main table using data frame
    val part1 = snc.sparkContext.textFile(LINEITEM_DATA_FILE_1)
      .map(_.split('|')).map(p => Row(p(0).trim.toInt, p(1).trim.toInt,
      p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toFloat, p(5).trim.toFloat,
      p(6).trim.toFloat, p(7).trim.toFloat,
      p(8).trim, p(9).trim, java.sql.Date.valueOf(p(10).trim),
      java.sql.Date.valueOf(p(11).trim), java.sql.Date.valueOf(p(12).trim),
      p(13).trim, p(14).trim, p(15).trim, p(16).trim.toInt))

    val df1 = snc.createDataFrame(part1, lineDf.schema)
    df1.write.format(tableType).mode(SaveMode.Append).saveAsTable(lineTable)

    this.assertInsertion()
    this.dropSampleTables()
  }

  test("dataframe write to base table reflects in aqp count - 1") {
    snc.dropTable(lineTable, true)
    this.createLineitemTable(false)
    this.initSampleTables()
    this.assertInsertion()
    this.populateLineTable()
    this.assertInsertion()
    assert(snc.sql(s"select count(*) from $lineTable").collect()(0).getLong(0) > 0)
    this.dropSampleTables()
  }

}
