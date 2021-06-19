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

import com.gemstone.gemfire.internal.AvailablePort
import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.sql.aqp.functions._
import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.sql.functions._
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{AssertAQPAnalysis, SparkConf}


class JoinQueryTest extends SnappyFunSuite with Matchers with BeforeAndAfterAll {
  val airlineRefTable = "airlineref"
  val airlineRefRowTable = "airlinerefrow"
  val airlineMainTable: String = "airline"
  val airlineSampleTable: String = "airline_sampled"
  var airlineSampleDataFrame: DataFrame = _
  var airlineMainTableDataFrame: DataFrame = _
  var airLineCodeDataFrame: DataFrame = _
  // Set up sample & Main table
  var hfile: String = getClass.getResource("/2015.parquet").getPath
  val codetableFile = getClass.getResource("/airlineCode_Lookup.csv").getPath

  val refTablePartitionedRow = 1
  val refTableRepliactedRow = 2

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

  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    /**
     * Pls do not change the flag values of Property.TestDisableCodeGenFlag.name
     * and Property.UseOptimizedHashAggregateForSingleKey.name
     * They are meant to suppress CodegenFallback Plan so that optimized
     * byte buffer code path is tested & prevented from false passing.
     * If your test needs CodegenFallback, then override the newConf function
     * & clear the flag from the conf of the test locally.
     */
    val conf = new org.apache.spark.SparkConf()
        .setAppName("JoinQueryTest")
        .setMaster("local[6]")
        .set("spark.sql.hive.metastore.sharedPrefixes",
          "com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc," +
              "com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity," +
              "com.mapr.fs.jni,org.apache.commons")
        .set("master.ui.port", "" + AvailablePort.getRandomAvailablePort(0))
      .set(io.snappydata.Property.AqpDebugFixedSeed.name, "true")
      .set(Property.TestDisableCodeGenFlag.name , "true")
      .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")

    addSpecificProps(conf)
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  protected def addSpecificProps(conf: SparkConf): Unit = {

    conf.setAppName("JoinQueryTest")
        .set(io.snappydata.Property.NumBootStrapTrials.name, "200")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator",
          "org.apache.spark.sql.execution.serializer.SnappyKryoRegistrator")
  }


  protected def initAirlineTables(): Unit = {
    snc.sql(s"drop table  if exists $airlineMainTable")
    snc.sql(s"drop table  if exists $airlineSampleTable")
    snc.sql(s"drop table  if exists airlinestaging")
    snc.sql(s"drop table  if exists airlinerefstaging")
    snc.sql(s"drop table  if exists $airlineRefTable")

    val stagingDF = snc.read.load(hfile)
    snc.createTable("airlinestaging", "column", stagingDF.schema,
      Map.empty[String, String])
    stagingDF.write.mode(SaveMode.Ignore).saveAsTable("airlinestaging")

    airlineMainTableDataFrame = snc.createTable(airlineMainTable, "column", stagingDF.schema,
      Map.empty[String, String])
    stagingDF.write.mode(SaveMode.Append).saveAsTable(airlineMainTable)

    airlineSampleDataFrame = snc.createSampleTable(airlineSampleTable,
      Some(airlineMainTable), Map("qcs" -> "UniqueCarrier,YearI,MonthI",
        "fraction" -> "0.06", "strataReservoirSize" -> "1000"),
      allowExisting = false)
    airlineMainTableDataFrame.write.insertInto(airlineSampleTable)

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


  test("Join query with one sample table & another ref table which is " +
      "nonlocal should use bootstrap for all types of " +
      "aggregate queries with or without condition") {
    var result = snc.sql(s"SELECT avg(arrDelay) as x, absolute_error(x), relative_error(x) FROM " +
      s"$airlineMainTable, $airlineRefTable where  code = UniqueCarrier " +
      s" with error .9 confidence .95")
    result.collect

    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x, absolute_error(x) FROM $airlineMainTable Join " +
      s" $airlineRefTable  On  code = UniqueCarrier with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay)  as x,  absolute_error(x)  FROM $airlineMainTable x " +
      s" Join $airlineRefTable y On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x)  FROM $airlineMainTable x," +
      s" $airlineRefTable y where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) FROM" +
      s" $airlineMainTable, $airlineRefTable where  code = UniqueCarrier  with error .9 " +
      s" confidence .95")
    result.collect

    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x)  FROM $airlineMainTable " +
      s" Join $airlineRefTable  On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x)  FROM $airlineMainTable x " +
      s" Join $airlineRefTable y On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s" FROM $airlineMainTable x, $airlineRefTable y where  y.code = x.UniqueCarrier  " +
      s"with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s" FROM $airlineMainTable, $airlineRefTable where  code = UniqueCarrier  " +
      s" with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable Join $airlineRefTable On  code = UniqueCarrier  with error .9 " +
      s"confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable x Join $airlineRefTable y On  y.code = x.UniqueCarrier " +
      s" with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable x, $airlineRefTable y  where  y.code = x.UniqueCarrier " +
      s" with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

  }

  test("Join query with one ref table & another sample table which is nonlocal should use " +
    "bootstrap for all types of aggregate queries with or without condition") {
    var result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s"  FROM $airlineRefTable, $airlineMainTable  where  code = UniqueCarrier with error .9" +
      s" confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s"FROM $airlineRefTable Join $airlineMainTable  On  code = UniqueCarrier with error .9 " +
      s"confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s"FROM $airlineRefTable y  Join $airlineMainTable x On  y.code = x.UniqueCarrier " +
      s" with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefTable y, $airlineMainTable x  where  y.code = x.UniqueCarrier  " +
      s"with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefTable, $airlineMainTable  where  code = UniqueCarrier  " +
      s"with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s"FROM $airlineRefTable Join  $airlineMainTable On  code = UniqueCarrier  " +
      s"with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s" FROM $airlineRefTable y Join $airlineMainTable x On  y.code = x.UniqueCarrier " +
      s" with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefTable y, $airlineMainTable x  where  y.code = x.UniqueCarrier " +
      s" with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s" FROM $airlineRefTable, $airlineMainTable where  code = UniqueCarrier  with error .9 " +
      s"confidence .95")
    result.collect

    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s"FROM $airlineRefTable Join $airlineMainTable  On  code = UniqueCarrier " +
      s" with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefTable y Join $airlineMainTable x   On  y.code = x.UniqueCarrier  " +
      s"with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s"FROM $airlineRefTable y, $airlineMainTable x  where  y.code = x.UniqueCarrier  " +
      s"with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

  }

  def createRefTable(tableType: Int): Unit = {
    snc.dropTable(airlineRefRowTable, true)
    if (tableType == refTablePartitionedRow) {
      snc.sql(s"CREATE TABLE $airlineRefRowTable(Code varchar(20), Description  varchar(100)) " +
          "USING row " +
          "options " +
          "(" +
          "PARTITION_BY 'Code'," +
          "BUCKETS '213')")
    } else if (tableType == refTableRepliactedRow) {
      snc.sql(s"CREATE TABLE $airlineRefRowTable(Code varchar(20), Description  varchar(100)) " +
          "USING row")
    }
    airLineCodeDataFrame.write.format("row").mode(SaveMode.Append).saveAsTable(airlineRefRowTable)
  }

  test("Join query with one sample table & another ref table which is " +
      "partitioned row table ,should use bootstrap for all types of " +
      "aggregate queries with or without condition") {

    this.createRefTable(refTablePartitionedRow)
    var result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable, $airlineRefRowTable where  code = UniqueCarrier  with error .9 " +
      s"confidence .95")
    result.collect

    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s"FROM $airlineMainTable Join $airlineRefRowTable " +
        s" On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay)  as x,  absolute_error(x), relative_error(x)" +
      s" FROM $airlineMainTable x Join $airlineRefRowTable y" +
        s" On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s" FROM $airlineMainTable x, $airlineRefRowTable y " +
      s" where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable, $airlineRefRowTable " +
        s"where  code = UniqueCarrier  with error .9 confidence .95")
    result.collect

    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s"FROM $airlineMainTable Join $airlineRefRowTable " +
        s" On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable x Join $airlineRefRowTable y" +
        s" On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable x, $airlineRefRowTable y " +
        s" where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable, $airlineRefRowTable " +
        s"where  code = UniqueCarrier  with error .9 confidence .95")
    result.collect

    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s"FROM $airlineMainTable Join $airlineRefRowTable " +
        s" On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable x Join $airlineRefRowTable y" +
        s" On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable x, $airlineRefRowTable y " +
        s" where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

  }


  test("Join query with one ref table & another sample table which is partitioned row table ," +
    "should use bootstrap for all types of aggregate queries with or without condition") {

    this.createRefTable(refTablePartitionedRow)
    var result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable,$airlineMainTable  " +
        s"where  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable Join $airlineMainTable  " +
        s" On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable y Join $airlineMainTable x " +
        s" On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable y,$airlineMainTable x  " +
        s" where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable,$airlineMainTable  " +
        s"where  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable Join $airlineMainTable  " +
        s" On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable y Join $airlineMainTable x  " +
        s" On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s"  FROM $airlineRefRowTable y, $airlineMainTable x  " +
        s" where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x)" +
      s" FROM $airlineRefRowTable, $airlineMainTable  " +
        s"where  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable Join $airlineMainTable   " +
        s" On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable y Join $airlineMainTable x  " +
        s" On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay)  as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable y , $airlineMainTable x  " +
        s" where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

  }


  test("Join query with one sample table & another ref table which is replicated row table ," +
    "should use closeform for sum aggregate else bootstrap") {

    this.createRefTable(refTableRepliactedRow)
    var result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s"FROM $airlineMainTable, $airlineRefRowTable " +
        s"where  code = UniqueCarrier  with error .9 confidence .95")
    result.collect

    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x)  " +
      s"FROM $airlineMainTable Join $airlineRefRowTable " +
        s" On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable x Join $airlineRefRowTable y" +
        s" On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable x, $airlineRefRowTable y " +
        s" where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable, $airlineRefRowTable " +
        s"where  code = UniqueCarrier  with error .9 confidence .95")
    result.collect

    AssertAQPAnalysis.closedFormAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable Join $airlineRefRowTable " +
        s" On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.closedFormAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable x Join $airlineRefRowTable y" +
        s" On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.closedFormAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable x, $airlineRefRowTable y " +
        s" where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.closedFormAnalysis(this.snc)

    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable, $airlineRefRowTable " +
        s"where  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable Join $airlineRefRowTable " +
        s" On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable x Join $airlineRefRowTable y" +
        s" On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineMainTable x, $airlineRefRowTable y " +
        s" where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
  }

  test("Join query with one ref table & another sample table which is replicated row table ," +
    "should use closeform for sum aggregate else bootstrap") {

    this.createRefTable(refTableRepliactedRow)
    var result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable, $airlineMainTable  " +
        s"where  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable Join $airlineMainTable   " +
        s" On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x)" +
      s"  FROM $airlineRefRowTable y Join $airlineMainTable x  " +
        s" On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT avg(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable y , $airlineMainTable x  " +
        s" where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable, $airlineMainTable  " +
        s"where  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.closedFormAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable  Join $airlineMainTable  " +
        s" On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.closedFormAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable y Join $airlineMainTable x  " +
        s" On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.closedFormAnalysis(this.snc)
    result = snc.sql(s"SELECT sum(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable y, $airlineMainTable x  " +
        s" where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.closedFormAnalysis(this.snc)

    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable , $airlineMainTable  " +
        s"where  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable  Join $airlineMainTable  " +
        s" On  code = UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable y Join $airlineMainTable x  " +
        s" On  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    result = snc.sql(s"SELECT count(arrDelay) as x,  absolute_error(x), relative_error(x) " +
      s" FROM $airlineRefRowTable y , $airlineMainTable x  " +
        s" where  y.code = x.UniqueCarrier  with error .9 confidence .95")
    result.collect
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

  }

  test("Join & single query using dataframe apis without datasource api") {
    val airlineDFrame = snc.read.load(hfile)

    val airlineSampleDFrame = airlineDFrame.stratifiedSample(Map(
      "qcs" -> "UniqueCarrier,YearI,MonthI", "fraction" -> 0.7,
      "strataReservoirSize" -> "50"))


    val codeTabledDFrame = snc.read
            .format("com.databricks.spark.csv") // CSV to DF package
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
        .option("maxCharsPerColumn", "4096")
        .load(codetableFile)

    var rf = airlineDFrame.join(codeTabledDFrame,
      airlineDFrame("UniqueCarrier") === codeTabledDFrame("Code")).
        groupBy(airlineDFrame("UniqueCarrier")).agg(sum("arrdelay").alias("sum_arr_delay"),
      absolute_error("sum_arr_delay")).withError(.5, .5)

    rf.collect()
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    rf = airlineDFrame.join(codeTabledDFrame,
      airlineDFrame("UniqueCarrier") === codeTabledDFrame("Code")).
        groupBy(airlineDFrame("UniqueCarrier")).agg(avg("arrdelay").alias("avg_arr_delay"),
      absolute_error("avg_arr_delay")).withError(.5, .5)

    rf.collect()
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    rf = airlineDFrame.join(codeTabledDFrame,
      airlineDFrame("UniqueCarrier") === codeTabledDFrame("Code")).
        groupBy(airlineDFrame("UniqueCarrier")).agg(count("arrdelay").alias("count_arr_delay"),
      absolute_error("count_arr_delay")).withError(.5, .5)

    rf.collect()
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    rf = airlineDFrame.groupBy(airlineDFrame("UniqueCarrier")).agg(count("arrdelay").
      alias("count_arr_delay"), absolute_error("count_arr_delay")).withError(.5, .5)

    rf.collect()
    AssertAQPAnalysis.closedFormAnalysis(this.snc)

    rf = airlineDFrame.groupBy(airlineDFrame("UniqueCarrier")).agg(sum("arrdelay").
      alias("sum_arr_delay"), absolute_error("sum_arr_delay")).withError(.5, .5)

    rf.collect()
    AssertAQPAnalysis.closedFormAnalysis(this.snc)

    rf = airlineDFrame.groupBy(airlineDFrame("UniqueCarrier")).agg(avg("arrdelay").
      alias("avg_arr_delay"), absolute_error("avg_arr_delay")).withError(.5, .5)

    rf.collect()
    AssertAQPAnalysis.closedFormAnalysis(this.snc)

    rf = airlineDFrame.filter("arrdelay > 0").groupBy(airlineDFrame("UniqueCarrier")).
      agg(avg("arrdelay").alias("avg_arr_delay"), absolute_error("avg_arr_delay")).withError(.5, .5)

    rf.collect()
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    rf = airlineDFrame.filter("arrdelay > 0").groupBy(airlineDFrame("UniqueCarrier")).
      agg(sum("arrdelay").alias("sum_arr_delay"), absolute_error("sum_arr_delay")).withError(.5, .5)

    rf.collect()
    AssertAQPAnalysis.closedFormAnalysis(this.snc)

    rf = airlineDFrame.filter("arrdelay > 0").groupBy(airlineDFrame("UniqueCarrier")).
      agg(count("arrdelay").alias("count_arr_delay"), absolute_error("count_arr_delay")).
      withError(.5, .5)

    rf.collect()
    AssertAQPAnalysis.closedFormAnalysis(this.snc)
  }

  /**
   * Check that two double values must be within given tolerance of each other (default is 0.1%).
   */
  private def compareDoubleWithTolerance(e1: Double, e2: Double,
      tolerance: Double = 0.001): Unit = {
    assert(math.abs(1 - (e1 / e2)) < tolerance)
  }

  test("test correctness of sum formula for closedform error estimate of join query") {
    this.createRefTable(refTableRepliactedRow)
    var result1 = snc.sql(s"SELECT sum(arrDelay) x, absolute_error(x), relative_error(x) " +
      s"  FROM $airlineMainTable, $airlineRefRowTable where  code = UniqueCarrier  " +
      s"with error .9 confidence .95")
    var rs1 = result1.collect()(0)
    AssertAQPAnalysis.closedFormAnalysis(this.snc)
    var result2 = snc.sql(s"SELECT sum(arrDelay) x, absolute_error(x), relative_error(x)  " +
      s" FROM $airlineMainTable with error .9 confidence .95")
    var rs2 = result2.collect()(0)
    AssertAQPAnalysis.closedFormAnalysis(this.snc)
    assert(rs1.getLong(0) === rs2.getLong(0) )
    compareDoubleWithTolerance(rs1.getDouble(1), rs2.getDouble(1))
    compareDoubleWithTolerance(rs1.getDouble(2), rs2.getDouble(2))

    result1 = snc.sql(s"SELECT sum(arrDelay) x, absolute_error(x), relative_error(x) " +
      s"  FROM $airlineMainTable, $airlineRefRowTable where  code = UniqueCarrier " +
      s"and arrDelay > 0 with error .9 confidence .95")
     rs1 = result1.collect()(0)
    AssertAQPAnalysis.closedFormAnalysis(this.snc)
    result2 = snc.sql(s"SELECT sum(arrDelay) x, absolute_error(x), relative_error(x) " +
      s"  FROM $airlineMainTable where arrDelay > 0 with error .9 confidence .95")
    rs2 = result2.collect()(0)
    AssertAQPAnalysis.closedFormAnalysis(this.snc)
    assert(rs1.getLong(0) === rs2.getLong(0) )
    compareDoubleWithTolerance(rs1.getDouble(1), rs2.getDouble(1))
    compareDoubleWithTolerance(rs1.getDouble(2), rs2.getDouble(2))
  }


  test("test correctness of  bootstrap error estimate for join query joining with " +
    "replicated table") {
    this.createRefTable(refTableRepliactedRow)
    var result1 = snc.sql(s"SELECT avg(arrDelay) x, absolute_error(x), relative_error(x)  " +
      s" FROM $airlineMainTable, $airlineRefRowTable where  code = UniqueCarrier and arrDelay > 0" +
      s"  with error .9 confidence .85")
    var rs1 = result1.collect()(0)
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    var result2 = snc.sql(s"SELECT avg(arrDelay) x, absolute_error(x), relative_error(x) " +
      s"  FROM $airlineMainTable where  arrDelay > 0 with error .9 confidence .85")
    var rs2 = result2.collect()(0)
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)

    compareDoubleWithTolerance(rs1.getDouble(0), rs2.getDouble(0))
    compareDoubleWithTolerance(rs1.getDouble(1), rs2.getDouble(1))
    compareDoubleWithTolerance(rs1.getDouble(2), rs2.getDouble(2))
  }

  test("Bug AQP-207") {
    this.createRefTable(refTablePartitionedRow)
    var joinRes = snc.sql(s"Select sum(arrDelay) as totalDelay,absolute_error(totalDelay)," +
        s" UniqueCarrier as uc, flightNum as flNu from $airlineMainTable," +
        s" $airlineRefRowTable " +
        s" where code = UniqueCarrier and arrDelay > 0 " +
        s"  group by UniqueCarrier ,flightNum  having sum(arrDelay) > 0 with error")
    joinRes.collect().foreach {
      row => row.getLong(0)
    }
    AssertAQPAnalysis.bootStrapAnalysis(snc)
  }

  ignore("test correctness of  bootstrap error estimate for join query joining with " +
    "partitioned table ") {
    this.createRefTable(refTablePartitionedRow)
    var result1 = snc.sql(s"SELECT avg(arrDelay) x, absolute_error(x), relative_error(x) " +
      s"  FROM $airlineRefRowTable, $airlineMainTable where  code = UniqueCarrier " +
      s" and arrDelay > 0  with error .9 confidence .85")
    var rs1 = result1.collect()(0)
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
    var result2 = snc.sql(s"SELECT avg(arrDelay) x, absolute_error(x), relative_error(x) " +
      s"  FROM $airlineMainTable where  arrDelay > 0 with error .9 confidence .85")
    var rs2 = result2.collect()(0)
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)


    assert(rs1.getDouble(0) === rs2.getDouble(0) )
    assert(rs1.getDouble(1) === rs2.getDouble(1) )
    assert(rs1.getDouble(2) === rs2.getDouble(2) )
  }




  def msg(m: String): Unit = DebugUtils.msg(m)

}
