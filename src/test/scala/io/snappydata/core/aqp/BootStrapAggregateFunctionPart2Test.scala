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
import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow}
import org.apache.spark.sql.execution.bootstrap.PoissonCreator.PoissonType
import org.apache.spark.sql.execution.bootstrap._
import org.apache.spark.sql.execution.closedform.ErrorAggregate
import org.apache.spark.sql.execution.common._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

 class BootStrapAggregateFunctionPart2Test
  extends SnappyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  val DEFAULT_ERROR = Constants.DEFAULT_ERROR

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
  val codetableFile = getClass.getResource("/airlineCode_Lookup.csv").getPath
  val numBootStrapTrials = 100
  // Set up sample & Main table
  val LINEITEM_DATA_FILE = getClass.getResource("/datafile.tbl").getPath

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
      .setAppName("ErrorEstimationFunctionTest")
      .setMaster("local[6]")
      .set("spark.sql.hive.metastore.sharedPrefixes",
        "com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc," +
          "com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity," +
          "com.mapr.fs.jni,org.apache.commons")
      .set("master.ui.port", "" + AvailablePort.getRandomAvailablePort(0))
      .set(Property.TestDisableCodeGenFlag.name , "true")
      .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")

    conf.set(Property.ClosedFormEstimates.name, "false")
    conf.set(Property.NumBootStrapTrials.name, numBootStrapTrials.toString)
    conf.set(Property.AqpDebug.name, "true")
    conf.set(Property.AqpDebugFixedSeed.name, "true")

    val random = new Random(731)
    var maxCodeGen: Int = 0
    while(maxCodeGen == 0) {
      maxCodeGen = random.nextInt(500)
    }

    conf.set("spark.sql.codegen.maxFields", maxCodeGen.toString)
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }



  protected def initTestTables(): Unit = {
    val snc = this.snc

    Property.AQPDebugPoissonType.set(snc.sessionState.conf, PoissonCreator.PoissonType.
      DbgIndpPredictable.toString)
    createLineitemTable(snc, lineTable)
    val mainTableDF = createLineitemTable(snc, mainTable)
    snc.createSampleTable(sampleTable, Some(mainTable),
      Map(
        "qcs" -> "l_quantity",
        "fraction" -> "0.01",
        "strataReservoirSize" -> "50"),
      allowExisting = false)

    mainTableDF.write.insertInto(sampleTable)
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
    airlineMainTableDataFrame.write.insertInto(airlineSampleTable)

    val s2 = snc.createSampleTable(airline_sampleTable2,
      Some(airlineMainTable), Map("qcs" -> "round(tan(ActualElapsedTime))",
        "fraction" -> "0.5", "strataReservoirSize" -> "1000"),
      allowExisting = false)
    airlineMainTableDataFrame.write.insertInto(airline_sampleTable2)

    val codeTabledf = snc.read
      .format("com.databricks.spark.csv") // CSV to DF package
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data type
      .option("maxCharsPerColumn", "4096")
      .load(codetableFile)

    airLineCodeDataFrame = snc.createTable(airlineRefTable, "column",
      codeTabledf.schema, Map.empty[String, String])
    codeTabledf.write.mode(SaveMode.Append).saveAsTable(airlineRefTable)
  }


   test("Sample Table Query with a given confidence on sum aggregate " +
     "should use correct quantiles") {
     val confidence = .9
     val result = snc.sql(s"SELECT sum(l_quantity) as T FROM $mainTable " +
       s"with error $DEFAULT_ERROR confidence " + confidence)

     val rows = result.collect()
     assert(rows(0).getStruct(0).length === numBootStrapTrials)

     var i = 0
     val arrayOfBS = Array.tabulate[Double](numBootStrapTrials)({
       rows(0).getStruct(0).getDouble(_)
     }
     )
     val estimate = arrayOfBS(0)
     val sortedData = arrayOfBS.sortWith(_.compareTo(_) < 0)
     val lowerBound = sortedData(4)
     val upperBound = sortedData(94)


     i = 0
     val arrayOfBSAny = new DoubleMutableRow(
       Array.tabulate[Double](numBootStrapTrials)(i => rows(0).getStruct(0).getDouble(i)),
       numBootStrapTrials)


     i = 0
     val columnExpr = BoundReference(i, BootstrapStructType(numBootStrapTrials, DoubleType,
       BootstrapStructType.trialField), nullable = false)
     i = i + 1
     val multiplicities = BoundReference(i, BootstrapStructType(100 / 7 + 1, ByteType,
       BootstrapStructType.multiplicityField), nullable = false)

     val arrayOfBytesAny = new ByteMutableRow(Array.fill[Byte](100 / 7 + 1)(127.asInstanceOf[Byte]))


     val approxColumn = ApproxColumn(columnExpr,
       multiplicities, confidence, ErrorAggregate.Sum, .9, HAC.getDefaultBehavior())
     val internalRow = new GenericInternalRow(Array[Any](arrayOfBSAny, arrayOfBytesAny))

     val evalRow = approxColumn.eval(internalRow).asInstanceOf[InternalRow]
     assert(estimate == evalRow.getDouble(0))
     assert(lowerBound == evalRow.getDouble(1))
     assert(upperBound == evalRow.getDouble(2))


   }


   test("Test Bootstrap columns generation & correctness of values for sum") {
     val confidence = .90
     val result = snc.sql(s"SELECT sum(l_quantity) as T FROM $mainTable " +
       s"with error $DEFAULT_ERROR confidence " + confidence)

     val rows = result.collect()
     assert(rows(0).getStruct(0).schema.length === numBootStrapTrials)

     val baseResult = snc.sql(
       s"SELECT l_quantity as T FROM $sampleTable ")
     val baseRows = baseResult.collect()
     val predictablePoisson = PoissonCreator.apply(PoissonType
       .DbgIndpPredictable, -1, null).asInstanceOf[DebugIndpndntPredictable]
     val poissonizedRows = predictablePoisson.applyMultiplicity(baseRows, numBootStrapTrials)
     val summedRow = Sum.applySum(poissonizedRows)
     assert(summedRow === rows(0).getStruct(0))
   }

   test("Test Bootstrap columns generation & correctness of values for avg") {
     val confidence = .9

     val result = snc.sql(s"SELECT avg(l_quantity) as T FROM $mainTable " +
       s"with error $DEFAULT_ERROR confidence " + confidence)

     val rows = result.collect()
     assert(rows(0).getStruct(0).schema.length === numBootStrapTrials)

     val baseResult = snc.sql(
       s"SELECT l_quantity as T FROM $mainTable ")
     val baseRows = baseResult.collect()
     val predictablePoisson = PoissonCreator.apply(PoissonType
       .DbgIndpPredictable, -1, null).asInstanceOf[DebugIndpndntPredictable]
     val poissonizedRows = predictablePoisson.applyMultiplicity(baseRows,
       numBootStrapTrials)
     val avgRow = Avg.applyAvg(poissonizedRows, predictablePoisson
       .getMultiplicity(numBootStrapTrials))
     assert(avgRow.schema.length == rows(0).getStruct(0).schema.length)
     0 until avgRow.schema.length foreach { i =>
       assert(Math.abs(avgRow.getFloat(i) - rows(0).getStruct(0).getDouble(i)) < .1)
     }
   }

   test("Test Bootstrap columns generation & correctness of values for count") {
     val confidence = .9
     val result = snc.sql(s"SELECT count(l_quantity) as T FROM $mainTable " +
       s"with error $DEFAULT_ERROR confidence " + confidence)

     val rows = result.collect()
     assert(rows(0).getStruct(0).schema.length === numBootStrapTrials)

     val baseResult = snc.sql(
       s"SELECT l_quantity as T FROM $sampleTable ")
     val baseRows = baseResult.collect()
     val predictablePoisson = PoissonCreator.apply(PoissonType.DbgIndpPredictable,
       -1, null).asInstanceOf[DebugIndpndntPredictable]
     val countedRow = Count.applyCount(baseRows.length,
       predictablePoisson.getMultiplicity(numBootStrapTrials),
       result.schema)
     assert(countedRow === rows(0).getStruct(0))
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

}


