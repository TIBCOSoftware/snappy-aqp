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
package io.snappydata.app

import java.sql.DriverManager

import scala.actors.Futures._

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.Logging
import org.apache.spark.sql._

/**
  * Hack to test : column, row tables using SQL, Data source API,
  * Join Column with row, concurrency, speed
  */
class ColumnRowSamplePerfSuite
    extends SnappyFunSuite
        with BeforeAndAfterAll {

  // context creation is handled by App main
  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
  }

  override def afterAll(): Unit = {
    val session = ColumnRowSamplePerfSuite.snSession
    // cleanup metastore
    if (session != null) {
      session.catalog.clearCache()
    }
    super.afterAll()
    stopAll()
  }

  test("Some performance tests for column store with airline schema") {
    ColumnRowSamplePerfSuite.main(Array[String]())
  }
}

object ColumnRowSamplePerfSuite extends App with Logging {

  // var hfile: String = "/Users/jramnara/Downloads/2007-8.01.csv.parts"
  // Too large ... You need to download from GoogleDrive/snappyDocuments/data;
  // or, use the one below
  var hfile: String = getClass.getResource("/2015.parquet").getPath
  val codetableFile = getClass.getResource("/airlineCode_Lookup.csv").getPath
  // also available in GoogleDrive/snappyDocuments/data

  var loadData: Boolean = true
  var debug: Boolean = false
  var setJars, executorExtraClassPath: String = _
  var executorExtraJavaOptions: String = _
  var setMaster: String = "local[6]"
  val tableName: String = "airline"
  val sampleTableName: String = "airlineSampled"

  // System.setProperty("gemfirexd.debug.true", "TraceLock_DD")

  if (args.length > 0) option(args.toList)

  val conf = new org.apache.spark.SparkConf()
      .setAppName("ColumnRowSamplePerfTest")
      .set("spark.logConf", "true")
      .set("spark.scheduler.mode", "FAIR")

  if (setMaster != null) {
    conf.setMaster(setMaster)
  }

  if (setJars != null) {
    conf.setJars(Seq(setJars))
  }

  if (executorExtraClassPath != null) {
    conf.set("spark.executor.extraClassPath", executorExtraClassPath)
  }

  if (executorExtraJavaOptions != null) {
    conf.set("spark.executor.extraJavaOptions", executorExtraJavaOptions)
  }

  var start: Long = 0
  var end: Long = 0
  var results: DataFrame = _
  var resultsC: Array[Row] = _

  val sc = new org.apache.spark.SparkContext(conf)

  val snSession = SnappyContext(sc).snappySession
  val session = SparkSession.builder().getOrCreate()

  logInfo(" Started spark context = " + SnappyContext.globalSparkContext)
  snSession.sql("set spark.sql.shuffle.partitions=6")

  var airlineDataFrame: DataFrame = _

  // start network server
  val serverHostPort = TestUtil.startNetServer()
  logInfo(" Network server started.")

  createTableLoadData()

  // run several queries concurrently in threads ...
  val tasks = for (i <- 1 to 5) yield future {
    logInfo("Executing task " + i)
    Thread.sleep(1000L)
    runQueries()
  }

  // wait a lot
  awaitAll(20000000L, tasks: _*)

  def createTableLoadData(): Unit = {

    if (loadData) {
      // All these properties will default when using snappyContext in the release
      val props = Map[String, String]()
      if (hfile.endsWith(".parquet")) {
        airlineDataFrame = snSession.read.load(hfile)
        session.read.load(hfile).createOrReplaceTempView("airline_pq")
        // sqlContext.cacheTable("airline_pq")
      } else {
        // Create the Airline columnar table
        airlineDataFrame = snSession.read
            .format("com.databricks.spark.csv") // CSV to DF package
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .option("maxCharsPerColumn", "4096")
            .load(hfile)
      }

      logInfo(s"The $tableName table schema ..")
      // airlineDataFrame.schema.printTreeString()
      // airlineDataFrame.show(10) // expensive extract of all partitions?

      snSession.dropTable(tableName, ifExists = true)

      var start = System.currentTimeMillis
      // This will do the real work. Load the data.
      snSession.createTable(tableName, "column",
        airlineDataFrame.schema, props)
      airlineDataFrame.write.mode(SaveMode.Append).saveAsTable(tableName)
      var end = System.currentTimeMillis
      results = snSession.sql(s"SELECT count(*) FROM $tableName")
      results.rdd.map(t => "Count: " + t(0)).collect().foreach(msg)
      msg(s"Time to load into table $tableName and count = " +
          (end - start) + " ms")

      // Now create the airline code row replicated table
      val codeTabledf = snSession.read
          .format("com.databricks.spark.csv") // CSV to DF package
          .option("header", "true") // Use first line of all files as header
          .option("inferSchema", "true") // Automatically infer data types
          .option("maxCharsPerColumn", "4096")
          .load(codetableFile)
      logInfo("The airline code table schema ..")
      // codeTabledf.schema.printTreeString()
      codeTabledf.collect()

      snSession.dropTable("airlineCode", ifExists = true)
      codeTabledf.write.format("row").options(props).saveAsTable("airlineCode")

      // finally creates some samples
      snSession.dropTable(sampleTableName, ifExists = true)
      snSession.sql(s"create sample table $sampleTableName on $tableName " +
          "options(qcs 'UniqueCarrier,YearI,MonthI', fraction '0.03'," +
          "  strataReservoirSize '50')")

      // This will do the real work. Load the data.
      start = System.currentTimeMillis
      airlineDataFrame = snSession.table(tableName)
      end = System.currentTimeMillis
      results = snSession.sql(s"SELECT count(*) as sample_count FROM $sampleTableName")
      msg(s"Time to load into table $sampleTableName = " + (end - start) + " ms")
      results.rdd.map(t => "Count: " + t(0)).collect().foreach(msg)
    }

    // check for SNAP-253
    start = System.currentTimeMillis()
    val expectedResult = session.sql("select distinct(FlightNum) as a " +
        "from airline_pq order by a").collect().map(_.getInt(0))
    end = System.currentTimeMillis()
    logInfo("Distinct on parquet took " + (end - start) + "ms")
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    try {
      start = System.currentTimeMillis()
      val rs = stmt.executeQuery(
        s"select distinct(flightNum) as a from $tableName order by a")
      var index = 0
      while (rs.next()) {
        assert(rs.getInt(1) == expectedResult(index),
          s"Expected ${expectedResult(index)} got ${rs.getInt(1)}")
        index += 1
      }
      rs.close()
      end = System.currentTimeMillis()
      logInfo("Distinct on JDBC took " + (end - start) + "ms")
    } finally {
      stmt.close()
      conn.close()
    }
  }

  def runQueries(): Unit = {
    var start: Long = 0
    var end: Long = 0
    var results: DataFrame = null

    val session = SnappyContext(sc).snappySession

    for (_ <- 0 until 1) {

      start = System.currentTimeMillis
      results = session.sql(s"SELECT count(*) FROM $tableName")
      results.rdd.map(t => "Count: " + t(0)).collect().foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for count(*): " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = session.sql(s"SELECT count(DepTime) FROM $tableName")
      results.rdd.map(t => "Count: " + t(0)).collect().foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for count(DepTime): " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = session.sql(
        s"""SELECT AVG(ArrDelay), count(*), UniqueCarrier,
            YearI, MonthI FROM $tableName GROUP BY UniqueCarrier, YearI, MonthI""")
      results.collect()
      end = System.currentTimeMillis
      msg("Time taken for AVG+count(*) with GROUP BY: " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = session.sql(
        s"""SELECT AVG(ArrDelay), count(*), UniqueCarrier,
            YearI, MonthI FROM $sampleTableName GROUP BY UniqueCarrier, YearI, MonthI""")
      results.collect()
      end = System.currentTimeMillis
      msg("SAMPLE: Time taken for AVG+count(*) with GROUP BY: " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = session.sql(
        s"""SELECT AVG(ArrDelay), count(*), UniqueCarrier, t2.DESCRIPTION,
            YearI, MonthI FROM $tableName t1, airlineCode t2 where t1.UniqueCarrier = t2.CODE
            GROUP BY UniqueCarrier, DESCRIPTION, YearI,MonthI""")
      results.collect()
      end = System.currentTimeMillis
      msg("Time taken for AVG+count(*) with JOIN + GROUP BY: " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = session.sql(
        s"""SELECT AVG(ArrDelay), count(*), UniqueCarrier, t2.DESCRIPTION,
            YearI, MonthI FROM $sampleTableName t1, airlineCode t2 where
            t1.UniqueCarrier = t2.CODE GROUP BY UniqueCarrier, DESCRIPTION, YearI,MonthI""")
      results.collect()
      end = System.currentTimeMillis
      msg("SAMPLE: Time taken for AVG+count(*) with JOIN + GROUP BY: " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = session.sql(
        s"""SELECT AVG(ArrDelay), UniqueCarrier, YearI,
            MonthI FROM $tableName GROUP BY UniqueCarrier, YearI, MonthI ORDER BY
            UniqueCarrier, YearI, MonthI""")
      // results.explain(true)
      results.collect()
      end = System.currentTimeMillis
      msg("Time taken for AVG on GROUP BY with ORDER BY: " +
          (end - start) + "ms")

      start = System.currentTimeMillis
      results = session.sql(
        s"""SELECT AVG(ArrDelay), UniqueCarrier, YearI,
            MonthI FROM $sampleTableName GROUP BY UniqueCarrier, YearI, MonthI ORDER BY
            UniqueCarrier, YearI, MonthI""")
      // results.explain(true)
      results.collect()
      end = System.currentTimeMillis
      msg("SAMPLE: Time taken for AVG on GROUP BY with ORDER BY: " +
          (end - start) + "ms")

      Thread.sleep(1000)
    }
  }

  def msg(s: Any): Unit = {
    logInfo("Thread :: " + Thread.currentThread().getName + " :: " + s)
  }

  def option(list: List[String]): Boolean = {
    list match {
      case "-hfile" :: value :: tail =>
        hfile = value
        print(" hfile " + hfile)
        option(tail)
      case "-noload" :: tail =>
        loadData = false
        print(" loadData " + loadData)
        option(tail)
      case "-set-master" :: value :: tail =>
        setMaster = value
        print(" setMaster " + setMaster)
        option(tail)
      case "-nomaster" :: tail =>
        setMaster = null
        print(" setMaster " + setMaster)
        option(tail)
      case "-set-jars" :: value :: tail =>
        setJars = value
        print(" setJars " + setJars)
        option(tail)
      case "-executor-extraClassPath" :: value :: tail =>
        executorExtraClassPath = value
        print(" executor-extraClassPath " + executorExtraClassPath)
        option(tail)
      case "-executor-extraJavaOptions" :: value :: tail =>
        executorExtraJavaOptions = value
        print(" executor-extraJavaOptions " + executorExtraJavaOptions)
        option(tail)
      case "-debug" :: tail =>
        debug = true
        print(" debug " + debug)
        option(tail)
      case opt :: _ =>
        logInfo(" Unknown option " + opt)
        sys.exit(1)
      case Nil => true
    }
  } // end of option
}
