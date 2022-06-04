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
package io.snappydata.app

import scala.actors.Futures._

import io.snappydata.{Constant, SnappyFunSuite}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.AssertAQPAnalysis
import org.apache.spark.sql._
import org.apache.spark.sql.execution.common.AnalysisType
import org.apache.spark.sql.snappy._

/**
 * Hack to test : column, row tables using SQL, Data source API,
 * Join Column with row, concurrency, speed
 */
class AQPAccuracyTest
  extends SnappyFunSuite
  with BeforeAndAfterAll {

  // var hfile: String = "/Users/jramnara/Downloads/2007-8.01.csv.parts"
  // Too large ... You need to download from GoogleDrive/snappyDocuments/data;
  // or, use the one below
  var hfile: String = getClass.getResource("/2015.parquet").getPath
  val codetableFile: String = getClass.getResource("/airlineCode_Lookup.csv").getPath
  // also available in GoogleDrive/snappyDocuments/data

  var loadData: Boolean = true
  var debug: Boolean = false
  var setJars, executorExtraClassPath: String = _
  var executorExtraJavaOptions: String = _
  var setMaster: String = "local[6]"
  var tableName: String = "airline"
  var sampleTableName: String = "airline_sampled"
  var airlineCode = "airlineCode"
  var totalNumberOfRowsInActualTable: Long = _
  var totalNumberOfRowsInSampleTable: Long = _
  /*
    ---- Find Flights to SFO that has been delayed(Arrival/Dep) at least 5 times ----
      SELECT FlightNum, COUNT(ArrDelay) as ARR_DEL
    FROM airline
      WHERE CAST(Dest AS VARCHAR(24)) = 'SFO'
    AND ArrDelay > 0
    GROUP BY FlightNum
    HAVING COUNT(ArrDelay) >= 5;

    ---- Query to get Avg ARR_DELAY with Airline name from AIRLINE and AIRLINE_REF table.----
    SELECT AVG(ArrDelay) as AVGDELAY, count(*) as TOTALCNT, UniqueCarrier, airlineref.DESCRIPTION,
           Year_, Month_
    FROM airline, airlineref
    WHERE airline.UniqueCarrier = airlineref.CODE
    GROUP BY UniqueCarrier, DESCRIPTION, Year_,Month_;

    ---- List the flights and its details ,that are affected due to delays caused by weather ----
      SELECT FlightNum, airlineref.DESCRIPTION
    FROM airline, airlineref
    WHERE airline.UniqueCarrier = airlineref.CODE AND  Origin like '%SFO%' AND WeatherDelay > 0
    GROUP BY FlightNum ,DESCRIPTION;
    */
  val queries = Array("SELECT avg(ArrDelay) as avgArrivalDelay, " +
      "lower_bound(avgArrivalDelay), upper_bound(avgArrivalDelay), " +
      s"UniqueCarrier as carrier FROM $tableName group by UniqueCarrier " +
      "order by UniqueCarrier with error .2 confidence .95 ",

    s"SELECT  COUNT(ArrDelay) as ARR_DEL " +
      s"  FROM $tableName   " +
      s" WHERE CAST(Dest AS VARCHAR(24)) = 'SFO'  AND ArrDelay > 0    " +
      s"HAVING COUNT(ArrDelay) >= 5 with error .2 confidence .95",

    s"SELECT FlightNum, COUNT(ArrDelay) as ARR_DEL " +
      s"  FROM $tableName   " +
      s" WHERE CAST(Dest AS VARCHAR(24)) = 'SFO'  AND ArrDelay > 0  GROUP BY FlightNum  " +
      s"HAVING COUNT(ArrDelay) >= 5 with error .2 confidence .95"
  )

  val sampleTablesQueries = Array(s"SELECT  avg(ArrDelay) as avgArrivalDelay," +
    " UniqueCarrier as carrier " +
    s"  FROM $sampleTableName group by UniqueCarrier  order by UniqueCarrier ",
    s"SELECT FlightNum, COUNT(ArrDelay) as ARR_DEL   FROM $sampleTableName   " +
      s" WHERE CAST(Dest AS VARCHAR(24)) = 'SFO'  AND ArrDelay > 0  GROUP BY FlightNum  " +
      s"HAVING COUNT(ArrDelay) >= 5"

  )

  var start: Long = 0
  var end: Long = 0
  var results: DataFrame = _
  var resultsC: Array[Row] = _

  var snContext: SnappyContext = _

  var airlineDataFrame: DataFrame = _

  protected def initData(): Unit = {
    val conf = new org.apache.spark.SparkConf()
      .setAppName("AQPAccuracyTest")
      .set("spark.logConf", "true")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.master", "local[6]")
    // .set("spark.sql.unsafe.enabled", "false")
    // .set(Constants.closedFormEstimates, "false")

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

    val sc = new org.apache.spark.SparkContext(conf)

    snContext = org.apache.spark.sql.SnappyContext(sc)
    logInfo("Started spark context =" + SnappyContext.globalSparkContext)
    snContext.sql("set spark.sql.shuffle.partitions=6")

    if (loadData) {
      // All these properties will default when using snappyContext in the release
      val props = Map[String, String]()
      if (hfile.endsWith(".parquet")) {
        airlineDataFrame = snContext.read.load(hfile)
      } else {
        // Create the Airline columnar table
        airlineDataFrame = snContext.read
            .format("com.databricks.spark.csv") // CSV to DF package
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .option("maxCharsPerColumn", "4096")
            .load(hfile)
      }

      logInfo(s"The $tableName table schema: " + airlineDataFrame.schema.treeString)
      // airlineDataFrame.collect() // expensive extract of all partitions?

      snContext.dropTable(tableName, ifExists = true)

      snContext.createTable(tableName, "column",
        airlineDataFrame.schema, props)
      airlineDataFrame.write.mode(SaveMode.Append).saveAsTable(tableName)
      totalNumberOfRowsInActualTable = airlineDataFrame.count()
      // Now create the airline code row replicated table
      val codeTabledf = snContext.read
            .format("com.databricks.spark.csv") // CSV to DF package
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .option("maxCharsPerColumn", "4096")
            .load(codetableFile)
      logInfo("The airline code table schema: " + codeTabledf.schema.treeString)
      logInfo(codeTabledf.collect().mkString("\n"))

      snContext.dropTable("airlineCode", ifExists = true)
      codeTabledf.write.format("row").options(props).saveAsTable("airlineCode")
      snContext.dropTable(sampleTableName, ifExists = true)
      loadSampleTable()
      results = snContext.sql("select avg(ArrDelay) as avg_delay, " +
          "absolute_error (avg_delay), UniqueCarrier, YearI, MonthI " +
          "from AIRLINE_sampled group by UniqueCarrier, YearI, MonthI with error 0.2")
      msg("printing test results")
      logInfo(results.collect().mkString("\n"))
    }
  }

  protected def loadSampleTable(): Unit = {
    // finally creates some samples
    val samples = airlineDataFrame.stratifiedSample(Map(
      "qcs" -> "UniqueCarrier,YearI,MonthI", "fraction" -> 0.06,
      "strataReservoirSize" -> "1000"))

    samples.registerSampleTable(sampleTableName, Some(tableName))
    totalNumberOfRowsInSampleTable = samples.collect().length
  }

  // context creation is handled by App main
  override def beforeAll(): Unit = {
    System.setProperty(Constant.defaultBehaviorAsDO_NOTHING, "true")
    super.beforeAll()
    stopAll()
    this.initData()
  }

  override def afterAll(): Unit = {

    // cleanup metastore
    if (snc != null) {
      snc.clearCache()
    }
    super.afterAll()
    stopAll()
    System.setProperty(Constant.defaultBehaviorAsDO_NOTHING, "false")
  }

  test("Test count queries without group by clause fired on main table & " +
      "compare results with sample table") {
    testCountQueriesWithoutGroupBy()
  }

  test("Test count queries with group by clause fired on main table & " +
      "compare results with sample table") {
    testCountQueriesWithGroupBy()
  }

  test("BugTest SNAP-357") {
    testBugSnap357()
  }

  test("Bug SNAP-348 sample table count is a weighted count and not actual count") {
    results = snContext.sql(s"SELECT count(*) FROM $sampleTableName")
    val data = results.collect()
    assert(data.length == 1)
    val sampleCount = data(0).getLong(0)
    assert(sampleCount - totalNumberOfRowsInSampleTable > 100000)
    assert(scala.math.abs(totalNumberOfRowsInActualTable - sampleCount) * 100 /
        totalNumberOfRowsInActualTable < 15)
  }

  test("test if avg query fired twice returns same result") {
    results = snContext.sql(s"Select avg(arrdelay) from  $sampleTableName")
    val data1 = results.collect()
    assert(data1.length == 1)
    val sampleCount1 = data1(0).getDouble(0)

    results = snContext.sql(s"Select avg(arrdelay) from  $sampleTableName with error 0.2")
    val data2 = results.collect()
    assert(data2.length == 1)
    val sampleCount2 = data2(0).getDouble(0)

    assert(Math.abs(sampleCount1 - sampleCount2) < 1)

    results = snContext.sql(s"Select avg(arrdelay) from  $sampleTableName with error 0.1")
    val data3 = results.collect()
    assert(data3.length == 1)
    val sampleCount3 = data3(0).getDouble(0)

    assert(Math.abs(sampleCount3 - sampleCount2) < 2)

  }

  test("test if avg query fired on main table & sample table return same result") {
    results = snContext.sql(s"Select avg(arrdelay) from  $sampleTableName")
    val data1 = results.collect()
    assert(data1.length == 1)
    val sampleCount1 = data1(0).getDouble(0)

    results = snContext.sql(s"Select avg(arrdelay) from  $tableName with error 0.2")
    val data2 = results.collect()
    assert(data2.length == 1)
    val sampleCount2 = data2(0).getDouble(0)

    assert(Math.abs(sampleCount1 - sampleCount2) < 1)

    results = snContext.sql(s"Select avg(arrdelay) from  $tableName with error 0.1")
    val data3 = results.collect()
    assert(data3.length == 1)
    val sampleCount3 = data3(0).getDouble(0)

    assert(Math.abs(sampleCount3 - sampleCount2) < 1 )

  }

  test("check quick start queries") {
    val queries = Array(
      // TODO: AQP-206
      s"Select uniqueCarrier, sum(ArrDelay) as x , absolute_error( x ),relative_error( x ) " +
          s" FROM $tableName " +
          s" group by uniqueCarrier " +
          s" having relative_error( x ) < 0.9 " +
          s" order by uniqueCarrier desc " +
          s" with error",

      s"Select uniqueCarrier, sum(ArrDelay) as x , absolute_error( x ),relative_error( x ) " +
          s" FROM $tableName " +
          s" group by uniqueCarrier " +
          s" having relative_error( x ) < 0.9 " +
          s" order by uniqueCarrier desc ",

      s"Select uniqueCarrier, sum(ArrDelay) as x , absolute_error( x ),relative_error( x ) " +
          s" FROM $tableName " +
          s" group by uniqueCarrier " +
          s" having relative_error( x ) < 0.9 " +
          s" order by uniqueCarrier desc " +
          s" limit 10 with error 0.07 behavior 'partial_run_on_base_table'",

      s"Select uniqueCarrier, sum(ArrDelay) as x , absolute_error( x ),relative_error( x ) " +
          s" FROM $tableName " +
          s" group by uniqueCarrier " +
          s" having relative_error( x ) < 0.9 " +
          s" order by uniqueCarrier desc " +
          s" limit 10 with error 0.07 behavior 'run_on_full_table'",

      s"select  count(*) flightRecCount,  UniqueCarrier carrierCode ," +
          s"YearI from $tableName " + // , $airlineCode   where UniqueCarrier = code  " +
          s"group by UniqueCarrier, YearI  order by flightRecCount desc limit 10 with error .05",

      s"select  count(*) flightRecCount,  UniqueCarrier carrierCode ," +
          s"YearI from $sampleTableName " + // , $airlineCode   where UniqueCarrier = code  " +
          s"group by UniqueCarrier, YearI  order by flightRecCount desc limit 10 with error .05",

      s"select AVG(ArrDelay) arrivalDelay" +
          s" , lower_bound(arrivalDelay), upper_bound(arrivalDelay), " +
          s" absolute_error(arrivalDelay), relative_error(arrivalDelay), " +
          s" UniqueCarrier carrier from $tableName     " +
          s" group by UniqueCarrier   order by arrivalDelay with error .05",

      s"select AVG(ArrDelay) arrivalDelay, " +
          s"lower_bound(arrivalDelay), upper_bound(arrivalDelay), " +
          s" absolute_error(arrivalDelay), relative_error(arrivalDelay), " +
          s" UniqueCarrier carrier" +
          s"  from $tableName " + // , $airlineCode where UniqueCarrier = Code " +
          s"group by UniqueCarrier   order by arrivalDelay with error .05",

      "select AVG(ArrDelay) ArrivalDelay, YearI ,lower_bound(ArrivalDelay), " +
          "upper_bound(ArrivalDelay), absolute_error(ArrivalDelay), " +
          s"relative_error(ArrivalDelay) from $tableName " +
          s"group by YearI order by YearI with error .05",

      s"SELECT sum(WeatherDelay) totalWeatherDelay, " +
          "lower_bound(totalWeatherDelay), upper_bound(totalWeatherDelay), " +
          " absolute_error(totalWeatherDelay), relative_error(totalWeatherDelay), " +
          "UniqueCarrier carrier" + // , two.DESCRIPTION " +
          s" FROM $tableName " + // , $airlineCode as two
          "where Origin like '%SFO%' AND WeatherDelay > 0 " +
          // + "WHERE UniqueCarrier = two.CODE AND""
          "GROUP BY UniqueCarrier   limit 20 with error .05"
    )

    for (query <- queries) {
      logInfo("query=" + query)
    }

    for(query <- queries) {
      try {
        results = snContext.sql(query)
        results.collect()
      } catch {
        case e: Exception =>
          logInfo("query failing=" + query, e)
          throw e
      }
    }
  }

  test("check quick start join queries") {
    val queries = Array("select  count(*) flightRecCount, description AirlineName, " +
        "UniqueCarrier carrierCode ,YearI, absolute_error(flightRecCount) " +
        s"from $tableName, $airlineCode where UniqueCarrier = code " +
        "group by UniqueCarrier,description, YearI  order by flightRecCount " +
        "desc limit 10  with error 0.5" -> AnalysisType.Bootstrap,

      "select AVG(ArrDelay) arrivalDelay, description AirlineName, " +
          "UniqueCarrier carrier, lower_bound(arrivalDelay), upper_bound(arrivalDelay)," +
          " absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
          s" from $tableName, $airlineCode  where UniqueCarrier = Code  " +
          s" group by UniqueCarrier, description order by arrivalDelay " +
          "with error 0.5" -> AnalysisType.Bootstrap,

      s"SELECT sum(WeatherDelay) totalWeatherDelay, DESCRIPTION, " +
          s" lower_bound(totalWeatherDelay), upper_bound(totalWeatherDelay), " +
          s" absolute_error(totalWeatherDelay), relative_error(totalWeatherDelay) " +
          s"   FROM $tableName, $airlineCode " +
          s" WHERE UniqueCarrier = CODE AND  Origin like '%SFO%' AND WeatherDelay > 0" +
          s"  GROUP BY DESCRIPTION  limit 20 with error .5" -> AnalysisType.Closedform,

      s" select AVG(ArrDelay) arrivalDelay, relative_error(arrivalDelay) rel_err," +
          s"description AirlineName, UniqueCarrier carrier " +
          s"from $sampleTableName, $airlineCode " +
          s"where $sampleTableName.UniqueCarrier = $airlineCode.Code " +
          s"group by UniqueCarrier, description " +
          s" order by arrivalDelay " -> AnalysisType.Bootstrap

    )

    for (query <- queries) {
      logInfo("query=" + query._1)
    }

    for (query <- queries) {
      results = snContext.sql(query._1)
      results.collect()
      assert(query._2 == AssertAQPAnalysis.getAnalysisType(snContext).get)
    }
  }

  test("check perf numbers for count queries on base table & sample table") {
    // Test count query fired directly on main table without group by clause
    val start1 = System.currentTimeMillis()
    results = snContext.sql(s"SELECT count(*) FROM $tableName")
    results.rdd.map(t => "Count on actual table: " + t(0)).collect()
        .foreach(msg)
    var data = results.collect()
    assert(data.length == 1)
    val actualCount = data(0).getLong(0)
    val end1 = System.currentTimeMillis()
    val start2 = System.currentTimeMillis()
    results = snContext.sql(s"SELECT count(*) FROM $sampleTableName   ")
    results.rdd.map(t => "Count on sample table: " + t(0)).collect()
        .foreach(msg)
    data = results.collect()
    assert(data.length == 1)
    val sampleCount = data(0).getLong(0)
    val end2 = System.currentTimeMillis()

    msg("count query on main table: time=" + (end1 - start1) + " milliseconds")
    msg("count query on sample table: time=" + (end2 - start2) + " milliseconds")

    msg("query on actual table count=" + actualCount +
        "; query on sample table count=" + sampleCount)
    msg(" actual total number of rows in table =" + totalNumberOfRowsInActualTable)
    msg("actual total number of rows in sample table =" + totalNumberOfRowsInSampleTable)

    assert(totalNumberOfRowsInSampleTable != actualCount)
    assert(scala.math.abs(actualCount - sampleCount) * 100 / actualCount < 15)
  }

  ignore("Some performance tests for column store with airline schema") {
    // run several queries concurrently in threads ...
    val tasks = for (i <- 1 to 1) yield future {
      logInfo("Executing task " + i)
      Thread.sleep(1000L)
      runQueries()
    }

    // wait a lot
    awaitAll(20000000L, tasks: _*)
  }

  def dropAndRecreateSample(): Unit = {
    snContext.dropTable(sampleTableName, ifExists = true)
    val samples = airlineDataFrame.stratifiedSample(Map(
      "qcs" -> "UniqueCarrier,YearI,MonthI", "fraction" -> 0.03,
      "strataReservoirSize" -> "50"))

    samples.registerSampleTable(sampleTableName, Some(tableName))
  }

  def runQueries(): Unit = {
    this.runQueries(snContext)
  }


  def testCountQueriesWithoutGroupBy(): Unit = {
    // Test count query fired directly on main table without group by clause
    results = snContext.sql(s"SELECT count(*) FROM $tableName")
    results.rdd.map(t => "Count on actual table: " + t(0)).collect()
        .foreach(msg)
    var data = results.collect()
    assert(data.length == 1)
    val actualCount = data(0).getLong(0)
    results = snContext.sql(s"SELECT count(*) FROM $sampleTableName")
    results.rdd.map(t => "Count on sample table: " + t(0)).collect()
        .foreach(msg)
    data = results.collect()
    assert(data.length == 1)
    val sampleCount = data(0).getLong(0)

    msg("query on actual table count=" + actualCount +
        "; query on sample table count=" + sampleCount)
    msg(" actual total number of rows in table =" +
        totalNumberOfRowsInActualTable )
    msg("actual total number of rows in sample table =" +
        totalNumberOfRowsInSampleTable)

    assert(totalNumberOfRowsInSampleTable != actualCount)
    assert(scala.math.abs(actualCount - sampleCount) * 100 / actualCount < 15)
  }

  def testBugSnap357(): Unit = {
    results = snContext.sql(s"Select avg(ArrDelay) as x, absolute_error(x)," +
        s"UniqueCarrier from $tableName " +
        s"group by UniqueCarrier order by UniqueCarrier with error .5")
    val data1 = results.collect()
    results = snContext.sql(s"Select avg(ArrDelay) as x, absolute_error(x),UniqueCarrier " +
      s" from $sampleTableName" +
      s" group by UniqueCarrier order by UniqueCarrier with error .5 ")
    val data2 = results.collect()

    assert(data1.length == data2.length)
    for (i <- data1.indices) {
      val row1 = data1(i)
      val row2 = data2(i)
      assert(math.abs(row1.getDouble(0) - row2.getDouble(0)) < 2)
      assert(math.abs(row1.getDouble(1) - row2.getDouble(1)) < 2)
      assert(row1.getString(2) == row2.getString(2))
    }
  }

  def testCountQueriesWithGroupBy(): Unit = {
    // Test count query fired directly on main table without group by clause
    results = snContext.sql("select count(*) , UniqueCarrier  from " +
        s"$tableName group by UniqueCarrier order by UniqueCarrier")
    val data1 = results.collect()
    results = snContext.sql("select count(*) , UniqueCarrier from " +
        s"$sampleTableName group by UniqueCarrier order by UniqueCarrier")
    val data2 = results.collect()
    var atleastSomeUnequality = false
    assert(data1.length == data2.length)
    for (i <- data1.indices) {
      val row1 = data1(i)
      val row2 = data2(i)
      assert(row1.getString(1) == row2.getString(1))
      atleastSomeUnequality = row1.getLong(0) != row2.getLong(0)
      assert((scala.math.abs(row1.getLong(0) - row2.getLong(0)) * 100) /
          row1.getLong(0) < 5)
    }
    // assert(!atleastSomeUnequality)
  }

  private def runQueries(sqlContext: SQLContext): Unit = {
    var start: Long = 0
    var end: Long = 0
    var results: DataFrame = null

    start = System.currentTimeMillis
    results = sqlContext.sql("SELECT UniqueCarrier as carrier, Sum(ArrDelay)" +
        " as totalArrivalDelay, lower_bound(totalArrivalDelay) " +
        s"FROM $tableName group by UniqueCarrier confidence .95")
    results.rdd.map(t => "carrier: " + t.getString(0) + "; totalDelay = " +
        t.getDouble(1) + "; lower_bound = " + t.getDouble(2)).collect()
        .foreach(msg)
    end = System.currentTimeMillis
    msg("Time taken for query: " + (end - start) + "ms")
    val numSamples = 1
    for (j <- queries.indices) {

      start = System.currentTimeMillis
      results = sqlContext.sql(queries(j))
      val closedFormEstimates = results.collect().map(row => (row.getDouble(0),
        row.getDouble(1), row.getDouble(2)))

      val iterations = for (i <- 1 to numSamples) yield {
        dropAndRecreateSample()
        val rs = sqlContext.sql(sampleTablesQueries(j))
        rs.collect().map(row => row.getDouble(0))

      }
      // msg("count from main table = " + results.collect()(0).getLong(0))

      /*
          start = System.currentTimeMillis
          results = sqlContext.sql(s"SELECT count(*) FROM $tableName confidence 95")
          results.map(t => "Count: " + t(0)).collect().foreach(msg)
          end = System.currentTimeMillis
          msg("Time taken for count(*): " + (end - start) + "ms")
          msg("count from sample table = " + results.collect()(0).getLong(0))

            start = System.currentTimeMillis
            results = sqlContext.sql(s"SELECT count(DepTime) FROM $tableName")
            results.map(t => "Count: " + t(0)).collect().foreach(msg)
            end = System.currentTimeMillis
            msg("Time taken for count(DepTime): " + (end - start) + "ms")

            start = System.currentTimeMillis
            results = sqlContext.sql(
              s"""SELECT AVG(ArrDelay), count(*), UniqueCarrier,
                YearI, MonthI FROM $tableName GROUP BY UniqueCarrier, YearI, MonthI""")
            results.collect() //.foreach(msg)
            end = System.currentTimeMillis
            msg("Time taken for AVG+count(*) with GROUP BY: " + (end - start) + "ms")

            start = System.currentTimeMillis
            results = sqlContext.sql(
              s"""SELECT AVG(ArrDelay), count(*), UniqueCarrier, t2.DESCRIPTION,
                YearI, MonthI FROM $tableName t1, airlineCode t2 where t1.UniqueCarrier = t2.CODE
                GROUP BY UniqueCarrier, DESCRIPTION, YearI,MonthI""")
            results.collect()
            end = System.currentTimeMillis
            msg("Time taken for AVG+count(*) with JOIN + GROUP BY: " + (end - start) + "ms")

            Thread.sleep(3000)

            start = System.currentTimeMillis
            results = sqlContext.sql(
              s"""SELECT AVG(ArrDelay), UniqueCarrier, YearI,
                MonthI FROM $tableName GROUP BY UniqueCarrier, YearI, MonthI ORDER BY
                UniqueCarrier, YearI, MonthI""")
            //results.explain(true)
            results.collect() //.foreach(msg)
            end = System.currentTimeMillis
            msg("Time taken for AVG on GROUP BY with ORDER BY: " +
                (end - start) + "ms")

            Thread.sleep(3000)

            start = System.currentTimeMillis
            results = sqlContext.sql(
              s"""SELECT AVG(ArrDelay), UniqueCarrier, YearI,
                MonthI FROM $tableName GROUP BY UniqueCarrier, YearI, MonthI
                ORDER BY YearI, MonthI""")
            results.collect()
            end = System.currentTimeMillis
            msg("Time taken for worst carrier processing: " + (end - start) + "ms")
      */

    }
  }

  def msg(s: Any): Unit = logInfo(s.toString)
}
