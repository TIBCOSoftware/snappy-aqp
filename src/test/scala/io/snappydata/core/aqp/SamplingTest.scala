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

import java.sql.{DriverManager, Statement}

import scala.collection.JavaConverters._

import com.gemstone.gemfire.cache.EvictionAlgorithm
import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, PartitionedRegion}
import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.Assert._

import org.apache.spark.sql.execution.common.AQPRules

class SamplingTest
  extends SnappyFunSuite
  with BeforeAndAfterAll {

  val ddlStr = "(YearI INT," + // NOT NULL
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

  var qcsColumns : String = "UniqueCarrier"   // Same as QCS
  var qcs : String = "START"
  var currQCS : String = ""
  val strataReserviorSize = 50
  // TODO Change as needed
  val sampleRatio : Double = 0.03

  var total = new Array[Double](100)
  var cnt = new Array[Integer](100)
  var wtCnt = new Array[Double](100)
  var results_sampled_table : DataFrame = _
  var airlineDataFrame : DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    setupData()
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
      * Setting local[n] here actually supposed to affect number of reservoir created
      * while sampling.
      *
      * Change of 'n' will influence results if they are dependent on weights - derived
      * from hidden column in sample table.
     */
    /**
     * Pls do not change the flag values of Property.TestDisableCodeGenFlag.name
     * and Property.UseOptimizedHashAggregateForSingleKey.name
     * They are meant to suppress CodegenFallback Plan so that optimized
     * byte buffer code path is tested & prevented from false passing.
     * If your test needs CodegenFallback, then override the newConf function
     * & clear the flag from the conf of the test locally.
     */
    new org.apache.spark.SparkConf().setAppName("ClosedFormEstimates")
        .setMaster("local[6]")
        .set("spark.logConf", "true")
        .set(Property.TestDisableCodeGenFlag.name , "true")
        .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")

   }

   def setupData(): Unit = {
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val snContext = this.snc
    snContext.sql("set spark.sql.shuffle.partitions=6")

    airlineDataFrame = snContext.read.load(hfile)
    // airlineDataFrame.registerTempTable("airline")
     airlineDataFrame.write.format("column").mode(SaveMode.Append).saveAsTable("airline")

       /**
      * Total number of executors (or partitions) = Total number of buckets +
      *                                             Total number of reservoir
      */
    /**
      * Partition by influences whether a strata is handled in totality on a
      * single executor or not, and thus an important factor.
      */

       snc.sql(s"CREATE TABLE airlineSampled $ddlStr" +
        "USING column_sample " +
        "options " +
        "(" +
        s"qcs '$qcsColumns'," +
        "BUCKETS '8'," +
        s"fraction '$sampleRatio'," +
        s"strataReservoirSize '$strataReserviorSize', " +
        "baseTable 'airline')")

    airlineDataFrame.write.
        format("column_sample")
        .mode(SaveMode.Append)
        .saveAsTable("airlineSampled")
  }

  def writeSampleTable(): Unit = {
    airlineDataFrame.write.insertInto("airlineSampled")
  }

  def deleteSampleTable(): Unit = {
    snc.sql(s"TRUNCATE TABLE airlineSampled")
  }

  def reCreateSampleTable(eviction: String = "NONE"): Unit = {
    snc.sql(s"CREATE SAMPLE TABLE airlineSampled " +
        " options " +
        " ( " +
        s"qcs '$qcsColumns'," +
        "BUCKETS '8'," +
        s"fraction '$sampleRatio'," +
        s"eviction_by '$eviction'," +
        s"strataReservoirSize '$strataReserviorSize' " +
        " ) " +
        "AS ( " +
        "SELECT YearI, MonthI , DayOfMonth, " +
        "DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, " +
        "UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, " +
        "CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin, " +
        "Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode, " +
        "Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, " +
        "LateAircraftDelay, ArrDelaySlot " +
        " FROM AIRLINE)")
  }

  def dropSampleTable(): Unit = {
    snc.sql(s"DROP TABLE if exists airlineSampled")
  }

  private def countOfSampleTable() : Long = {
    val query: String = s"SELECT Count(*) as sample_cnt" +
        s" FROM airlineSampled"
    val count_rows_on_sample_table = snc.sql(query)
    count_rows_on_sample_table.collect()(0).getLong(0)
  }

  private def tcCountOfSampleTable(s : Statement) : Long = {
    val query: String = s"SELECT Count(*) as sample_cnt" +
        s" FROM airlineSampled"
    s.execute(query)
    val rs = s.getResultSet
    var count : Long = 0
    while (rs.next()) {
      count = rs.getLong(1)
    }
    count
  }

  def tcDeleteSampleTable(s : Statement): Unit = {
    s.execute(s"TRUNCATE TABLE airlineSampled")
  }


  test("Default sample table created has eviction disabled") {
    val allRegions = GemFireCacheImpl.getInstance().getPartitionedRegions.asScala
    val sampleReservoirRgn = allRegions.filter(x => x.getFullPath.indexOf
    ("_SAMPLE_INTERNAL_AIRLINESAMPLED") != -1).iterator.next()
    val sampleColRegion = allRegions.filter(x => x.getFullPath.indexOf
    ("/AIRLINESAMPLED") != -1).iterator.next()


    val ra = sampleReservoirRgn.getAttributes
    val ea = ra.getEvictionAttributes
    assert (ea.getAlgorithm == EvictionAlgorithm.NONE)


    val ra1 = sampleColRegion.getAttributes
    val ea1 = ra1.getEvictionAttributes
    assert (ea1.getAlgorithm == EvictionAlgorithm.NONE)
  }


  test("Sample table is correctly configured when explicit eviction is specified") {
    this.dropSampleTable()
    this.reCreateSampleTable("LRUHEAPPERCENT")
    val allRegions = GemFireCacheImpl.getInstance().getPartitionedRegions.asScala
    val sampleReservoirRgn = allRegions.filter(x => x.getFullPath.indexOf
    ("_SAMPLE_INTERNAL_AIRLINESAMPLED") != -1).iterator.next()
    val sampleColRegion = allRegions.filter(_.getFullPath.matches(
      ".*AIRLINESAMPLED.*_COLUMN_STORE_")).iterator.next()

    val ra = sampleReservoirRgn.getAttributes
    val ea = ra.getEvictionAttributes
    assert (ea.getAlgorithm == EvictionAlgorithm.NONE)

    val ra1 = sampleColRegion.getAttributes
    val ea1 = ra1.getEvictionAttributes
    assert (ea1.getAlgorithm == EvictionAlgorithm.LRU_HEAP)
  }

  ignore("SNAP-676") {
    val serverHostPort = TestUtil.startNetServer()
    // scalastyle:off println
    println(" Network server started.")
    // scalastyle:on println
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val s = conn.createStatement()
    for(i <- 0 to 5) {
      val tcCount = tcCountOfSampleTable(s)
      val count = countOfSampleTable()
      // scalastyle:off println
      println(s"Count-$i = " + count + s" ,tcCount-$i = " + tcCount)
      // scalastyle:on println
      // deleteSampleTable()
      tcDeleteSampleTable(s)
      val tc_count_after_truncate = tcCountOfSampleTable(s)
      val count_after_truncate = countOfSampleTable()
      // scalastyle:off println
      println(s"count_after_truncate-$i = " + count_after_truncate
          + s" ,tc_count_after_truncate-$i = " + tc_count_after_truncate)
      // scalastyle:on println
      writeSampleTable()
    }

    for (i <- 0 to 5) {
      val tcCount = tcCountOfSampleTable(s)
      val count = countOfSampleTable()
      // scalastyle:off println
      println(s"Count-$i = " + count + s" ,tcCount-$i = " + tcCount + " : Now drop")
      // scalastyle:on println
      dropSampleTable()
      reCreateSampleTable()
    }
  }

  test("sample table not getting recreated correctly on cluster restart. Bug AQP-295") {
    this.dropSampleTable()
    this.reCreateSampleTable("LRUHEAPPERCENT")
    val countMap = snc.sql(s"select $qcsColumns, count(*) as sample_actual_count " +
      s"from airlineSampled group by $qcsColumns").collect().
      map(row => (row.getString(0), row.getLong(1))).toMap
    assertTrue(countMap.size > 0)
    countMap.foreach(x => assertTrue(x._2 > 0))
    // stop the cluster
    this.stopAll()
    // restart the cluster
    snc
    val countMap1 = snc.sql(s"select $qcsColumns, count(*) as sample_actual_count " +
      s"from airlineSampled group by $qcsColumns").collect().
      map(row => (row.getString(0), row.getLong(1))).toMap
    assertTrue(countMap1.size > 0)
    assertEquals(countMap.size, countMap1.size)
    countMap.foreach(tup => assertEquals(tup._2, countMap1.getOrElse(tup._1, null)))
    snc.sql(s"DROP TABLE airlineSampled")
  }

}
