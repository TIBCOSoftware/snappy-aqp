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

import java.sql.{Connection, DriverManager, Statement}

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.sql.{DataFrame, SnappyContext}

class AQPDUnitTest (val s: String) extends ClusterManagerTestBase(s) {

  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE

  val mainTable: String = "airline"
  val sampleTable: String = "airline_sampled"
  var airlineDataFrame: DataFrame = _

  override def tearDown2(): Unit = {
    // reset the chunk size on lead node
    setDMLMaxChunkSize(default_chunk_size)
    super.tearDown2()
  }


  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

   def getANetConnection(netPort: Int): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Class.forName(driver).newInstance //scalastyle:ignore
    val url = s"jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  def setupData(snc : SnappyContext): Unit = {
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    snc.sql("set spark.sql.shuffle.partitions=6")
    airlineDataFrame = snc.read.load(hfile)

    airlineDataFrame.registerTempTable(s"$mainTable")
    /** To run test with main tables as sample table, uncomment following.
      * Also, increase required memory for VMs from
      * 1. AQP's build.gradle (for controller VM)
      * For Child VMs, memory is configurable from ProcessManager.java
      *
      * Spark specific settings are mentioned in ClusterManagerTestBase.
      * See "conf.set" lines.
      */
    // Create main table as column table
    // val p1 = Map(("BUCKETS" -> "16"))
    // airlineDataFrame.write.format("column").mode(SaveMode.Append).
    // options(p1).saveAsTable(mainTable)
  }

  def setupData(snc : SnappyContext, s: Statement): Unit = {
    snc.sql("set spark.sql.shuffle.partitions=6")

    s.executeUpdate(s"drop table if exists $sampleTable")
    s.executeUpdate(s"drop table if exists STAGING_$mainTable")
    s.executeUpdate(s"drop table if exists $mainTable")

    // val hfile: String = getClass.getResource("/2015.parquet").getPath
    s.executeUpdate(s"CREATE EXTERNAL TABLE STAGING_$mainTable " +
        s"USING parquet OPTIONS(path " +
        "'../../../../snappy/quickstart/data/airlineParquetData'" +
        // "../../../../snappy/quickstart/data/airlineParquetData_2007-15" +
        s")")

    s.executeUpdate(s"CREATE TABLE $mainTable USING column AS ("
        + " SELECT Year AS Year_, Month AS Month_ , DayOfMonth,"
        + " DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,"
        + " UniqueCarrier, FlightNum, TailNum, ActualElapsedTime,"
        + " CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin,"
        + " Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode,"
        + " Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay,"
        + " LateAircraftDelay, ArrDelaySlot"
        + s" FROM STAGING_$mainTable)")

    s.executeUpdate(s"CREATE SAMPLE TABLE AIRLINE_SAMPLE ON AIRLINE "
        + " OPTIONS("
        + " buckets '8',"
        + " qcs 'UniqueCarrier, Year_, Month_',"
        + " fraction '0.03',"
        + " strataReservoirSize '50')"
        + " AS ("
        + " SELECT Year_, Month_ , DayOfMonth,"
        + " DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,"
        + " UniqueCarrier, FlightNum, TailNum, ActualElapsedTime,"
        + " CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin,"
        + " Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode,"
        + " Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay,"
        + " LateAircraftDelay, ArrDelaySlot"
        + s" FROM $mainTable)")
  }

  def dropSampleTable(snc : SnappyContext): Unit = {
    // scalastyle:off println
    println(" DROP...")
    // scalastyle:on println
    snc.sql(s"DROP TABLE if exists $sampleTable")
  }

  def tcDropSampleTable(s : Statement): Unit = {
    // scalastyle:off println
    println(" DROP...")
    // scalastyle:on println
    s.executeUpdate(s"DROP TABLE if exists $sampleTable")
  }

  def createSampleTable(snc : SnappyContext): Unit = {
    snc.sql(s"CREATE SAMPLE TABLE $sampleTable " +
        " options " +
        " ( " +
        s"qcs 'UniqueCarrier'," +
        "BUCKETS '8'," +
        s"fraction '0.03'," +
        s"strataReservoirSize '50' " +
        " ) " +
        "AS ( " +
        "SELECT YearI, MonthI , DayOfMonth, " +
        "DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, " +
        "UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, " +
        "CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin, " +
        "Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode, " +
        "Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, " +
        "LateAircraftDelay, ArrDelaySlot " +
        s" FROM $mainTable)")
  }

  def truncateSampleTable(snc: SnappyContext): Unit = {
    // scalastyle:off println
    println(" TRUNCATE...")
    // scalastyle:on println
    snc.sql(s"TRUNCATE TABLE $sampleTable")
  }

  def tcTruncateSampleTable(s: Statement): Unit = {
    // scalastyle:off println
    println(" TRUNCATE...tc...")
    // scalastyle:on println
    s.executeUpdate(s"TRUNCATE TABLE $sampleTable")
  }

  def rePopulateSampleTable(): Unit = {
    // scalastyle:off println
    println(" REPOPULATE...")
    // scalastyle:on println
    airlineDataFrame.write.insertInto(s"$sampleTable")
  }

  def showDirectCount(snc: SnappyContext) : Unit = {
    val rs = snc.sql(s"SELECT count(*) as sample_cnt, count(*)" +
        s" FROM $sampleTable")
    // scalastyle:off println
    println(" direct_sample_count..." + rs.collect()(0).getLong(0))
    println(" direct_count..." + rs.collect()(0).getLong(1))
    // scalastyle:on println
  }

  def tcShowDirectCount(s: Statement) : Unit = {
    val rs = s.executeQuery(s"SELECT count(*) as sample_cnt, count(*)" +
        s" FROM $sampleTable")
    rs.next()
    // scalastyle:off println
    println(" tc_sample_count..." + rs.getDouble(1))
    println(" tc_count..." + rs.getDouble(2))
    // scalastyle:on println
  }

  def showSampleCount(snc: SnappyContext, doPrint : Boolean) : Long = {
    val rs = snc.sql(s"SELECT count(*) as sample_cnt " +
        s" FROM $sampleTable")
    if (doPrint) {
      // scalastyle:off println
      println(" direct_sample_count..." + rs.collect()(0).getLong(0))
      // scalastyle:on println
    }
    rs.collect()(0).getLong(0)
  }

  def tcShowSampleCount(s: Statement, doPrint : Boolean) : Double = {
    val rs = s.executeQuery(s"SELECT count(*) as sample_cnt " +
        s" FROM $sampleTable")
    rs.next()
    if (doPrint) {
      // scalastyle:off println
      println(" tc_sample_count..." + rs.getDouble(1))
      // scalastyle:on println
    }
    rs.getDouble(1)
  }

  def testSNAP376(): Unit = {
    val verbose = false
    val snc = SnappyContext(sc)
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    setupData(snc)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()

    createSampleTable(snc)
    showSampleCount(snc, verbose)
    tcShowSampleCount(s, verbose)

    // Now truncate
    truncateSampleTable(snc)
    val dsc = showSampleCount(snc, verbose)
    assert(dsc < 1, s"count = $dsc")
    val tcsc = tcShowSampleCount(s, verbose)
    assert(tcsc < 1, s"count = $tcsc")
    rePopulateSampleTable()
    showSampleCount(snc, verbose)
    tcShowSampleCount(s, verbose)


    dropSampleTable(snc)

//   TODO:Enable after SNAP-1812 fix.

//    for(i <- 0 to 5) {
//      createSampleTable(snc)
//      showSampleCount(snc, verbose)
//      tcShowSampleCount(s, verbose)
//      dropSampleTable(snc)
//    }

  }

  /**
    * This test creates all tables same as Quick Start.
    * But queries can be changed.
    */
  def testQuickStart(): Unit = {
    val doPrint = true
    val snc = SnappyContext(sc)
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()
    setupData(snc, s)
    queryRoutingSum(snc, s, doPrint)
    queryRoutingAvg(snc, s, doPrint)
  }

  private def queryRoutingAvg(snc: SnappyContext, s: Statement, doPrint: Boolean): Unit = {
    val queries = Array(
      s"select AVG(ArrDelay) arrivalDelay" +
          s" ,lower_bound(arrivalDelay), upper_bound(arrivalDelay) " +
          s" ,absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
          // s" ,UniqueCarrier carrier " +
          s" from $mainTable     " +
          s" where DayOfWeek > 2 " +
          s" group by UniqueCarrier  " +
          s" order by UniqueCarrier " +
          s" with error .2" +
          s" behavior 'DO_NOTHING'",

      s"select AVG(ArrDelay) arrivalDelay" +
          s" ,lower_bound(arrivalDelay), upper_bound(arrivalDelay) " +
          s" ,absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
          // s" ,UniqueCarrier carrier " +
          s" from $mainTable     " +
          s" where DayOfWeek > 2 " +
          s" group by UniqueCarrier  " +
          s" order by UniqueCarrier " +
          s" with error .2" +
          s" behavior 'RUN_ON_FULL_TABLE'",

      s"select AVG(ArrDelay) arrivalDelay" +
          s" ,lower_bound(arrivalDelay), upper_bound(arrivalDelay) " +
          s" ,absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
          s" ,UniqueCarrier carrier " +
          s" from $mainTable     " +
          s" where DayOfWeek > 2 " +
          s" group by UniqueCarrier  " +
          s" order by UniqueCarrier " +
          s" with error .2" +
          s" behavior 'PARTIAL_RUN_ON_BASE_TABLE'",

    s"select AVG(ArrDelay) arrivalDelay" +
        s" ,lower_bound(arrivalDelay), upper_bound(arrivalDelay) " +
        s" ,absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
        // s" ,UniqueCarrier carrier " +
        s" from $mainTable     " +
        s" where DayOfWeek > 2 " +
        s" group by UniqueCarrier  " +
        s" order by arrivalDelay " +
        s" with error .2" +
        s" behavior 'PARTIAL_RUN_ON_BASE_TABLE'",

    s"select AVG(ArrDelay) arrivalDelay" +
        s" ,lower_bound(arrivalDelay), upper_bound(arrivalDelay) " +
        s" ,absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
        // s" ,UniqueCarrier carrier " +
        s" from $mainTable     " +
        s" where DayOfWeek > 2 " +
        s" group by UniqueCarrier  " +
        // s" order by arrivalDelay " +
        s" with error .2" +
        s" behavior 'PARTIAL_RUN_ON_BASE_TABLE'"

    /** TODO AQP-201
      s"select AVG(ArrDelay) arrivalDelay" +
          s" ,lower_bound(arrivalDelay), upper_bound(arrivalDelay) " +
          s" ,absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
          // s" ,UniqueCarrier carrier " +
          s" from $mainTable     " +
          s" where DayOfWeek > 2 " +
          s" group by UniqueCarrier  " +
          s" order by arrivalDelay " +
          s" with error .2" +
          s" behavior 'PARTIAL_RUN_ON_BASE_TABLE'"
      */
    )

    for (query <- queries) {
      // scalastyle:off println
      System.out.println("query=" + query)
      // scalastyle:on println
      val rs = s.executeQuery(query)
      while (rs.next()) {
        if (doPrint) {
          // scalastyle:off println
          println(rs.getDouble(1) + " ," +
              rs.getDouble(2) + " ," +
              rs.getDouble(3) + " ," +
              rs.getDouble(4) + " ," +
              rs.getDouble(5) + " ," +
              "")
          // scalastyle:on println
        }
      }
    }

    //    for(query <- queries) {
    //      val results = snc.sql(query)
    //      results.show
    //    }
  }

  private def queryRoutingSum(snc: SnappyContext, s: Statement, doPrint: Boolean): Unit = {
    val queries = Array(
      s"select Sum(ArrDelay) arrivalDelay" +
          s" ,lower_bound(arrivalDelay), upper_bound(arrivalDelay) " +
          s" ,absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
          // s" ,UniqueCarrier carrier " +
          s" from $mainTable     " +
          s" where DayOfWeek > 2 " +
          s" group by UniqueCarrier  " +
          s" order by UniqueCarrier " +
          s" with error .2" +
          s" behavior 'DO_NOTHING'",

      s"select Sum(ArrDelay) arrivalDelay" +
          s" ,lower_bound(arrivalDelay), upper_bound(arrivalDelay) " +
          s" ,absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
          // s" ,UniqueCarrier carrier " +
          s" from $mainTable     " +
          s" where DayOfWeek > 2 " +
          s" group by UniqueCarrier  " +
          s" order by UniqueCarrier " +
          s" with error .2" +
          s" behavior 'RUN_ON_FULL_TABLE'",

      s"select Sum(ArrDelay) arrivalDelay" +
          s" ,lower_bound(arrivalDelay), upper_bound(arrivalDelay) " +
          s" ,absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
          s" ,UniqueCarrier carrier " +
          s" from $mainTable     " +
          s" where DayOfWeek > 2 " +
          s" group by UniqueCarrier  " +
          s" order by UniqueCarrier " +
          s" with error .2" +
          s" behavior 'PARTIAL_RUN_ON_BASE_TABLE'",

      s"select Sum(ArrDelay) arrivalDelay" +
          s" ,lower_bound(arrivalDelay), upper_bound(arrivalDelay) " +
          s" ,absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
          // s" ,UniqueCarrier carrier " +
          s" from $mainTable     " +
          s" where DayOfWeek > 2 " +
          s" group by UniqueCarrier  " +
          s" order by arrivalDelay " +
          s" with error .2" +
          s" behavior 'PARTIAL_RUN_ON_BASE_TABLE'",

      s"select Sum(ArrDelay) arrivalDelay" +
          s" ,lower_bound(arrivalDelay), upper_bound(arrivalDelay) " +
          s" ,absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
          // s" ,UniqueCarrier carrier " +
          s" from $mainTable     " +
          s" where DayOfWeek > 2 " +
          s" group by UniqueCarrier  " +
          // s" order by arrivalDelay " +
          s" with error .2" +
          s" behavior 'PARTIAL_RUN_ON_BASE_TABLE'"

      /** TODO AQP-201
      s"select AVG(ArrDelay) arrivalDelay" +
          s" ,lower_bound(arrivalDelay), upper_bound(arrivalDelay) " +
          s" ,absolute_error(arrivalDelay), relative_error(arrivalDelay) " +
          // s" ,UniqueCarrier carrier " +
          s" from $mainTable     " +
          s" where DayOfWeek > 2 " +
          s" group by UniqueCarrier  " +
          s" order by arrivalDelay " +
          s" with error .2" +
          s" behavior 'PARTIAL_RUN_ON_BASE_TABLE'"
        */
    )

    for (query <- queries) {
      // scalastyle:off println
      System.out.println("query=" + query)
      // scalastyle:on println
      val rs = s.executeQuery(query)
      while (rs.next()) {
        if (doPrint) {
          // scalastyle:off println
          println(rs.getInt(1) + " ," +
              rs.getInt(2) + " ," +
              rs.getInt(3) + " ," +
              rs.getInt(4) + " ," +
              rs.getInt(5) + " ," +
              "")
          // scalastyle:on println
        }
      }
    }

    //    for(query <- queries) {
    //      val results = snc.sql(query)
    //      results.show
    //    }
  }
}
