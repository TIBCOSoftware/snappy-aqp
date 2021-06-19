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

import java.io.ByteArrayOutputStream
import java.sql.{Connection, DriverManager}

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.{Constant => Constants}

import org.apache.spark.sql.{DataFrame, SaveMode, SnappyContext, functions}

class SamplingDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  val testNo: Int = 1

  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE
  private var airlineDataFrame: DataFrame = _

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

  var qcsColumns: String = "UniqueCarrier"
  // Same as QCS
  var qcs: String = "START"
  var currQCS: String = ""
  val strataReserviorSize = 50
  // TODO Change as needed
  val sampleRatio: Double = 0.03

  override def beforeClass(): Unit = {
    // stop any running lead first to update the "maxErrorAllowed" property
    ClusterManagerTestBase.stopSpark()
    bootProps.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
    bootProps.setProperty(Constants.RESERVOIR_AS_REGION, "true")
    // bootProps.setProperty("log-level", "info")
    super.beforeClass()
  }

  override def afterClass(): Unit = {
    super.afterClass()
    // force restart with default properties in subsequent tests
    ClusterManagerTestBase.stopSpark()
  }

  override def tearDown2(): Unit = {
    // reset the chunk size on lead node
    setDMLMaxChunkSize(default_chunk_size)
    super.tearDown2()
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  def getANetConnection(netPort: Int): Connection = {
    val driver = "com.pivotal.gemfirexd.jdbc.ClientDriver"
    Class.forName(driver).newInstance //scalastyle:ignore
    val url = s"jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  protected val createTableOptions: String =
    s" using column options (partition_by 'DayOfMonth', Buckets '8') "

  def dropData(snc: SnappyContext): Unit = {
    snc.sql(s"drop table if exists airlineSampled$testNo")
    snc.sql(s"drop table if exists airline$testNo")
  }

  def setupData(snc: SnappyContext): Unit = {
    snc.sql("set spark.sql.shuffle.partitions=6")

    snc.sql(s"drop table if exists airlineSampled$testNo")
    snc.sql(s"drop table if exists airline$testNo")

    snc.sql(s"create table if not exists airline$testNo $ddlStr $createTableOptions ")

    val hfile: String = getClass.getResource("/2015.parquet").getPath
    this.airlineDataFrame = snc.read.load(hfile)
    val start0 = System.currentTimeMillis
    // airlineDataFrame.write.insertInto(s"airline$testNo")

    val start1 = System.currentTimeMillis
    snc.sql(s"CREATE TABLE airlineSampled$testNo $ddlStr" +
        "USING column_sample " +
        "options " +
        "(" +
        s"qcs '$qcsColumns'," +
        s"fraction '$sampleRatio'," +
        s"strataReservoirSize '$strataReserviorSize', " +
        s"baseTable 'airline$testNo')")

    val start2 = System.currentTimeMillis
    airlineDataFrame.write.
        format("column_sample")
        .mode(SaveMode.Append)
        .saveAsTable(s"airlineSampled$testNo")


    val start3 = System.currentTimeMillis
    // scalastyle:off println
    println(s"Time sample ddl=${start2 - start1}ms " +
        s"insert=${start3 - start2}ms base=${start1 - start0}ms")
    // scalastyle:on println
  }

  private def countBase(snc: SnappyContext, aggCol: String): Long = {
    val query: String =
      s"SELECT COUNT($aggCol) as COUNT FROM airline$testNo"
    val count_result_direct_on_sample_table = snc.sql(query)
    count_result_direct_on_sample_table.collect()(0).getLong(0)
  }

  private def countDirect(snc: SnappyContext, aggCol: String): Long = {
    val query: String =
      s"SELECT COUNT($aggCol) as COUNT FROM airline$testNo with error"
    val count_result_direct_on_sample_table = snc.sql(query)
    count_result_direct_on_sample_table.collect()(0).getLong(0)
  }

  private def countSample(snc: SnappyContext, aggCol: String): Long = {
    val query: String =
      s"SELECT COUNT($aggCol) as SAMPLE_COUNT FROM airline$testNo with error"
    val count_result_direct_on_sample_table = snc.sql(query)
    count_result_direct_on_sample_table.collect()(0).getLong(0)
  }

  private def sumBase(snc: SnappyContext, aggCol: String): Long = {
    val query: String =
      s"SELECT sum($aggCol) as SUM FROM airline$testNo"
    val sum_result_direct_on_sample_table = snc.sql(query)
    sum_result_direct_on_sample_table.collect()(0).getLong(0)
  }

  private def sumDirect(snc: SnappyContext, aggCol: String): Long = {
    val query: String =
      s"SELECT sum($aggCol) as SUM FROM airline$testNo with error"
    val sum_result_direct_on_sample_table = snc.sql(query)
    sum_result_direct_on_sample_table.collect()(0).getLong(0)
  }

  private def printDouble(des: String, value: Double): Boolean = {
    // scalastyle:off println
    println(des + " = %.5f".format(value))
    // scalastyle:on println
    false
  }

  def testAQP79(): Unit = {
    val snc = SnappyContext(sc)

    // val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    // vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    // val conn = getANetConnection(netPort1)
    // val s = conn.createStatement()

    setupData(snc: SnappyContext)
    val n = 1
    // scalastyle:off println
    println(s"AQP-79-$n :")
    // scalastyle:on println

    // val countB = countBase(snc, "ArrDelay")
    // val sumB = sumBase(snc, "ArrDelay")
    // scalastyle:off println
    // println(s"Base - Count : $countB Sum: $sumB")
    // scalastyle:on println
    val countActual = this.airlineDataFrame.agg(
      functions.count(airlineDataFrame("ArrDelay"))).collect()(0).getLong(0)
    val count_sample = countSample(snc, "ArrDelay")
    val count = countDirect(snc, "ArrDelay") // ~1888622
    val sum = sumDirect(snc, "ArrDelay") // ~(9537150 - 10285273)
    // scalastyle:off println
    println(s"Sample - Count : $count Sum: $sum Sample-count: $count_sample  " +
        s"Base Table Count = $countActual")
    // scalastyle:on println

    // assert
    assert((countActual - count).abs < 2, s"countActual=$countActual count=$count")

    if (false) {
      val dump_sampled_table1 = snc.sql(
        s"SELECT SNAPPY_SAMPLER_WEIGHTAGE, count(ArrDelay) " +
            s"FROM airlineSampled$testNo GROUP BY SNAPPY_SAMPLER_WEIGHTAGE")
      val samstream1 = new ByteArrayOutputStream()
      Console.withOut(new java.io.PrintStream(samstream1))(dump_sampled_table1.show(99999))
      // scalastyle:off println
      println("Sample1-start: ")
      println(samstream1.toString)
      println("Sample1-end: ")
      // scalastyle:on println
    }
    dropData(snc)
    // scalastyle:off println
    println(s"Done ")
    // scalastyle:on println
  }
}
