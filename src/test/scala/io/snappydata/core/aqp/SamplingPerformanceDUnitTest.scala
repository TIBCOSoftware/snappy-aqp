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

import java.io.ByteArrayOutputStream
import java.sql.{Connection, DriverManager}

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.{Constant => Constants}

import org.apache.spark.sql.{SaveMode, SnappyContext}

class SamplingPerformanceDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  private val disabled = true

  val testNo: Int = 1

  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE

  val ddlInternalStr = "YearI INT," + // NOT NULL
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
      "ArrDelaySlot INT"
  val ddlStr = s"($ddlInternalStr)"

  val groupByCol = s" MonthI "

  var qcsColumns: String = "UniqueCarrier"
  // Same as QCS
  var qcs: String = "START"
  var currQCS: String = ""
  val strataReserviorSize = 50
  // TODO Change as needed
  val sampleRatio: Double = 0.8

  override def beforeClass(): Unit = {
    if (disabled) {
      super.beforeClass()
      return
    }
    // stop any running lead first to update the "maxErrorAllowed" property
    ClusterManagerTestBase.stopSpark()
    bootProps.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
    bootProps.setProperty(Constants.RESERVOIR_AS_REGION, "true")
    // bootProps.setProperty("log-level", "info")
    super.beforeClass()
  }

  override def afterClass(): Unit = {
    super.afterClass()
    if (disabled) return

    // force restart with default properties in subsequent tests
    ClusterManagerTestBase.stopSpark()
  }

  override def tearDown2(): Unit = {
    if (disabled) return

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

  protected val createTableOptions: String =
    s" using column options (partition_by 'DayOfMonth', Buckets '8', eviction_by 'none') "

  def dropData(snc: SnappyContext): Unit = {
    snc.sql(s"drop table if exists airlineWOE$testNo")
    snc.sql(s"drop table if exists airlineSampled$testNo")
    snc.sql(s"drop table if exists airline$testNo")
  }

  def setupData(snc: SnappyContext): Unit = {
    snc.sql("set spark.sql.shuffle.partitions=6")

    dropData(snc)
    snc.sql(s"create table if not exists airline$testNo $ddlStr $createTableOptions ")

    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val airlineDataFrame = snc.read.load(hfile)
    val start0 = System.currentTimeMillis
    // airlineDataFrame.write.insertInto(s"airline$testNo")

    val start1 = System.currentTimeMillis
    snc.sql(s"CREATE TABLE airlineSampled$testNo $ddlStr " +
        "USING column_sample " +
        "options " +
        "(" +
        s"qcs '$qcsColumns'," +
        s"fraction '$sampleRatio'," +
        s"eviction_by 'NONE'," +
        s"strataReservoirSize '$strataReserviorSize', " +
        s"baseTable 'airline$testNo')")

    val start2 = System.currentTimeMillis
   /* airlineDataFrame.write.
        format("column_sample")
        .mode(SaveMode.Append)
        .saveAsTable(s"airlineSampled$testNo") */

    val start3 = System.currentTimeMillis
    snc.sql(s"create table if not exists airlineWOE$testNo" +
        s" ($ddlInternalStr, weightage Long) $createTableOptions ")
    val sampleWOE_df = snc.sql(s"Select * from airlineSampled$testNo")

    val start4 = System.currentTimeMillis
    sampleWOE_df.write.insertInto(s"airlineWOE$testNo")

    val start5 = System.currentTimeMillis
    sampleWOE_df.unpersist()

    // scalastyle:off println
    println(s"Time sample ddl=${start2 - start1}ms " +
        s" base=${start1 - start0}ms woe=${start5 - start4}ms")
    // scalastyle:on println
  }

  private def executeQuery(snc: SnappyContext, query: String,
      isGroupBy: Boolean): (Long, Long, Long) = {
    val start11 = System.currentTimeMillis
    val result = snc.sql(query)
    val start12 = System.currentTimeMillis
    val resultCollect = if (isGroupBy) {
      result.collect().map(r => r.getLong(0)).sum
    } else result.collect()(0).getLong(0)
    val start13 = System.currentTimeMillis
    (resultCollect, start12 - start11, start13 - start12)
  }

  private def countBase(snc: SnappyContext, aggCol: String,
      isGroupBy: Boolean): (Long, Long, Long) = {
    val query: String = if (isGroupBy) {
      s"SELECT COUNT($aggCol) as COUNT FROM airline$testNo group by $groupByCol"
    } else s"SELECT COUNT($aggCol) as COUNT FROM airline$testNo"
    executeQuery(snc, query, isGroupBy)
  }

  private def countWOE(snc: SnappyContext, aggCol: String,
      isGroupBy: Boolean): (Long, Long, Long) = {
    val query: String = if (isGroupBy) {
      s"SELECT COUNT($aggCol) as COUNT FROM airlineWOE$testNo group by $groupByCol"
    } else s"SELECT COUNT($aggCol) as COUNT FROM airlineWOE$testNo"
    executeQuery(snc, query, isGroupBy)
  }

  private def countSample(snc: SnappyContext, aggCol: String,
      isGroupBy: Boolean): (Long, Long, Long) = {
    val query: String = if (isGroupBy) {
      s"SELECT COUNT($aggCol) as COUNT FROM airline$testNo group by $groupByCol with error"
    } else s"SELECT COUNT($aggCol) as COUNT FROM airline$testNo with error"
    executeQuery(snc, query, isGroupBy)
  }

  private def countDirect(snc: SnappyContext, aggCol: String,
      isGroupBy: Boolean): (Long, Long, Long) = {
    val query: String = if (isGroupBy) {
      s"SELECT COUNT($aggCol) as SAMPLE_COUNT FROM airline$testNo group by $groupByCol with error"
    } else s"SELECT COUNT($aggCol) as SAMPLE_COUNT FROM airline$testNo with error"
    executeQuery(snc, query, isGroupBy)
  }

  private def countDirect2(snc: SnappyContext, aggCol: String,
      isGroupBy: Boolean): (Long, Long, Long) = {
    val query: String = if (isGroupBy) {
      s"SELECT COUNT($aggCol) as SAMPLE_COUNT FROM airlineSampled$testNo group by $groupByCol"
    } else s"SELECT COUNT($aggCol) as SAMPLE_COUNT FROM airlineSampled$testNo"
    executeQuery(snc, query, isGroupBy)
  }

  private def sumBase(snc: SnappyContext, aggCol: String,
      isGroupBy: Boolean): (Long, Long, Long) = {
    val query: String = if (isGroupBy) {
      s"SELECT sum($aggCol) as SUM FROM airline$testNo group by $groupByCol"
    } else s"SELECT sum($aggCol) as SUM FROM airline$testNo"
    executeQuery(snc, query, isGroupBy)
  }

  private def sumSample(snc: SnappyContext, aggCol: String,
      isGroupBy: Boolean): (Long, Long, Long) = {
    val query: String = if (isGroupBy) {
      s"SELECT sum($aggCol) as SUM FROM airline$testNo group by $groupByCol with error"
    } else s"SELECT sum($aggCol) as SUM FROM airline$testNo with error"
    executeQuery(snc, query, isGroupBy)
  }

  private def sumDirect(snc: SnappyContext, aggCol: String,
      isGroupBy: Boolean): (Long, Long, Long) = {
    val query: String = if (isGroupBy) {
      s"SELECT sum($aggCol) as SAMPLE_SUM FROM airline$testNo group by $groupByCol with error"
    } else s"SELECT sum($aggCol) as SAMPLE_SUM FROM airline$testNo with error"
    executeQuery(snc, query, isGroupBy)
  }

  private def sumDirect2(snc: SnappyContext, aggCol: String,
      isGroupBy: Boolean): (Long, Long, Long) = {
    val query: String = if (isGroupBy) {
      s"SELECT sum($aggCol) as SAMPLE_SUM FROM airlineSampled$testNo group by $groupByCol"
    } else s"SELECT sum($aggCol) as SAMPLE_SUM FROM airlineSampled$testNo"
    executeQuery(snc, query, isGroupBy)
  }

  private def sumWOE(snc: SnappyContext, aggCol: String,
      isGroupBy: Boolean): (Long, Long, Long) = {
    val query: String = if (isGroupBy) {
      s"SELECT sum($aggCol) as SUM FROM airlineWOE$testNo group by $groupByCol"
    } else s"SELECT sum($aggCol) as SUM FROM airlineWOE$testNo"
    executeQuery(snc, query, isGroupBy)
  }

  def printSampleCount(snc: SnappyContext, numTimes: Int,
      isGroupBy: Boolean): Unit = {
    if (numTimes == 1) {
      val (count, count_df_t, count_ex_t) = countSample(snc, "ArrDelay", isGroupBy) // ~1888622
      // scalastyle:off println
      println(s"Count : $count df_creation_time=$count_df_t execution=$count_ex_t " +
          s"total=${count_df_t + count_ex_t} : Sample estimation")
      // scalastyle:on println

      if (!isGroupBy) {
        // assert
        assert((1888622 - count).abs < 2)
      }
    } else if (numTimes > 1) {
      var ncount: Long = 0
      var ncount_df_t: Long = 0
      var ncount_ex_t: Long = 0
      for (i <- 0 until numTimes) {
        val (count, count_df_t, count_ex_t) = countSample(snc, "ArrDelay", isGroupBy) // ~1888622
        ncount += count
        ncount_df_t += count_df_t
        ncount_ex_t += count_ex_t
      }
      // scalastyle:off println
      println(s"Count : ${ncount / numTimes} df_creation_time=${ncount_df_t / numTimes} " +
          s"execution_time=${ncount_ex_t / numTimes} " +
          s"total-time=${(ncount_df_t + ncount_ex_t) / numTimes} :" +
          s" $numTimes times : Sample estimation")
      // scalastyle:on println
    }
  }

  def printSampleSum(snc: SnappyContext, numTimes: Int,
      isGroupBy: Boolean): Unit = {
    if (numTimes == 1) {
      val (sum, sum_df_t, sum_ex_t) = sumSample(snc, "ArrDelay", isGroupBy) // ~(9537150 - 10285273)
      // scalastyle:off println
      println(s"Sum : $sum df_creation_time=$sum_df_t execution=$sum_ex_t " +
          s"total-time=${sum_df_t + sum_ex_t} : Sample estimation")
      // scalastyle:on println
    } else if (numTimes > 1) {
      var nsum: Long = 0
      var nsum_df_t: Long = 0
      var nsum_ex_t: Long = 0
      for (i <- 0 until numTimes) {
        val (sum, sum_df_t, sum_ex_t) = sumSample(snc, "ArrDelay",
          isGroupBy) // ~(9537150 - 10285273)
        nsum += sum
        nsum_df_t += sum_df_t
        nsum_ex_t += sum_ex_t
      }
      // scalastyle:off println
      println(s"Sum : ${nsum / numTimes} df_creation_time=${nsum_df_t / numTimes} " +
          s"execution_time=${nsum_ex_t / numTimes} " +
          s"total-time=${(nsum_df_t + nsum_ex_t) / numTimes} : $numTimes times : Sample estimation")
      // scalastyle:on println
    }
  }

  def printSampleCountDirect(snc: SnappyContext, numTimes: Int,
      isGroupBy: Boolean): Unit = {
    if (numTimes == 1) {
      val (count, count_df_t, count_ex_t) = countDirect(snc, "ArrDelay", isGroupBy)
      // scalastyle:off println
      println(s"Count : $count df_creation_time=$count_df_t " +
          s"execution_time=$count_ex_t total-time=${
            count_df_t +
                count_ex_t
          } : Sample direct (no estimation)")
      // scalastyle:on println
    } else if (numTimes > 1) {
      var ncount: Long = 0
      var ncount_df_t: Long = 0
      var ncount_ex_t: Long = 0
      for (i <- 0 until numTimes) {
        val (count, count_df_t, count_ex_t) = countDirect(snc, "ArrDelay", isGroupBy)
        ncount += count
        ncount_df_t += count_df_t
        ncount_ex_t += count_ex_t
      }
      // scalastyle:off println
      println(s"Count : ${ncount / numTimes} df_creation_time=${ncount_df_t / numTimes} " +
          s"execution_time=${ncount_ex_t / numTimes} " +
          s"total-time=${(ncount_df_t + ncount_ex_t) / numTimes} : $numTimes times " +
          s": Sample direct (no estimation)")
      // scalastyle:on println
    }
  }

  def printSampleSumDirect(snc: SnappyContext, numTimes: Int,
      isGroupBy: Boolean): Unit = {
    if (numTimes == 1) {
      val (sum, sum_df_t, sum_ex_t) = sumDirect(snc, "ArrDelay", isGroupBy)
      // scalastyle:off println
      println(s"Sum : $sum df_creation_time=$sum_df_t execution_time=$sum_ex_t " +
          s"total-time=${sum_df_t + sum_ex_t} : Sample direct (no estimation)")
      // scalastyle:on println
    } else if (numTimes > 1) {
      var nsum: Long = 0
      var nsum_df_t: Long = 0
      var nsum_ex_t: Long = 0
      for (i <- 0 until numTimes) {
        val (sum, sum_df_t, sum_ex_t) = sumDirect(snc, "ArrDelay", isGroupBy)
        nsum += sum
        nsum_df_t += sum_df_t
        nsum_ex_t += sum_ex_t
      }
      // scalastyle:off println
      println(s"Sum : ${nsum / numTimes} df_creation_time=${nsum_df_t / numTimes} " +
          s"execution_time=${nsum_ex_t / numTimes} " +
          s"total-time=${(nsum_df_t + nsum_ex_t) / numTimes} : $numTimes times : " +
          s"Sample direct (no estimation)")
      // scalastyle:on println
    }
  }

  def printSampleCountDirect2(snc: SnappyContext, numTimes: Int,
      isGroupBy: Boolean): Unit = {
    if (numTimes == 1) {
      val (count, count_df_t, count_ex_t) = countDirect2(snc, "ArrDelay", isGroupBy)
      // scalastyle:off println
      println(s"Count : $count df_creation_time=$count_df_t " +
          s"execution_time=$count_ex_t " +
          s"total-time=${count_df_t + count_ex_t} : Direct on sample table (no estimation)")
      // scalastyle:on println
    } else if (numTimes > 1) {
      var ncount: Long = 0
      var ncount_df_t: Long = 0
      var ncount_ex_t: Long = 0
      for (i <- 0 until numTimes) {
        val (count, count_df_t, count_ex_t) = countDirect2(snc, "ArrDelay", isGroupBy)
        ncount += count
        ncount_df_t += count_df_t
        ncount_ex_t += count_ex_t
      }
      // scalastyle:off println
      println(s"Count : ${ncount / numTimes} df_creation_time=${ncount_df_t / numTimes} " +
          s"execution_time=${ncount_ex_t / numTimes} " +
          s"total-time=${(ncount_df_t + ncount_ex_t) / numTimes} : $numTimes times :" +
          s" Direct on sample table (no estimation)")
      // scalastyle:on println
    }
  }

  def printSampleSumDirect2(snc: SnappyContext, numTimes: Int,
      isGroupBy: Boolean): Unit = {
    if (numTimes == 1) {
      val (sum, sum_df_t, sum_ex_t) = sumDirect2(snc, "ArrDelay", isGroupBy)
      // scalastyle:off println
      println(s"Sum : $sum df_creation_time=$sum_df_t execution_time=$sum_ex_t " +
          s"total-time=${sum_df_t + sum_ex_t} : Direct on sample table (no estimation)")
      // scalastyle:on println
    } else if (numTimes > 1) {
      var nsum: Long = 0
      var nsum_df_t: Long = 0
      var nsum_ex_t: Long = 0
      for (i <- 0 until numTimes) {
        val (sum, sum_df_t, sum_ex_t) = sumDirect2(snc, "ArrDelay", isGroupBy)
        nsum += sum
        nsum_df_t += sum_df_t
        nsum_ex_t += sum_ex_t
      }
      // scalastyle:off println
      println(s"Sum : ${nsum / numTimes} df_creation_time=${nsum_df_t / numTimes} " +
          s"execution_time=${nsum_ex_t / numTimes} " +
          s"total-time=${(nsum_df_t + nsum_ex_t) / numTimes} : $numTimes times :" +
          s" Direct on sample table (no estimation)")
      // scalastyle:on println
    }
  }

  def printSampleCountWOE(snc: SnappyContext, numTimes: Int,
      isGroupBy: Boolean): Unit = {
    if (numTimes == 1) {
      val (count, count_df_t, count_ex_t) = countWOE(snc, "ArrDelay", isGroupBy)
      // scalastyle:off println
      println(s"Count : $count df_creation_time=$count_df_t " +
          s"execution_time=$count_ex_t " +
          s"total-time=${count_df_t + count_ex_t} : WOE")
      // scalastyle:on println
    } else if (numTimes > 1) {
      var ncount: Long = 0
      var ncount_df_t: Long = 0
      var ncount_ex_t: Long = 0
      for (i <- 0 until numTimes) {
        val (count, count_df_t, count_ex_t) = countWOE(snc, "ArrDelay", isGroupBy)
        ncount += count
        ncount_df_t += count_df_t
        ncount_ex_t += count_ex_t
      }
      // scalastyle:off println
      println(s"Count : ${ncount / numTimes} df_creation_time=${ncount_df_t / numTimes} " +
          s"execution_time=${ncount_ex_t / numTimes} " +
          s"total-time=${(ncount_df_t + ncount_ex_t) / numTimes} : $numTimes times : WOE")
      // scalastyle:on println
    }
  }

  def printSampleSumWOE(snc: SnappyContext, numTimes: Int,
      isGroupBy: Boolean): Unit = {
    if (numTimes == 1) {
      val (sum, sum_df_t, sum_ex_t) = sumWOE(snc, "ArrDelay", isGroupBy)
      // scalastyle:off println
      println(s"Sum : $sum df_creation_time=$sum_df_t execution_time=$sum_ex_t " +
          s"total-time=${sum_df_t + sum_ex_t} : WOE")
      // scalastyle:on println
    } else if (numTimes > 1) {
      var nsum: Long = 0
      var nsum_df_t: Long = 0
      var nsum_ex_t: Long = 0
      for (i <- 0 until numTimes) {
        val (sum, sum_df_t, sum_ex_t) = sumWOE(snc, "ArrDelay", isGroupBy)
        nsum += sum
        nsum_df_t += sum_df_t
        nsum_ex_t += sum_ex_t
      }
      // scalastyle:off println
      println(s"Sum : ${nsum / numTimes} df_creation_time=${nsum_df_t / numTimes} " +
          s"execution_time=${nsum_ex_t / numTimes} " +
          s"total-time=${(nsum_df_t + nsum_ex_t) / numTimes} : $numTimes times : WOE")
      // scalastyle:on println
    }
  }

  def printSampleCountBase(snc: SnappyContext, numTimes: Int,
      isGroupBy: Boolean): Unit = {
    if (numTimes == 1) {
      val (count, count_df_t, count_ex_t) = countBase(snc, "ArrDelay", isGroupBy)
      // scalastyle:off println
      println(s"Count : $count df_creation_time=$count_df_t " +
          s"execution_time=$count_ex_t " +
          s"total-time=${count_df_t + count_ex_t} : Base")
      // scalastyle:on println
    } else if (numTimes > 1) {
      var ncount: Long = 0
      var ncount_df_t: Long = 0
      var ncount_ex_t: Long = 0
      for (i <- 0 until numTimes) {
        val (count, count_df_t, count_ex_t) = countBase(snc, "ArrDelay", isGroupBy)
        ncount += count
        ncount_df_t += count_df_t
        ncount_ex_t += count_ex_t
      }
      // scalastyle:off println
      println(s"Count : ${ncount / numTimes} df_creation_time=${ncount_df_t / numTimes} " +
          s"execution_time=${ncount_ex_t / numTimes} " +
          s"total-time=${(ncount_df_t + ncount_ex_t) / numTimes} : $numTimes times : Base")
      // scalastyle:on println
    }
  }

  def printSampleSumBase(snc: SnappyContext, numTimes: Int,
      isGroupBy: Boolean): Unit = {
    if (numTimes == 1) {
      val (sum, sum_df_t, sum_ex_t) = sumBase(snc, "ArrDelay", isGroupBy)
      // scalastyle:off println
      println(s"Sum : $sum df_creation_time=$sum_df_t execution_time=$sum_ex_t " +
          s"total-time=${sum_df_t + sum_ex_t} : Base")
      // scalastyle:on println
    } else if (numTimes > 1) {
      var nsum: Long = 0
      var nsum_df_t: Long = 0
      var nsum_ex_t: Long = 0
      for (i <- 0 until numTimes) {
        val (sum, sum_df_t, sum_ex_t) = sumBase(snc, "ArrDelay", isGroupBy)
        nsum += sum
        nsum_df_t += sum_df_t
        nsum_ex_t += sum_ex_t
      }
      // scalastyle:off println
      println(s"Sum : ${nsum / numTimes} df_creation_time=${nsum_df_t / numTimes} " +
          s"execution_time=${nsum_ex_t / numTimes} " +
          s"total-time=${(nsum_df_t + nsum_ex_t) / numTimes} : $numTimes times : Base")
      // scalastyle:on println
    }
  }

  private def printDouble(des: String, value: Double): Boolean = {
    // scalastyle:off println
    println(des + " = %.5f".format(value))
    // scalastyle:on println
    false
  }

  def doQuery(snc: SnappyContext, doGroupBy: Boolean): Unit = {
    // scalastyle:off println
    println(s"Count: (isGroupBy=$doGroupBy)")
    // scalastyle:on println
    printSampleCountWOE(snc, 1, doGroupBy)
    printSampleCountWOE(snc, 1, doGroupBy)
    printSampleCountWOE(snc, 10, doGroupBy)
    printSampleCountWOE(snc, 10, doGroupBy)
    // scalastyle:off println
    println()
    // scalastyle:on println
    printSampleCountDirect2(snc, 1, doGroupBy)
    printSampleCountDirect2(snc, 1, doGroupBy)
    printSampleCountDirect2(snc, 10, doGroupBy)
    printSampleCountDirect2(snc, 10, doGroupBy)
    // scalastyle:off println
    println()
    // scalastyle:on println
    printSampleCountDirect(snc, 1, doGroupBy)
    printSampleCountDirect(snc, 1, doGroupBy)
    printSampleCountDirect(snc, 10, doGroupBy)
    printSampleCountDirect(snc, 10, doGroupBy)
    // scalastyle:off println
    println()
    // scalastyle:on println
    printSampleCount(snc, 1, doGroupBy)
    printSampleCount(snc, 1, doGroupBy)
    printSampleCount(snc, 10, doGroupBy)
    printSampleCount(snc, 10, doGroupBy)
    // scalastyle:off println
    println()
    // scalastyle:on println
    // printSampleCountBase(snc)

    // scalastyle:off println
    println(s"Sum: (isGroupBy=$doGroupBy)")
    // scalastyle:on println
    printSampleSumWOE(snc, 1, doGroupBy)
    printSampleSumWOE(snc, 1, doGroupBy)
    printSampleSumWOE(snc, 10, doGroupBy)
    printSampleSumWOE(snc, 10, doGroupBy)
    // scalastyle:off println
    println()
    // scalastyle:on println
    printSampleSumDirect2(snc, 1, doGroupBy)
    printSampleSumDirect2(snc, 1, doGroupBy)
    printSampleSumDirect2(snc, 10, doGroupBy)
    printSampleSumDirect2(snc, 10, doGroupBy)
    // scalastyle:off println
    println()
    // scalastyle:on println
    printSampleSumDirect(snc, 1, doGroupBy)
    printSampleSumDirect(snc, 1, doGroupBy)
    printSampleSumDirect(snc, 10, doGroupBy)
    printSampleSumDirect(snc, 10, doGroupBy)
    // scalastyle:off println
    println()
    // scalastyle:on println
    printSampleSum(snc, 1, doGroupBy)
    printSampleSum(snc, 1, doGroupBy)
    printSampleSum(snc, 10, doGroupBy)
    printSampleSum(snc, 10, doGroupBy)
    // scalastyle:off println
    println()
    // scalastyle:on println
    // printSampleSumBase(snc)
  }

  def testAQP79(): Unit = {
    if (disabled) return

    val snc = SnappyContext(sc)

    // val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    // vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    // val conn = getANetConnection(netPort1)
    // val s = conn.createStatement()

    setupData(snc: SnappyContext)
    doQuery(snc: SnappyContext, false)
    doQuery(snc: SnappyContext, true)

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

    // Thread.sleep(10000000)

    dropData(snc)
    // scalastyle:off println
    println(s"Done ")
    // scalastyle:on println
  }
}
