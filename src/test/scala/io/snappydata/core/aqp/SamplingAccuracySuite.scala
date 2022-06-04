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

import com.gemstone.gemfire.distributed.internal.ReplyException
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.{Constant, Property, SnappyFunSuite}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.sql.{DataFrame, SaveMode}

class SamplingAccuracySuite
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

  var qcsColumns: String = "UniqueCarrier"
  // Same as QCS
  var qcs: String = "START"
  var currQCS: String = ""
  val strataReserviorSize = 50
  // TODO Change as needed
  val sampleRatio: Double = 0.03

  var airlineDataFrame: DataFrame = _

  override def beforeAll(): Unit = {
    System.setProperty(Constant.defaultBehaviorAsDO_NOTHING, "true")
    System.setProperty(Constant.RESERVOIR_AS_REGION, "true")
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    setupData()
  }

  override def afterAll(): Unit = {
    val snc = this.snc
    dropData()
    // cleanup metastore
    if (snc != null) {
      snc.clearCache()
    }
    super.afterAll()
    stopAll()
    System.setProperty(Constant.RESERVOIR_AS_REGION, "true")
    System.setProperty(Constant.defaultBehaviorAsDO_NOTHING, "false")
  }

  val dumpBuckets = false

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

  def columnBatchTableName(table: String): String = {
    ColumnFormatRelation.columnBatchTableName("APP." + table)
  }

  def dropData(): Unit = {
    snc.sql(s"drop table if exists airlineSampled")
    snc.sql(s"drop table if exists airline")
  }

  def setupData(): Unit = {
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val snContext = this.snc
    snContext.sql("set spark.sql.shuffle.partitions=6")

    snc.sql(s"drop table if exists airlineSampled")
    snc.sql(s"drop table if exists airline")

    airlineDataFrame = snContext.read.load(hfile)
    airlineDataFrame.registerTempTable("airline")

    /**
      * Total number of executors (or partitions) = Total number of buckets +
      * Total number of reservoir
      */
    /**
      * Partition by influences whether a strata is handled in totality on a
      * single executor or not, and thus an important factor.
      */
    val start1 = System.currentTimeMillis
    snc.sql(s"CREATE TABLE airlineSampled $ddlStr" +
        "USING column_sample " +
        "options " +
        "(" +
        s"qcs '$qcsColumns'," +
        "BUCKETS '8'," +
        s"fraction '$sampleRatio'," +
        s"strataReservoirSize '$strataReserviorSize', " +
        "baseTable 'airline')")

    val start2 = System.currentTimeMillis
   /* airlineDataFrame.write.
        format("column_sample")
        .mode(SaveMode.Append)
        .saveAsTable("airlineSampled") */

   // val start3 = System.currentTimeMillis
    logInfo(s"Time sample ddl=${start2-start1}ms ")
  }

  private def countBase(aggCol: String): Long = {
    val query: String =
      s"SELECT COUNT($aggCol) as COUNT FROM airline"
    val count_result_direct_on_sample_table = snc.sql(query)
    count_result_direct_on_sample_table.collect()(0).getLong(0)
  }

  private def countDirect(aggCol: String): Long = {
    val query: String =
      s"SELECT COUNT($aggCol) as COUNT FROM airline with error"
    val count_result_direct_on_sample_table = snc.sql(query)
    count_result_direct_on_sample_table.collect()(0).getLong(0)
  }

  private def countSample(aggCol: String): Long = {
    val query: String =
      s"SELECT COUNT($aggCol) as SAMPLE_COUNT FROM airline with error"
    val count_result_direct_on_sample_table = snc.sql(query)
    count_result_direct_on_sample_table.collect()(0).getLong(0)
  }

  private def sumBase(aggCol: String): Long = {
    val query: String =
      s"SELECT sum($aggCol) as SUM FROM airline"
    val sum_result_direct_on_sample_table = snc.sql(query)
    sum_result_direct_on_sample_table.collect()(0).getLong(0)
  }

  private def sumDirect(aggCol: String): Long = {
    val query: String =
      s"SELECT sum($aggCol) as SUM FROM airline with error"
    val sum_result_direct_on_sample_table = snc.sql(query)
    sum_result_direct_on_sample_table.collect()(0).getLong(0)
  }

  private def printDouble(des: String, value: Double): Boolean = {
    logInfo(des + " = %.5f".format(value))
    false
  }

  /**
    * See variables set above before adding another test in this file
    */
  test("AQP-79") {
    val n = 1
    if (dumpBuckets) {
      sc.setLogLevel("INfo")
    }

    logInfo(s"AQP-79-$n :")

    val countB = countBase("ArrDelay")
    val sumB = sumBase("ArrDelay")
    logInfo(s"Base - Count : $countB Sum: $sumB")

    val count_sample = countSample("ArrDelay")
    val count = countDirect("ArrDelay") // ~1888622
    val sum = sumDirect("ArrDelay") // ~(9537150 - 10285273)
    logInfo(s"Sample - Count : $count Sum: $sum Sample-count: $count_sample")

    // assert
    assert((countB - count).abs < 2)

    if (false) {
      val dump_sampled_table1 = snc.sql(
        s"SELECT SNAPPY_SAMPLER_WEIGHTAGE, count(ArrDelay) FROM airlineSampled " +
            s"GROUP BY SNAPPY_SAMPLER_WEIGHTAGE")
      val samstream1 = new ByteArrayOutputStream()
      Console.withOut(new java.io.PrintStream(samstream1))(dump_sampled_table1.show(99999))
      logWarning("Sample1-start: ")
      logWarning(samstream1.toString)
      logWarning("Sample1-end: ")
    }

    if (dumpBuckets) {
      val resolvedBaseName = "APP.AIRLINESAMPLED"
      val regionBaseRow = Misc.getRegionForTable(resolvedBaseName, false)
      if (regionBaseRow != null) {
        val ppr: PartitionedRegion = regionBaseRow.asInstanceOf[PartitionedRegion]
        try {
          // Misc.getCacheLogWriter.convertToLogWriterI18n
          ppr.dumpAllBuckets(false, null)
        }
        catch {
          case re: ReplyException => {
            fail("dumpAllBuckets", re)
          }
        }
      }

      val columnBatchTable = columnBatchTableName("AIRLINESAMPLED")
      val regionBase = Misc.getRegionForTable(columnBatchTable, false)
      if (regionBase != null) {
        val ppr: PartitionedRegion = regionBase.asInstanceOf[PartitionedRegion]
        try {
          // Misc.getCacheLogWriter.convertToLogWriterI18n
          ppr.dumpAllBuckets(false, null)
        }
        catch {
          case re: ReplyException => {
            fail("dumpAllBuckets", re)
          }
        }
      }

      val reservoirName = Misc.getReservoirRegionNameForSampleTable(Constant.DEFAULT_SCHEMA,
        resolvedBaseName)
      val childRegion = Misc.createReservoirRegionForSampleTable(reservoirName, resolvedBaseName)
      if (childRegion != null) {
        val ppr: PartitionedRegion = childRegion.asInstanceOf[PartitionedRegion]
        try {
          // Misc.getCacheLogWriter.convertToLogWriterI18n
          ppr.dumpAllBuckets(false, null)
        }
        catch {
          case re: ReplyException => {
            fail("dumpAllBuckets", re)
          }
        }
      }
    }

    logInfo(s"Done ")
  }
}
