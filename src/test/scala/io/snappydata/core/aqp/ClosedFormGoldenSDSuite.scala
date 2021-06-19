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

import sys.process._
import java.io.{DataInputStream, DataOutputStream, FileInputStream, FileOutputStream, InputStream}

import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

class ClosedFormGoldenSDSuite
  extends SnappyFunSuite
  with BeforeAndAfterAll {

  private val disabled = true
  val ddlStr: String = "(YearI INT," + // NOT NULL
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

  var isSingleTest = true
  var results_sampled_table : DataFrame = _
  var airlineDataFrame : DataFrame = _
  val strataReserviorSize = 50
  // TODO Change as needed
  val sampleRatio : Double = 0.03
  // Also Set these as appropriate
  val qcsColumns : String = "UniqueCarrier"   // Same as QCS
  var aggCol : String = "ArrDelay"
  val aggColCnt: String = "1"
 // var countFlag:Boolean = false

  // TODO Setting for where condition
  val additionalCols : String = " ,MonthI ,YearI "
  val colPosMonth : Int = 1
  val colPosYear : Int = 2

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
     * Pls do not change the flag values of Property.TestDisableCodeGenFlag.name
     * and Property.UseOptimizedHashAggregateForSingleKey.name
     * They are meant to suppress CodegenFallback Plan so that optimized
     * byte buffer code path is tested & prevented from false passing.
     * If your test needs CodegenFallback, then override the newConf function
     * & clear the flag from the conf of the test locally.
     */
    new org.apache.spark.SparkConf().setAppName("ClosedFormEstimates")
        .setMaster("local[4]")
        .set("spark.logConf", "true")
        .set("spark.sql.shuffle.partitions", "6")
        .set(io.snappydata.Property.NumBootStrapTrials.name, "100")
        .set(io.snappydata.Property.ClosedFormEstimates.name, "false")
        .set(Property.TestDisableCodeGenFlag.name , "true")
        .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")
  }

  def setupData(): Unit = {
    if (disabled) return
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val snContext = this.snc
    // snContext.sql("set spark.sql.shuffle.partitions=6")

    airlineDataFrame = snContext.read.load(hfile)
    airlineDataFrame.persist(StorageLevel.MEMORY_ONLY)
    airlineDataFrame.registerTempTable("airline")

    snc.sql(s"CREATE TABLE airlineSampled $ddlStr" +
        "USING column_sample " +
        "options " +
        "(" +
        s"qcs '$qcsColumns'," +
        s"fraction '$sampleRatio'," +
        s"strataReservoirSize '$strataReserviorSize', " +
        "baseTable 'airline')")
  }

  def writeSampleTable(): Unit = {
    airlineDataFrame.write.insertInto("airlineSampled")
  }

  def deleteSampleTable(): Unit = {
    snc.sql(s"TRUNCATE TABLE airlineSampled")
  }

  def noFilter(t: Row, pos: Integer): Boolean = {
    false
  }

  def filterOnMonth(t: Row, pos: Integer): Boolean = {
    if (t.getInt(pos + colPosMonth) < 3) false else true
  }

  def filterOnYear(t: Row, pos: Integer): Boolean = {
    if (t.getInt(pos + colPosYear) == 2015) false else true
  }

  def setupCounters(total: Array[Double], cnt: Array[Integer], wtCnt: Array[Double],
      filterRow: (Row, Integer) => Boolean, aggColumn: String): Unit = {
    val arr: Array[String] = qcsColumns.split(",")
    val pos: Integer = arr.length + 1

   // println("Aggcol value is "+aggColumn)

    results_sampled_table = snc.sql(
      s"SELECT $aggColumn, $qcsColumns, SNAPPY_SAMPLER_WEIGHTAGE $additionalCols" +
          s" FROM airlineSampled ORDER BY $qcsColumns, SNAPPY_SAMPLER_WEIGHTAGE")

    var i: Integer = 0
    var l: Integer = 1
    var prevWt: Long = 0
    var qcs: String = "START"
    var currQCS: String = ""
    results_sampled_table.collect().foreach(t => {
      arr.foreach(a => {
        currQCS += t.get(l).toString; l += 1
      })
      l = 1
      var rowValue = t.getInt(0)
      if (filterRow(t, pos)) {
        rowValue = 0
      }

      if (qcs.equals("START")) {
        total(i) = rowValue
        cnt(i) = 1
        wtCnt(i) = getWtCount(t.getLong(pos))
        prevWt = t.getLong(pos)
        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1
      }
      else if (qcs == "" || qcs.equals(currQCS)
          && (prevWt == getWtCount(t.getLong(pos)))) {
        cnt(i) = cnt(i) + 1
        wtCnt(i) = wtCnt(i) + getWtCount(t.getLong(pos))
        total(i) = total(i) + rowValue
        prevWt = t.getLong(pos)
      }
      else {
        i = i + 1
        // carr = t.getString(1)
        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1
        cnt(i) = 1
        wtCnt(i) = getWtCount(t.getLong(pos))
        total(i) = rowValue
        prevWt = t.getLong(pos)
      }
      currQCS = ""
    })
  }

  private def calculateMean(meanArray: Array[Double], numLoop: Int): Unit = {
    // Calculate Mean
    var mean: Double = 0
    for (i <- 0 to numLoop - 1) {
      mean = mean + meanArray(i)
    }
    mean = mean / numLoop

    // Calculate SD
    var Variance: Double = 0
    for (i <- 0 to numLoop - 1) {
      Variance = Variance + (meanArray(i) - mean) * (meanArray(i) - mean)
    }
    Variance = Variance / numLoop
    val SD = math.sqrt(Variance)

    // scalastyle:off println
    println()
    println(s"SD for mean queries with sample ratio $sampleRatio for Loop($numLoop) = "
        + "%.5f".format(SD))
    // scalastyle:on println
  }

  private def meanOneLoop(index : Int , meanArray : Array[Double]) : Unit = {
    val total = new Array[Double](1000)
    val cnt = new Array[Integer](1000)
    val wtCnt = new Array[Double](1000)
    setupCounters(total, cnt, wtCnt, noFilter, aggCol)
    meanArray(index) = meanTest(total, cnt, wtCnt, index)
  }

  private def calculateSD(sumArray: Array[Double], numLoop: Int, whereCondition: String,
      msg: String): Unit = {
    // Calculate Mean
    var mean: Double = 0
    for (i <- 0 to numLoop - 1) {
      mean = mean + sumArray(i)
    }
    mean = mean / numLoop

    // Calculate SD
    var Variance: Double = 0
    for (i <- 0 to numLoop - 1) {
      Variance = Variance + (sumArray(i) - mean) * (sumArray(i) - mean)
    }
    Variance = Variance / numLoop
    val SD = math.sqrt(Variance)

    // scalastyle:off println
    println()
    println(s"$msg ($whereCondition) and ($sampleRatio) for Loop($numLoop)= "
        + "%.5f".format(SD))
    // scalastyle:off println
  }

  private def sumConditionalOneLoop(index: Int, sumArray: Array[Double],
      filterRow: (Row, Integer) => Boolean, whereCondition: String): Unit = {
    val total = new Array[Double](1000)
    val cnt = new Array[Integer](1000)
    val wtCnt = new Array[Double](1000)
    setupCounters(total, cnt, wtCnt, filterRow, aggCol)
    sumArray(index) = conditionalSumTest(total, cnt, wtCnt, index, whereCondition)
  }

  private def countConditionalOneLoop(index: Int, countArray: Array[Double],
      filterRow: (Row, Integer) => Boolean, whereCondition: String): Unit = {
    val total = new Array[Double](1000)
    val cnt = new Array[Integer](1000)
    val wtCnt = new Array[Double](1000)
    setupCounters(total, cnt, wtCnt, filterRow, aggColCnt)
    countArray(index) = conditionalCountTest(total, cnt, wtCnt, index, whereCondition)
  }

  private def meanTest(total: Array[Double], cnt: Array[Integer], wtCnt: Array[Double],
      index: Integer): Double = {
    var sumFactor: Double = 0
    var l = 0
    cnt.foreach(c => {
      if (c != null) {
        sumFactor += (total(l) / c) * wtCnt(l)
      }
      l = l + 1
    })
    var population: Double = 0
    wtCnt.foreach(t => population += t)
    val meanFactor = sumFactor / population

    if (doPrint(index)) {
      // scalastyle:off println
      println()
      println(s"===== MEAN($index) RESULTS FROM NEW FORMULAS =======")
      println("Mean->>>>>> " + "%.5f".format(meanFactor))
      // scalastyle:on println
    }

    if (doPrint(index)) {
      val sampleQuery: String =
        s"SELECT AVG($aggCol) as AVG FROM airlineSampled"
      val mean_results_sample_table = snc.sql(sampleQuery)
      // scalastyle:off println
      println()
      println(s"======== AVG QUERY ON Sample TABLE with old formula =======")
      println(sampleQuery)
      // scalastyle:on println
      mean_results_sample_table.show()
    }

    meanFactor
  }

  private def conditionalSumTest(total: Array[Double], cnt: Array[Integer], wtCnt: Array[Double],
      index: Integer, whereCondition: String): Double = {
    val doDetailedPrint = doPrint(index) && isSingleTest

    if (doDetailedPrint) {
      // scalastyle:off println
      println()
      // results_sampled_table.show()
      println(s"====== SUM($index) for $whereCondition STARTS ===============")
      println(s"====== SUM STATS ON SAMPLE TABLE $whereCondition===============")
      println("Cnt->>>>>>" + cnt.toList) // ni
      println("Total->>>>>>" + total.toList) // Sum-i
      println("WtCnt->>>>>>" + wtCnt.toList) // Ni
      // scalastyle:on println
    }

    var sumFactor: Double = 0
    var l = 0
    cnt.foreach(c => {
      if (c != null) {
        sumFactor += (total(l) / c) * wtCnt(l)
      }
      l = l + 1
    })

    if (doPrint(index)) {
      // scalastyle:off println
      println()
      println(s"==== SUM($index): Results from new formulas $whereCondition===============")
      println("Sum->>>>>> " + "%.5f".format(sumFactor))
      // scalastyle:on println
    }

    if (doDetailedPrint) {
      val query: String = s"SELECT SUM($aggCol) as SUM FROM airline where $whereCondition"
      val results_population_table1 = snc.sql(query)
      // scalastyle:off println
      println()
      println(s"======= DIRECT SUM QUERY ON POPULATION TABLE $whereCondition ========")
      println(query)
      // scalastyle:on println
      results_population_table1.show()
    }

    if (doPrint(index)) {
      val sampleQuery: String =
        s"SELECT SUM($aggCol) as SUM FROM airlineSampled where $whereCondition"
      val new_results_sample_table2 = snc.sql(sampleQuery)
      // scalastyle:off println
      println()
      println(s"======== SUM QUERY ON Sample TABLE with old formula $whereCondition =======")
      println(sampleQuery)
      // scalastyle:on println
      new_results_sample_table2.show()
    }

    if (doDetailedPrint) {
      val query: String = s"SELECT count($aggCol) as COUNT FROM airline"
      val results_population_table3 = snc.sql(query)
      // scalastyle:off println
      println()
      println(query)
      results_population_table3.show()
      // scalastyle:on println
    }

    if (doDetailedPrint) {
      val query: String = s"SELECT count($aggCol) as COUNT FROM airline where $whereCondition"
      val results_population_table4 = snc.sql(query)
      // scalastyle:off println
      println()
      println(query)
      results_population_table4.show()
      // scalastyle:on println
    }

    if (doDetailedPrint) {
      val query: String = s"SELECT $aggCol FROM airlineSampled"
      val results_sample_table5 = snc.sql(query)
      // scalastyle:off println
      println()
      println(query)
      var count: Integer = 0
      results_sample_table5.collect().foreach(t => count = count + 1)
      println("Count: " + count)
      // scalastyle:on println
    }

    if (doDetailedPrint) {
      val query: String = s"SELECT $aggCol FROM airlineSampled where $whereCondition"
      val results_sample_table6 = snc.sql(query)
      // scalastyle:off println
      println()
      println(query)
      var count: Integer = 0
      results_sample_table6.collect().foreach(t => count = count + 1)
      println("Count: " + count)
      // scalastyle:on println
    }

    if (doDetailedPrint) {
      // scalastyle:off println
      println()
      println(s"====== SUM($index) for $whereCondition DONE ===============")
      // scalastyle:off println
    }

    sumFactor
  }

  private def conditionalCountTest(total: Array[Double], cnt: Array[Integer], wtCnt: Array[Double],
                                   index: Integer, whereCondition: String): Double = {
    val doDetailedPrint = doPrint(index) && isSingleTest

    var count : Double = 0
    var i = 0
    cnt.foreach(w => {
      if (w != null && w != 0) {
        count += total(i) * wtCnt(i) / cnt(i)
      }
      i = i + 1
    })

    if (doPrint(index)) {
      // scalastyle:off println
      println()
      println(s"==== COUNT($index): Results from new formulas $whereCondition===============")
      println("COUNT->>>>>> " + "%.5f".format(count))
      // scalastyle:on println
    }

    if (doDetailedPrint) {
      val query1: String = s"SELECT count($aggColCnt) as COUNT FROM airline"
      val results_population_table = snc.sql(query1)
      // scalastyle:off println
      println()
      println(query1)
      results_population_table.show()
    }

    if (doDetailedPrint) {
      // scalastyle:off println
      println()
      println(s"====== COUNT($index) for $whereCondition DONE ===============")
      // scalastyle:off println
    }
    count
  }


    ignore("test for mean queries") {
    val numLoop : Integer = 10000 // 10
    val meanArray = new Array[Double](numLoop)

    // scalastyle:off println
    println()
    print("Average = ")
    // scalastyle:on println
    for (i <- 0 to numLoop-1) {
      writeSampleTable()
      meanOneLoop(i, meanArray)
      deleteSampleTable()

      if (i == 5 || i == 10 || i % 1000 == 0) {
        calculateMean(meanArray, numLoop)
      }
    }
    calculateMean(meanArray, numLoop)
    // scalastyle:off println
    println("Success...")
    // scalastyle:on println
  }

  ignore("test for conditional sum queries on Year") {
    val numLoop : Integer = 10000 // 10
    val sumArrayOnYear = new Array[Double](numLoop)
    val whereConditionOnYear : String = s" YearI = 2015 "

    // scalastyle:off println
    println()
    print("Sum = ")
    // scalastyle:on println
    for (i <- 0 to numLoop-1) {
      writeSampleTable()
      sumConditionalOneLoop(i, sumArrayOnYear, filterOnYear, whereConditionOnYear)
      deleteSampleTable()

      if (i == 5 || i == 10 || i % 1000 == 0) {
        calculateSD(sumArrayOnYear, numLoop, whereConditionOnYear, "SD for sum")
      }
    }
    calculateSD(sumArrayOnYear, numLoop, whereConditionOnYear, "SD for sum")

    // scalastyle:off println
    println("Success...")
    // scalastyle:on println
  }

  ignore("test for conditional sum queries on Month") {
    val numLoop : Integer = 10000 // 10
    val sumArrayOnMonth = new Array[Double](numLoop)
    val whereConditionOnMonth : String = s" MonthI < 3 "

    // scalastyle:off println
    println()
    print("Sum = ")
    // scalastyle:on println
    for (i <- 0 to numLoop-1) {
      writeSampleTable()
      sumConditionalOneLoop(i, sumArrayOnMonth, filterOnMonth, whereConditionOnMonth)
      deleteSampleTable()

      if (i == 10 || i % 1000 == 0) {
        calculateSD(sumArrayOnMonth, numLoop, whereConditionOnMonth, "SD for sum")
      }
    }
    calculateSD(sumArrayOnMonth, numLoop, whereConditionOnMonth, "SD for sum")

    // scalastyle:off println
    println("Success...")
    // scalastyle:on println
  }

  ignore("common test") {
    isSingleTest = false
    val numLoop: Integer = 10000

    val sumArrayOnYear = new Array[Double](numLoop)
    val meanArray = new Array[Double](numLoop)
    val sumArrayOnMonth = new Array[Double](numLoop)
    val countArrayOnMonth = new Array[Double](numLoop)
    val countArrayOnYear = new Array[Double](numLoop)


    val whereConditionOnYear: String = s" YearI = 2015 "
    val whereConditionOnMonth: String = s" MonthI < 3 "

    for (i <- 0 to numLoop - 1) {
      writeSampleTable()
      sumConditionalOneLoop(i, sumArrayOnYear, filterOnYear, whereConditionOnYear)
      meanOneLoop(i, meanArray)
      sumConditionalOneLoop(i, sumArrayOnMonth, filterOnMonth, whereConditionOnMonth)
      countConditionalOneLoop(i, countArrayOnMonth, filterOnMonth, whereConditionOnMonth)
      countConditionalOneLoop(i, countArrayOnYear, filterOnYear, whereConditionOnYear)

      if (i > 0 && (i == 5 || i == 10 || (i % 100 == 0))) {
        calculateSD(sumArrayOnYear, i, whereConditionOnYear, "SD for sum")
        calculateMean(meanArray, i)
        calculateSD(sumArrayOnMonth, i, whereConditionOnMonth, "SD for sum")
        calculateSD(countArrayOnMonth, i, whereConditionOnMonth, "SD for COUNT")
        calculateSD(countArrayOnYear, i, whereConditionOnYear, "SD for COUNT")
      }
      deleteSampleTable()
    }
    // scalastyle:off println
    println("Final Numbers...")
    // scalastyle:on println
    calculateSD(sumArrayOnYear, numLoop, whereConditionOnYear, "SD for sum")
    calculateMean(meanArray, numLoop)
    calculateSD(sumArrayOnMonth, numLoop, whereConditionOnMonth, "SD for sum")
    calculateSD(countArrayOnMonth, numLoop, whereConditionOnMonth, "SD for COUNT")
    calculateSD(countArrayOnYear, numLoop, whereConditionOnYear, "SD for COUNT")

    // scalastyle:off println
    println("Success...")
    // scalastyle:on println
    isSingleTest = true
  }

  ignore("sanity test") {
    this.writeSampleTable()
    val sumQueryMainTable: String = s"SELECT SUM($aggCol) as summ FROM airline "
    val avgQueryMainTable: String = s"SELECT AVG($aggCol) as avgg FROM airline "
    val sumMainRS = snc.sql(sumQueryMainTable)
    val sumMain = sumMainRS.collect()(0).getLong(0)
    val avgMainRS = snc.sql(avgQueryMainTable)
    val avgMain = avgMainRS.collect()(0).getDouble(0)

    val sumQuerySampleTable: String = s"SELECT SUM($aggCol) as summ FROM airline confidence .90"
    val avgQuerySampleTable: String = s"SELECT AVG($aggCol) as avgg FROM airline confidence .90 "


    val sumSampleRS = snc.sql(sumQuerySampleTable)
    val sumSample = sumSampleRS.collect()(0).getDouble(0)
    val avgSampleRS = snc.sql(avgQuerySampleTable)
    val avgSample = avgSampleRS.collect()(0).getDouble(0)

    // scalastyle:off println
    println(" main table sum = " + sumMain)
    println(" sample table sum = " + sumSample)
    println(" main table avg = " + avgMain)
    println(" sample table avg = " + avgSample)
    // scalastyle:on println

    this.deleteSampleTable()
  }


  def directMeanFromSampleTable() : Any = {
    val sampleQuery: String =
      s"SELECT AVG($aggCol) FROM airlineSampled"
    val meandirect_results_sampled_table = snc.sql(sampleQuery)
//    // scalastyle:off println
//    println()
//    println(sampleQuery)
//    // scalastyle:on println
//    results_sampled_table.show()
    meandirect_results_sampled_table.first().get(0)
  }

  def directMeanFromSampleTable(whereCondition : String) : Any = {
    val sampleQuery: String =
      s"SELECT AVG($aggCol) FROM airlineSampled where $whereCondition"
    val meandirect_results_sampled_table = snc.sql(sampleQuery)
    //    // scalastyle:off println
    //    println()
    //    println(sampleQuery)
    //    // scalastyle:on println
    //    results_sampled_table.show()
    meandirect_results_sampled_table.first().get(0)
  }

  def directSumFromSampleTable(whereCondition : String) : Any = {
    val sampleQuery: String =
      s"SELECT SUM($aggCol) FROM airlineSampled where $whereCondition"
    val sumdirect_results_sampled_table = snc.sql(sampleQuery)
//    // scalastyle:off println
//    println()
//    println(sampleQuery)
//    // scalastyle:on println
//    results_sampled_table.show()
    sumdirect_results_sampled_table.first().get(0)
  }

  def directCountFromSampleTable(whereCondition : String) : Any = {
    val sampleQuery: String =
      s"SELECT COUNT($aggCol) FROM airlineSampled where $whereCondition"
    val countdirect_results_sampled_table = snc.sql(sampleQuery)
    //    // scalastyle:off println
    //    println()
    //    println(sampleQuery)
    //    // scalastyle:on println
    //    results_sampled_table.show()
    countdirect_results_sampled_table.first().get(0)
  }

  ignore("another test") {
    isSingleTest = false
    val numLoop: Integer = 1000

    val newSumArrayOnYear = new Array[Double](numLoop)
    val newMeanArray = new Array[Double](numLoop)
    val newSumArrayOnMonth = new Array[Double](numLoop)

    val oldSumArrayOnYear = new Array[Any](numLoop)
    val oldMeanArray = new Array[Any](numLoop)
    val oldSumArrayOnMonth = new Array[Any](numLoop)

    val whereConditionOnYear: String = s" YearI = 2015 "
    val whereConditionOnMonth: String = s" MonthI < 3 "

    for (i <- 0 to numLoop - 1) {
      writeSampleTable()
      sumConditionalOneLoop(i, newSumArrayOnYear, filterOnYear, whereConditionOnYear)
      oldSumArrayOnYear(i) = directSumFromSampleTable(whereConditionOnYear)
      meanOneLoop(i, newMeanArray)
      oldMeanArray(i) = directMeanFromSampleTable()
      sumConditionalOneLoop(i, newSumArrayOnMonth, filterOnMonth, whereConditionOnMonth)
      oldSumArrayOnMonth(i) = directSumFromSampleTable(whereConditionOnMonth)
      deleteSampleTable()
    }

    var newMeanOfmean : Double = 0
    newMeanArray.foreach(el => newMeanOfmean = newMeanOfmean + el)
    newMeanOfmean = newMeanOfmean /  numLoop

    var oldMeanOfmean : Double = 0
    oldMeanArray.foreach(el => oldMeanOfmean = oldMeanOfmean + toDouble(el))
    oldMeanOfmean = oldMeanOfmean /  numLoop

    var newSumOnYear : Double = 0
    newSumArrayOnYear.foreach(el => newSumOnYear = newSumOnYear + el)
    newSumOnYear = newSumOnYear /  numLoop

    var oldSumOnYear : Double = 0
    oldSumArrayOnYear.foreach(el => oldSumOnYear = oldSumOnYear + toDouble(el))
    oldSumOnYear = oldSumOnYear /  numLoop

    var newSumOnMonth : Double = 0
    newSumArrayOnMonth.foreach(el => newSumOnMonth = newSumOnMonth + el)
    newSumOnMonth = newSumOnMonth /  numLoop

    var oldSumOnMonth : Double = 0
    oldSumArrayOnMonth.foreach(el => oldSumOnMonth = oldSumOnMonth + toDouble(el))
    oldSumOnMonth = oldSumOnMonth /  numLoop

    // scalastyle:off println
    println("Values after 1000 iterations...")
    println("Mean->>>>>> " + "OLD %.5f".format(oldMeanOfmean) + " NEW %.5f".format(newMeanOfmean))
    println(s"SUM ($whereConditionOnYear)->>>>>> "
        + "OLD %.5f".format(oldSumOnYear) + " NEW %.5f".format(newSumOnYear))
    println(s"SUM ($whereConditionOnMonth)->>>>>> "
        + "OLD %.5f".format(oldSumOnMonth) + " NEW %.5f".format(newSumOnMonth))
    println("Success...")
    // scalastyle:on println
    isSingleTest = true
  }

  private def toDouble: (Any) => Double =
  { case i: Int => i case f: Float => f case d: Double => d case l: Long => l }

  private def toDouble2: (Any) => Double = {
    case i: Int => i
    case f: Float => (f * 100000.0).round / 100000.0
    case d: Double => (d * 100000.0).round / 100000.0
    case l: Long => l
  }

  private def getWtCount (cellValue : Long) : Double = {
    val Ni = ((cellValue >> 8) & 0xffffffffL).toDouble
    val ni = ((cellValue >> 40) & 0xffffffffL).toDouble
    Ni / ni
  }

  private def doPrint (index : Integer) : Boolean = {
    index > 0 && (index < 3 || index == 10 || index == 1000 || index == 9000)
  }

  ignore("common test to write file") {
    val numLoop: Integer = 10 // 000

    val rand = scala.util.Random.nextInt(99)
    // scalastyle:off println
    println(s"Write: Start... r$rand")
    // scalastyle:on println

    val userName = "whoami".!!
    // val pathName = "/home/" + userName.trim
    val pathName = "/users/" + userName.trim

    val fos = new Array[FileOutputStream](6)
    val dos = new Array[DataOutputStream](6)

    for (i <- 0 to 5) {
      fos(0) = null
      dos(0) = null
    }

    try {
      isSingleTest = false

      val whereConditionOnYear: String = s" YearI = 2015 "
      val whereConditionOnMonth: String = s" MonthI < 3 "
      val sampleNum = (sampleRatio * 100).toInt
      val fileName = Array(pathName + s"/SUM_Year_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/SUM_Month_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/Count_Year_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/Count_Month_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/Mean_Year_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/Mean_Month_s$sampleNum" + s"_r$rand.txt"
      )

      for (i <- 0 to 5) {
        fos(i) = new FileOutputStream(fileName(i))
        dos(i) = new DataOutputStream(fos(i))
      }

      for (i <- 0 to numLoop - 1) {
        writeSampleTable()
        val sumYear = directSumFromSampleTable(whereConditionOnYear)
        dos(0).writeDouble(toDouble2(sumYear))
        val sumMonth = directSumFromSampleTable(whereConditionOnMonth)
        dos(1).writeDouble(toDouble2(sumMonth))
        val countYear = directCountFromSampleTable(whereConditionOnYear)
        dos(2).writeDouble(toDouble2(countYear))
        val countMonth = directCountFromSampleTable(whereConditionOnMonth)
        dos(3).writeDouble(toDouble2(countMonth))
        val meanYear = directMeanFromSampleTable(whereConditionOnYear)
        dos(4).writeDouble(toDouble2(meanYear))
        val meanMonth = directMeanFromSampleTable(whereConditionOnMonth)
        dos(5).writeDouble(toDouble2(meanMonth))
        deleteSampleTable()
      }

      for (elem <- dos) elem.flush()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      for (elem <- dos) {if (elem != null) elem.close()}
      for (elem <- fos) {if (elem != null) elem.close()}
      isSingleTest = true
    }

    // scalastyle:off println
    println(s"Write: Done...r$rand")
    // scalastyle:on println
  }

  ignore("common test to read file") {
    val rand : Integer = 7 // r
    val sampleNum : Integer = 3 // s

    // scalastyle:off println
    println(s"Read: Start...r$rand")
    // scalastyle:on println

    val userName = "whoami".!!
    // val pathName = "/home/" + userName.trim
    val pathName = "/users/" + userName.trim

    val is = new Array[InputStream](6)
    val dis = new Array[DataInputStream](6)
    for (i <- 0 to 5) {
      is(0) = null
      dis(0) = null
    }

    try {
      val fileName = Array(pathName + s"/SUM_Year_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/SUM_Month_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/Count_Year_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/Count_Month_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/Mean_Year_s$sampleNum" + s"_r$rand.txt"
        , pathName + s"/Mean_Month_s$sampleNum" + s"_r$rand.txt"
      )
      for (i <- 0 to 5) {
        is(i) = new FileInputStream(fileName(i))
        dis(i) = new DataInputStream(is(i))
      }

      for (i <- 0 to 5) {
        // scalastyle:off println
        println
        print(s"Reading: " + fileName(i) + " : ")
        // scalastyle:on println
        while (dis(i).available() > 0) print(dis(i).readDouble().formatted("%.5f") + " ")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      for (elem <- is) {if (elem != null) elem.close()}
      for (elem <- dis) {if (elem != null) elem.close()}
    }

    // scalastyle:off println
    println
    print(s"Read: Done...r$rand")
    // scalastyle:on println
  }
}
