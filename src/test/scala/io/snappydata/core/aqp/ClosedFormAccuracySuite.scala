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

import scala.collection.mutable

import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.apache.commons.math3.distribution.NormalDistribution
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.util.StatCounter

class ClosedFormAccuracySuite
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
    airlineDataFrame.registerTempTable("airline")

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

  def reCreateSampleTable(): Unit = {
    snc.sql(s"CREATE SAMPLE TABLE airlineSampled " +
        " options " +
        " ( " +
        s"qcs '$qcsColumns'," +
        "BUCKETS '8'," +
        s"fraction '$sampleRatio'," +
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

  def noFilter(t: Row, pos: Integer): Boolean = {
    false
  }

  def filterOnMonth(t: Row, pos: Integer): Boolean = {
    if (t.getInt(pos) < 3) false else true
  }

  def filterOnYear(t: Row, pos: Integer): Boolean = {
    if (t.getInt(pos) == 2015) false else true
  }

  var pos : Integer = 0

  def setupCounters(aggCol: String, conditionalCol: String,
      filterRow: (Row, Integer) => Boolean): Int = {

    qcsColumns = "UniqueCarrier" // Same as QCS
    qcs = "START"
    currQCS = ""

    total = new Array[Double](1000)
    cnt = new Array[Integer](1000)
    wtCnt = new Array[Double](1000)

    val arr: Array[String] = qcsColumns.split(",")
    pos = arr.length + 1

    var additionalProj: String = ""
    val applyCondition: Boolean = conditionalCol.trim.length > 0
    if (applyCondition) {

      additionalProj = " , " + conditionalCol
    }

    results_sampled_table = snc.sql(
      s"SELECT $aggCol, $qcsColumns, SNAPPY_SAMPLER_WEIGHTAGE $additionalProj" +
          s" FROM airlineSampled ORDER BY $qcsColumns, SNAPPY_SAMPLER_WEIGHTAGE")
    // results_sampled_table.collect()

    var i: Integer = 0
    var l: Integer = 1
    var prevWt: Long = 0

    results_sampled_table.collect().foreach(t => {

      arr.foreach(a => {
        currQCS += t.get(l).toString; l += 1
      })
      l = 1

      var rowValue = t.getInt(0)
      if (applyCondition) {
        if (filterRow(t, pos + 1)) {
          rowValue = 0
        }
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
      else if (qcs == "" || qcs.equals(currQCS) && prevWt == t.getLong(pos)) {
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
        prevWt = t.getLong(pos)
        total(i) = rowValue
      }
      currQCS = ""
    })
    pos + 1
  }

  test("test for mean queries") {
    val printDebug = false
    val aggCol : String = "ArrDelay"
    val conditionalCol : String = ""

    setupCounters(aggCol, conditionalCol, noFilter)

    val query : String = "SELECT Avg(ArrDelay) as AD FROM airline"
    val conf: Double = new NormalDistribution().inverseCumulativeProbability(0.5 + 0.95 / 2.0)
    val sampleQuery: String =
      s"SELECT AVG(ArrDelay) as AVERAGE, absolute_error(AVERAGE) FROM airlineSampled"
    val arr : Array[String] = qcsColumns.split(",")

    if (printDebug) {
      // scalastyle:off println
      println("Cnt->>>>>>" + cnt.toList) // ni
      println("Total->>>>>>" + total.toList) // Sum-i
      println("WtCnt->>>>>>" + wtCnt.toList) // Ni
      // scalastyle:on println
    }
    var qcs = "START"
    var i, l = 0
    var value : Double = 0
    val vari = new Array[Double](1000)

    var variFactor : Double = 0 // Add for all L ( Ni*(Ni-ni)/ni-1)* Vari
    var meanFactor : Double = 0 // Add for all L Ni*Yi-Bar
    var prevWt : Long = 0

    results_sampled_table.collect().foreach(t => {

      arr.foreach(a => {
        currQCS += t.get(l).toString; l += 1
      })
      l = 1

      if (qcs.equals("START") || qcs == "" ||
          (qcs.equals(currQCS) && prevWt == t.getLong(pos))) {
        val avg = total(i) / cnt(i)
        value = value + (t.getInt(0) - avg) * (t.getInt(0) - avg)
        prevWt = t.getLong(pos)

        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1
      }
      else {
        vari(i) = value / cnt(i) // Si
        // (Ni*(Ni-ni)/(ni-1))*vari(i)
        variFactor += ((wtCnt(i) * (wtCnt(i) - cnt(i))) / (cnt(i) - 1)) * vari(i)
        meanFactor += (total(i) / cnt(i)) * wtCnt(i) // Ni*Yi-Bar

        i = i + 1 // increment or set
        value = 0

        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1

        prevWt = t.getLong(pos)
        val avg = total(i) / cnt(i)
        value = value + (t.getInt(0) - avg) * (t.getInt(0) - avg)
      }
      currQCS = ""
    })

    vari(i) = value/cnt(i) // Si
    // (Ni*(Ni-ni)/(ni-1))*vari(i)
    variFactor +=  ((wtCnt(i)*(wtCnt(i) - cnt(i)))/(cnt(i) - 1))*vari(i)
    meanFactor +=  (total(i)/cnt(i)) * wtCnt (i) // Ni*Yi-Bar

    if (printDebug) {
      // scalastyle:off println
      println("Vari->>>>>>>" + vari.toList)
      // scalastyle:on println
    }
    var population : Double = 0
    wtCnt.foreach(t => population += t)
    val variance = variFactor/(population * population) // sigma sq theata
    val newSD = math.sqrt(variance)
    if (printDebug) {
      // scalastyle:off println
      println()
      println("=============== RESULTS FROM NEW FORMULAS ===============")
      println("Variance->>>>>> " + "%.5f".format(variance))
      println("SD->>>>>> " + "%.5f".format(math.sqrt(variance)))
      println("Average->>>>>> " + "%.5f".format(meanFactor / population))
      // scalastyle:on println
    }

    val results_sample_table_mean = snc.sql(sampleQuery)
    if (printDebug) {
      // scalastyle:off println
      println("=============== DIRECT QUERY ON SAMPLE TABLE ===============")
    }
    val mean = results_sample_table_mean.collect()(0).getDouble(0)
    val error = results_sample_table_mean.collect()(0).getDouble(1)
    val sdDirect: Double = error / conf
    if (printDebug) {
      printDouble(s"Mean", mean)
      printDouble(s"Error", error)
      printDouble(s"SD", sdDirect)
      // results_sample_table_mean.collect()
    }

    if (printDebug) {
      val results_population_table_mean = snc.sql(query)
      // scalastyle:off println
      println()
      println("=============== DIRECT QUERY ON POPULATION TABLE ===============")
      // scalastyle:on println
      results_population_table_mean.show()

      // scalastyle:off println
      println("Success...")
      // scalastyle:on println
    }
    val resultsDifferent = Math.abs(sdDirect - newSD) < 1.0
    if (!newSD.isNaN && !resultsDifferent) {
      printDouble(s"Got SD Mean", sdDirect)
      printDouble(s"Expected SD mean", newSD)
      fail()
    }



    // repeat once
    val results_sample_table_mean1 = snc.sql(sampleQuery)
    val mean1 = results_sample_table_mean1.collect()(0).getDouble(0)
    val error1 = results_sample_table_mean1.collect()(0).getDouble(1)
    val sdDirect1: Double = error / conf
    val resultsDifferent1 = Math.abs(sdDirect - sdDirect1) < 1.0
    if (!resultsDifferent1) {
      printDouble(s"Got Repeat SD Mean", sdDirect1)
      printDouble(s"Expected SD Mean", sdDirect)
      fail()
    }

    // repeat twice
    val results_sample_table_mean2 = snc.sql(sampleQuery)
    val mean2 = results_sample_table_mean2.collect()(0).getDouble(0)
    val error2 = results_sample_table_mean2.collect()(0).getDouble(1)
    val sdDirect2: Double = error / conf
    val resultsDifferent2 = Math.abs(sdDirect - sdDirect2) < 1.0
    if (!resultsDifferent2) {
      printDouble(s"Got Repeat SD Mean", sdDirect2)
      printDouble(s"Expected SD Mean", sdDirect)
      fail()
    }
  }

  private def meanOneLoop(aggCol: String): Double = {
    val conditionalCol : String = ""

    setupCounters(aggCol, conditionalCol, noFilter)

    val arr : Array[String] = qcsColumns.split(",")
    var qcs = "START"
    var i, l = 0
    var value : Double = 0
    val vari = new Array[Double](1000)

    var variFactor : Double = 0 // Add for all L ( Ni*(Ni-ni)/ni-1)* Vari
    var meanFactor : Double = 0 // Add for all L Ni*Yi-Bar
    var prevWt : Long = 0

    results_sampled_table.collect().foreach(t => {

      arr.foreach(a => {
        currQCS += t.get(l).toString; l += 1
      })
      l = 1

      if (qcs.equals("START") || qcs == "" ||
          (qcs.equals(currQCS) && prevWt == t.getLong(pos))) {
        val avg = total(i) / cnt(i)
        value = value + (t.getInt(0) - avg) * (t.getInt(0) - avg)
        prevWt = t.getLong(pos)

        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1
      }
      else {
        vari(i) = value / cnt(i) // Si
        // (Ni*(Ni-ni)/(ni-1))*vari(i)
        variFactor += ((wtCnt(i) * (wtCnt(i) - cnt(i))) / (cnt(i) - 1)) * vari(i)
        meanFactor += (total(i) / cnt(i)) * wtCnt(i) // Ni*Yi-Bar

        i = i + 1 // increment or set
        value = 0

        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1

        prevWt = t.getLong(pos)
        val avg = total(i) / cnt(i)
        value = value + (t.getInt(0) - avg) * (t.getInt(0) - avg)
      }
      currQCS = ""
    })

    vari(i) = value/cnt(i) // Si
    // (Ni*(Ni-ni)/(ni-1))*vari(i)
    variFactor +=  ((wtCnt(i)*(wtCnt(i) - cnt(i)))/(cnt(i) - 1))*vari(i)
    meanFactor +=  (total(i)/cnt(i)) * wtCnt (i) // Ni*Yi-Bar

    var population : Double = 0
    wtCnt.foreach(t => population += t)
    val variance = variFactor/(population * population) // sigma sq theata
    math.sqrt(variance)
  }

  private def meanOneLoop2(aggCol: String, index : Integer): Double = {
    val conditionalCol : String = ""

    setupCounters(aggCol, conditionalCol, noFilter)

    val arr : Array[String] = qcsColumns.split(",")
    var qcs = "START"
    var i, l = 0
    var count : Integer = 0
    val partitionedValues = new Array[mutable.MutableList[Double]](3)
    partitionedValues(0) = new mutable.MutableList[Double]
    partitionedValues(1) = new mutable.MutableList[Double]
    partitionedValues(2) = new mutable.MutableList[Double]
    var variFactor : Double = 0
    var prevWt : Long = 0

    results_sampled_table.collect().foreach(t => {

      arr.foreach(a => {
        currQCS += t.get(l).toString; l += 1
      })
      l = 1
      var rowValue = t.getInt(0)
      if (qcs.equals("START") || qcs == "" ||
          (qcs.equals(currQCS) && prevWt == t.getLong(pos))) {
        count = count + 1
        partitionedValues(count % 3) += rowValue
        prevWt = t.getLong(pos)

        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1
      }
      else {
        val statCounter = StatCounter(partitionedValues(0))
        partitionedValues(0).clear()
        statCounter.merge(partitionedValues(1))
        partitionedValues(1).clear()
        statCounter.merge(partitionedValues(2))
        partitionedValues(2).clear()
        variFactor += ((wtCnt(i) * (wtCnt(i) - cnt(i))) / (cnt(i) - 1)) * statCounter.variance
        if ((index == 10 || index % 100 == 0) && i % 20 == 0) {
//          printDouble(s"statCounter.variance for SUM $conditionalCol" +
//              s": New Variance index=$index i=$i", statCounter.variance)
        }

        i = i + 1 // increment or set
        count = 0

        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1

        prevWt = t.getLong(pos)
        count = count + 1
        partitionedValues(count % 3) += rowValue
      }
      currQCS = ""
    })

    val statCounter = StatCounter(partitionedValues(0))
    partitionedValues(0).clear()
    statCounter.merge(partitionedValues(1))
    partitionedValues(1).clear()
    statCounter.merge(partitionedValues(2))
    partitionedValues(2).clear()
    variFactor += ((wtCnt(i) * (wtCnt(i) - cnt(i))) / (cnt(i) - 1)) * statCounter.variance

    var population : Double = 0
    wtCnt.foreach(t => population += t)
    val variance = variFactor/(population * population) // sigma sq theata
    math.sqrt(variance)
}

  private def meanDirect(aggCol: String): Double = {
    val conf: Double = new NormalDistribution().inverseCumulativeProbability(0.5 + 0.95 / 2.0)
    val query: String = s"SELECT AVG($aggCol) as AVG, absolute_error(AVG)" +
        s" FROM airlineSampled"
    val meanDirect_results_on_sample_table = snc.sql(query)
    var SD: Double = 0
    meanDirect_results_on_sample_table.collect()
        .foreach(t => {
          // val avg: Double = t.getDouble(0)
          val error: Double = t.getDouble(1)
          SD = error / conf
        })
    SD
  }

  private def conditionalCountDirect(aggCol : String, whereCondition : String) : Long = {
    val query: String =
      s"SELECT COUNT($aggCol) as COUNT FROM airlineSampled where $whereCondition"
    val count_result_direct_on_sample_table = snc.sql(query)
    count_result_direct_on_sample_table.collect()(0).getLong(0)
  }

  private def conditionalCountDirect2(aggCol : String, whereCondition : String,
      needError : Boolean, doPrint : Boolean) : Double = {
    if (needError) {
      val query: String = s"SELECT COUNT($aggCol) as COUNT, absolute_error(COUNT)" +
          s" FROM airlineSampled where $whereCondition"
      val count_result_direct_on_sample_table_2 = snc.sql(query)
      val count = count_result_direct_on_sample_table_2.collect()(0).getLong(0)
      val error = count_result_direct_on_sample_table_2.collect()(0).getDouble(1)
      val conf: Double = new NormalDistribution().inverseCumulativeProbability(0.5 + 0.95 / 2.0)
      val SD: Double = error / conf
      if (doPrint) {
        printDouble(s"Count", count)
        printDouble(s"Error", error)
        printDouble(s"SD", SD)
      }
      SD
    }
    else {
      val query: String = s"SELECT COUNT($aggCol) as COUNT " +
          s" FROM airlineSampled where $whereCondition"
      val count_result_direct_on_sample_table_2 = snc.sql(query)
      val count = count_result_direct_on_sample_table_2.collect()(0).getLong(0)
      if (doPrint) {
        printDouble(s"Count = ", count)
      }
      0.0
    }

  }

  ignore("1000 runs for SD") {
    val aggCol: String = "ArrDelay"
    val conditionalColYear: String = "YearI"
    val whereConditionYear: String = s" $conditionalColYear = 2015 "
    val conditionalColMonth: String = "MonthI"
    val whereConditionMonth: String = s" $conditionalColMonth < 3 "

    val numLoop: Integer = 1000

    var newSDMean: Long = 0
    var newSDMean2: Long = 0
    var oldSDMean: Long = 0
    var newSDonYear: Long = 0
    var newSDonYear2: Long = 0
    var oldSDonYear: Long = 0
    var newSDonMonth: Long = 0
    var newSDonMonth2: Long = 0
    var oldSDonMonth: Long = 0

    deleteSampleTable()
    // scalastyle:off println
    println()
    println("=============== 1000 runs for SD ===============")
    // scalastyle:on println

    for (i <- 0 to numLoop - 1) {
      writeSampleTable()
      val newSDMeanOneLoop = meanOneLoop(aggCol)
      val newSDMeanOneLoop2 = meanOneLoop2(aggCol, i)
      val oldSDMeanOneLoop = meanDirect(aggCol)

      val doPrintSum = false
      val doPrintDetailed = false
      val newSDonYearOneLoop = conditionalSumOneLoop(aggCol, conditionalColYear,
        whereConditionYear, filterOnYear, doPrintSum, doPrintDetailed)
      val newSDonYearOneLoop2 = conditionalSumOneLoop2(aggCol, conditionalColYear,
        whereConditionYear, filterOnYear, i)
      val oldSDonYearOneLoop = conditionalSumDirect(aggCol, whereConditionYear, doPrintSum)

      val newSDonMonthOneLoop = conditionalSumOneLoop(aggCol, conditionalColMonth,
        whereConditionMonth, filterOnMonth, doPrintSum, doPrintDetailed)
      val newSDonMonthOneLoop2 = conditionalSumOneLoop2(aggCol, conditionalColMonth,
        whereConditionMonth, filterOnMonth, i)
      val oldSDonMonthOneLoop = conditionalSumDirect(aggCol, whereConditionMonth, doPrintSum)
      deleteSampleTable()

      if (i < 5) {
        printDouble(s"new SD Mean - one loop $i", newSDMeanOneLoop)
        printDouble(s"new SD Mean - 2 - one loop $i", newSDMeanOneLoop2)
        printDouble(s"old SD Mean - one loop $i", oldSDMeanOneLoop)
        printDouble(s"new SD Year - one loop $i", newSDonYearOneLoop)
        printDouble(s"new SD Year - 2 - one loop $i", newSDonYearOneLoop2)
        printDouble(s"old SD Year - one loop $i", oldSDonYearOneLoop)
        printDouble(s"new SD Month - one loop $i", newSDonMonthOneLoop)
        printDouble(s"new SD Month - 2 - one loop $i", newSDonMonthOneLoop2)
        printDouble(s"old SD Month - one loop $i", oldSDonMonthOneLoop)
      }

      newSDMean += Math.round(newSDMeanOneLoop * 100)
      newSDMean2 += Math.round(newSDMeanOneLoop2 * 100)
      oldSDMean += Math.round(oldSDMeanOneLoop * 100)
      newSDonYear += Math.round(newSDonYearOneLoop * 100)
      newSDonYear2 += Math.round(newSDonYearOneLoop2 * 100)
      oldSDonYear += Math.round(oldSDonYearOneLoop * 100)
      newSDonMonth += Math.round(newSDonMonthOneLoop * 100)
      newSDonMonth2 += Math.round(newSDonMonthOneLoop2 * 100)
      oldSDonMonth += Math.round(oldSDonMonthOneLoop * 100)

      if (i == 10 || i % 100 == 0) {
        printDouble(s"new SD Mean loop=$i", newSDMean, i + 1)
        printDouble(s"new SD Mean 2 - loop=$i", newSDMean2, i + 1)
        printDouble(s"old SD Mean loop=$i", oldSDMean, i + 1)
        printDouble(s"new SD Year loop=$i", newSDonYear, i + 1)
        printDouble(s"new SD Year - 2 - loop=$i", newSDonYear2, i + 1)
        printDouble(s"old SD Year loop=$i", oldSDonYear, i + 1)
        printDouble(s"new SD Month loop=$i", newSDonMonth, i + 1)
        printDouble(s"new SD Month - 2 - loop=$i", newSDonMonth2, i + 1)
        printDouble(s"old SD Month loop=$i", oldSDonMonth, i + 1)
      }
    }

    printDouble("new SD Mean - final", newSDMean, numLoop)
    printDouble("new SD Mean - 2 - final", newSDMean2, numLoop)
    printDouble("old SD Mean - final", oldSDMean, numLoop)
    printDouble("new SD Year - final", newSDonYear, numLoop)
    printDouble("new SD Year - 2 - final", newSDonYear2, numLoop)
    printDouble("old SD Year - final", oldSDonYear, numLoop)
    printDouble("new SD Month - final", newSDonMonth, numLoop)
    printDouble("new SD Month - 2 - final", newSDonMonth2, numLoop)
    printDouble("old SD Month - final", oldSDonMonth, numLoop)

    printBigDecimal("new SD Mean - final 2", newSDMean, numLoop)
    printBigDecimal("new SD Mean - 2 - final 2", newSDMean2, numLoop)
    printBigDecimal("old SD Mean - final 2", oldSDMean, numLoop)
    printBigDecimal("new SD Year - final 2", newSDonYear, numLoop)
    printBigDecimal("new SD Year - 2 - final 2", newSDonYear2, numLoop)
    printBigDecimal("old SD Year - final 2", oldSDonYear, numLoop)
    printBigDecimal("new SD Month - final 2", newSDonMonth, numLoop)
    printBigDecimal("new SD Month - 2 - final 2", newSDonMonth2, numLoop)
    printBigDecimal("old SD Month - final 2", oldSDonMonth, numLoop)
  }


  ignore("test for conditional count queries - 1000 runs"){
    val numLoop: Integer = 1000

    val aggCol : String = "1"
    val conditionalColMonth : String = "MonthI"
    val conditionalColYear : String = "YearI"
    val whereConditionMonth : String = s" $conditionalColMonth < 3 "
    val whereConditionYear: String = s" $conditionalColYear = 2015 "

    val newCntArrayOnMonth = new Array[Double](numLoop)
    val newCntArrayOnYear = new Array[Double](numLoop)
    val oldCntArrayOnMonth = new Array[Long](numLoop)
    val oldCntArrayOnYear = new Array[Long](numLoop)
    for(i <- 0 to numLoop - 1) {
      newCntArrayOnMonth(i) = 0
      newCntArrayOnYear(i) = 0
      oldCntArrayOnMonth(i) = 0
      oldCntArrayOnYear(i) = 0
    }

    deleteSampleTable()
    for(i <- 0 to numLoop - 1){
      writeSampleTable()
      val printCount = false
      val printDetailed = false
      val returnSD = true
      val doPrintStrata = false
      newCntArrayOnMonth(i) = conditionalCount(aggCol, conditionalColMonth, whereConditionMonth,
        filterOnMonth, i, printCount, printDetailed, doPrintStrata, returnSD)
      newCntArrayOnYear(i) = conditionalCount(aggCol, conditionalColYear, whereConditionYear,
        filterOnYear, i, printCount, printDetailed, doPrintStrata, returnSD)
      oldCntArrayOnMonth(i) = conditionalCountDirect(aggCol, whereConditionMonth)
      oldCntArrayOnYear(i) = conditionalCountDirect(aggCol, whereConditionYear)
      // if (i == 5 || i == 10 || i % 100 == 0) {
        // scalastyle:off println
        println()
        println(s"==COUNT values for $i th iterration=============")
        println("=======New Count for Month >>>>> " + newCntArrayOnMonth(i))
        println("=======New Count for Year >>>>> " + newCntArrayOnYear(i))
        println("=======Old Count for Month >>>>> " + oldCntArrayOnMonth(i))
        println("=======Old Count for Year >>>>> " + oldCntArrayOnYear(i))
        // scalastyle:on println
      // }
      deleteSampleTable()
    }

    var newMonthCnt : Double = 0.0
    newCntArrayOnMonth.foreach(e => newMonthCnt = newMonthCnt + e)

    var newYearCnt : Double = 0.0
    newCntArrayOnYear.foreach(e => newYearCnt = newYearCnt + e)

    var oldMonthCnt : Double = 0.0
    oldCntArrayOnMonth.foreach(e => oldMonthCnt = oldMonthCnt + e)

    var oldYearCnt : Double = 0.0
    oldCntArrayOnYear.foreach(e => oldYearCnt = oldYearCnt + e)

    val finalCntNewSDMonth : Double = newMonthCnt/numLoop
    val finalCntNewSDYear : Double = newYearCnt/numLoop
    val finalCntOldSDMonth : Double = oldMonthCnt/numLoop
    val finalCntOldSDYear : Double = oldYearCnt/numLoop

    // scalastyle:off println
    println()
    println("================ Final Counts =================")
    println("====== Mean NewCount for Month >>>>> " + finalCntNewSDMonth)
    println("====== Mean NewCount for Year >>>>> " + finalCntNewSDYear)
    println("====== Mean OldCount for Month >>>>> " + finalCntOldSDMonth)
    println("====== Mean OldCount for Year >>>>> " + finalCntOldSDYear)
    // scalastyle:on println
  }

  test("test for conditional count queries - Month") {
    val aggCol: String = "1"
    val conditionalCol: String = "MonthI"
    val whereCondition: String = s" $conditionalCol < 3 "
    val doPrint = false
    val doDetailedPrint = false
    val returnSD = true
    val doPrintStrata = false
    val newSD = conditionalCount(aggCol, conditionalCol, whereCondition,
      filterOnMonth, 0, doPrint, doDetailedPrint, doPrintStrata, returnSD)
    val needError = true
    val sdDirect = conditionalCountDirect2(aggCol, whereCondition, needError, doPrint)
    val resultsDifferent = Math.abs(sdDirect - newSD) < 1.0
    if (!newSD.isNaN && !resultsDifferent) {
      printDouble(s"Got SD $whereCondition Count", sdDirect)
      printDouble(s"Expected SD $whereCondition Count", newSD)
      fail()
    }


    // repeat once
    val sdDirect1 = conditionalCountDirect2(aggCol, whereCondition, needError, doPrint)
    val resultsDifferent1 = Math.abs(sdDirect - sdDirect1) < 1.0
    if (!resultsDifferent1) {
      printDouble(s"Got Repeat SD $whereCondition Count", sdDirect1)
      printDouble(s"Expected SD $whereCondition Count", sdDirect)
      fail()
    }

    // repeat twice
    val sdDirect2 = conditionalCountDirect2(aggCol, whereCondition, needError, doPrint)
    val resultsDifferent2 = Math.abs(sdDirect - sdDirect2) < 1.0
    if (!resultsDifferent2) {
      printDouble(s"Got Repeat SD $whereCondition Count", sdDirect2)
      printDouble(s"Expected SD $whereCondition Count", sdDirect)
      fail()
    }
  }

  test("test for conditional count queries - Year") {
    val aggCol: String = "1"
    val conditionalCol: String = "YearI"
    val whereCondition: String = s" $conditionalCol = 2015 "
    val doPrint = false
    val doDetailedPrint = false
    val returnSD = true
    val doPrintStrata = false
    val newSD = conditionalCount(aggCol, conditionalCol, whereCondition,
      filterOnYear, 0, doPrint, doDetailedPrint, doPrintStrata, returnSD)
    val needError = true
    val sdDirect = conditionalCountDirect2(aggCol, whereCondition, needError, doPrint)
    val resultsDifferent = Math.abs(sdDirect - newSD) < 1.0
    if (!newSD.isNaN && !resultsDifferent) {
      printDouble(s"Got SD $whereCondition Count", sdDirect)
      printDouble(s"Expected SD $whereCondition Count", newSD)
      fail()
    }


    // repeat once
    val sdDirect1 = conditionalCountDirect2(aggCol, whereCondition, needError, doPrint)
    val resultsDifferent1 = Math.abs(sdDirect - sdDirect1) < 1.0
    if (!resultsDifferent1) {
      printDouble(s"Got Repeat SD $whereCondition Count", sdDirect1)
      printDouble(s"Expected SD $whereCondition Count", sdDirect)
      fail()
    }

    // repeat twice
    val sdDirect2 = conditionalCountDirect2(aggCol, whereCondition, needError, doPrint)
    val resultsDifferent2 = Math.abs(sdDirect - sdDirect2) < 1.0
    if (!resultsDifferent2) {
      printDouble(s"Got Repeat SD $whereCondition Count", sdDirect2)
      printDouble(s"Expected SD $whereCondition Count", sdDirect)
      fail()
    }
  }

  private def conditionalCount(aggCol: String, conditionalCol: String, whereCondition: String,
      filterRow: (Row, Integer) => Boolean, index : Int,
      printCount : Boolean, printDetailed : Boolean, printStrata : Boolean,
      returnSD : Boolean): Double = {

    setupCounters(aggCol, conditionalCol, filterRow)

    val arr : Array[String] = qcsColumns.split(",")

    if (printDetailed) {
      // scalastyle:off println
      println("Cnt->>>>>>" + cnt.toList) // ni
      println("Total->>>>>>" + total.toList) // tuples satisfying c in S
      println("WtCnt->>>>>>" + wtCnt.toList) // Ni
      // scalastyle:on println
    }

    var qcs = "START"
    var i, l = 0
    var value : Double = 0
    var variance : Double = 0 // Add for all L (Ni*(Ni-ni)/ni-1) * fci * (1-fci)
    var count : Double = 0
    var prevWt : Long = 0

    results_sampled_table.collect().foreach(t => {
      arr.foreach( a => { currQCS += t.get(l).toString; l += 1})
      l = 1

      if (qcs.equals("START") || qcs == "" ||
          (qcs.equals(currQCS) && prevWt == t.getLong(pos))) {
        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1
        prevWt = t.getLong(pos)
      }
      else {
        val fci = total(i) / cnt(i)
        // Can result into NaN if cnt(i) is 1
        variance += ((wtCnt(i) * (wtCnt(i) - cnt(i))) / (cnt(i) - 1)) * (fci * (1 - fci))
        count += total(i) * wtCnt(i) / cnt(i)
        if (printStrata) {
          // scalastyle:off println
          println("right = %.2f".format(wtCnt(i))
              + " ,left = " + cnt(i)
              + " ,count = " + total(i)
              + " : New")
          // scalastyle:on println
        }

        i = i + 1 // increment or set
        value = 0

        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1
        prevWt = t.getLong(pos)
      }
      currQCS = ""
    })

    val fci = total(i)/cnt(i)
    variance +=  ((wtCnt(i)*(wtCnt(i) - cnt(i)))/(cnt(i) - 1)) * (fci * (1- fci))
    count += total(i) * wtCnt(i) / cnt(i)
    if (printStrata) {
      // scalastyle:off println
      println("right = %.2f".format(wtCnt(i))
          + " ,left = " + cnt(i)
          + " ,count = " + total(i)
          + " : New")
      // scalastyle:on println
    }

    if (printCount) {
      // scalastyle:off println
      println()
      println(s"=== RESULTS FROM NEW FORMULAS $whereCondition for $index th iteration======")
      println("Variance->>>>>> " + "%.5f".format(variance))
      println("SD->>>>>> " + "%.5f".format(math.sqrt(variance)))
      println("Count->>>>>> " + "%.5f".format(count))
      // scalastyle:on println
    }

    if (printDetailed) {
      val query: String = s"SELECT count(*) as COUNT FROM airline where $whereCondition"
      val results_population_table_count = snc.sql(query)
      // scalastyle:off println
      println()
      println(query)
      results_population_table_count.show()
      // scalastyle:on println
    }

    if (returnSD) {
      math.sqrt(variance)
    }
    else {
      count
    }
  }

  private def conditionalSum(aggCol: String, conditionalCol: String, whereCondition: String,
      filterRow: (Row, Integer) => Boolean): Unit = {

    val position = setupCounters(aggCol, conditionalCol, filterRow)

    val arr : Array[String] = qcsColumns.split(",")

    logInfo("Cnt->>>>>>" + cnt.toList) // ni
    logInfo("Total->>>>>>" + total.toList) // Sum-i
    logInfo("WtCnt->>>>>>" + wtCnt.toList) // Ni

    var qcs = "START"
    var i, l = 0
    var value : Double = 0
    val vari = new Array[Double](1000)

    var variFactor : Double = 0 // Add for all L ( Ni*(Ni-ni)/ni-1)* Vari
    var sumFactor : Double = 0 // Add for all L Ni*Yi-Bar
    var prevWt : Long = 0
    var zeroStrataCount: Integer = 0

    results_sampled_table.collect().foreach(t => {

      arr.foreach( a => { currQCS += t.get(l).toString; l += 1})
      l = 1

      var rowValue = t.getInt(0)
      if (filterRow(t, position)) {
        rowValue = 0
      }

      if (qcs.equals("START") || qcs == "" ||
          (qcs.equals(currQCS) && prevWt == t.getLong(pos))) {
        val avg = total(i) / cnt(i)
        value = value + (rowValue - avg) * (rowValue - avg)

        prevWt = t.getLong(pos)
        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1
      }
      else {
        vari(i) = value / cnt(i) // Si
        // (Ni*(Ni-ni)/(ni-1))*vari(i)
        variFactor += ((wtCnt(i) * (wtCnt(i) - cnt(i))) / (cnt(i) - 1)) * vari(i)
        val sumi = (total(i) / cnt(i)) * wtCnt(i) // Ni*Yi-Bar
        sumFactor += sumi
        if (sumi == 0) {
          zeroStrataCount = zeroStrataCount + 1
        }
        i = i + 1 // increment or set
        value = 0

        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1

        prevWt = t.getLong(pos)
        val avg = total(i) / cnt(i)
        value = value + (rowValue - avg) * (rowValue - avg)
      }
      currQCS = ""
    })

    vari(i) = value/cnt(i) // Si
    // (Ni*(Ni-ni)/(ni-1))*vari(i)
    variFactor +=  ((wtCnt(i)*(wtCnt(i) - cnt(i)))/(cnt(i) - 1))*vari(i)
    sumFactor +=  (total(i)/cnt(i)) * wtCnt (i) // Ni*Yi-Bar

    logInfo("Vari->>>>>>>" + vari.toList)
    var population : Double = 0
    wtCnt.foreach(t => population += t)
//    val variance = variFactor/(population * population) // sigma sq theata
    val variance = variFactor // sigma sq theata
    logInfo("\n")
    logInfo("=============== Results from new formulas ===============")
    logInfo("Variance->>>>>> " + "%.5f".format(variance))
    logInfo("SD->>>>>> " + "%.5f".format(math.sqrt(variance)))
    logInfo("Sum->>>>>> " + "%.5f".format(sumFactor))
    logInfo("#Of Starta->>>>>> " + i)
    logInfo("#Of Zero Starta->>>>>> " + zeroStrataCount)

    {
      val query: String = s"SELECT SUM($aggCol) as SUM FROM airline where $whereCondition"
      val results_population_table1 = snc.sql(query)
      logInfo("\n")
      logInfo("=============== DIRECT QUERY ON POPULATION TABLE ===============")
      logInfo(query)
      logInfo(results_population_table1.collect().mkString("\n"))
    }

    {
      val query: String = s"SELECT count($aggCol) as COUNT FROM airline"
      val results_population_table2 = snc.sql(query)
      logInfo("\n")
      logInfo(query)
      logInfo(results_population_table2.collect().mkString("\n"))
    }

    {
      val query: String = s"SELECT count($aggCol) as COUNT FROM airline where $whereCondition"
      val results_population_table3 = snc.sql(query)
      logInfo("\n")
      logInfo(query)
      logInfo(results_population_table3.collect().mkString("\n"))
    }

    {
      val query: String = s"SELECT $aggCol FROM airlineSampled"
      val results_sample_table4 = snc.sql(query)
      logInfo("\n")
      logInfo(query)
      var count : Integer = 0
      results_sample_table4.collect().foreach(t => count = count + 1)
      logInfo("Count: " + count)
    }

    {
      val query: String = s"SELECT $aggCol FROM airlineSampled where $whereCondition"
      val results_sample_table5 = snc.sql(query)
      logInfo("\n")
      logInfo(query)
      var count : Integer = 0
      results_sample_table5.collect().foreach(t => count = count + 1)
      logInfo("Count: " + count)
    }

    {
      val conf: Double = new NormalDistribution().inverseCumulativeProbability(0.5 + 0.95 / 2.0)
      val query: String = s"SELECT SUM($aggCol) as SUM, absolute_error(SUM)" +
          s" FROM airlineSampled where $whereCondition"
      val results_population_table6 = snc.sql(query)
      logInfo("\n")
      logInfo("=============== DIRECT QUERY ON SAMPLE TABLE ===============")
      logInfo(query)
      results_population_table6.collect()
          .foreach(t => {
            val sum: Double = t.getDouble(0)
            printDouble("SUM", sum)
            val error: Double = t.getDouble(1)
            printDouble("Absolute Error", error)
            printDouble("SD", error / conf)
          })
    }

    logInfo("Success...")
  }

  private def conditionalSumOneLoop2(aggCol: String, conditionalCol: String, whereCondition: String,
      filterRow: (Row, Integer) => Boolean, index : Integer): Double = {
    val position = setupCounters(aggCol, conditionalCol, filterRow)
    val arr : Array[String] = qcsColumns.split(",")

    var qcs = "START"
    var i, l = 0
    var count : Integer = 0
    val partitionedValues = new Array[mutable.MutableList[Double]](3)
    partitionedValues(0) = new mutable.MutableList[Double]
    partitionedValues(1) = new mutable.MutableList[Double]
    partitionedValues(2) = new mutable.MutableList[Double]
    var variance : Double = 0
    var prevWt : Long = 0

    results_sampled_table.collect().foreach(t => {
      arr.foreach( a => { currQCS += t.get(l).toString; l += 1})
      l = 1

      var rowValue = t.getInt(0)
      if (filterRow(t, position)) {
        rowValue = 0
      }

      if (qcs.equals("START") || qcs == "" ||
          (qcs.equals(currQCS) && prevWt == t.getLong(pos))) {
        count = count + 1
        partitionedValues(count % 3) += rowValue

        prevWt = t.getLong(pos)
        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1
      }
      else {
        val statCounter = StatCounter(partitionedValues(0))
        partitionedValues(0).clear()
        statCounter.merge(partitionedValues(1))
        partitionedValues(1).clear()
        statCounter.merge(partitionedValues(2))
        partitionedValues(2).clear()
        if ((index == 10 || index % 100 == 0) && i % 20 == 0) {
//          printDouble(s"statCounter.variance for SUM $conditionalCol" +
//              s": New Variance index=$index i=$i", statCounter.variance)
        }
        variance +=
            ((wtCnt(i) * (wtCnt(i) - cnt(i))) / (cnt(i) - 1)) * statCounter.variance

        i = i + 1 // increment or set
        count = 0
        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1

        prevWt = t.getLong(pos)
        count = count + 1
        partitionedValues(count % 3) += rowValue
      }
      currQCS = ""
    })

    val statCounter = StatCounter(partitionedValues(0))
    statCounter.merge(partitionedValues(1))
    statCounter.merge(partitionedValues(2))
    variance +=
        ((wtCnt(i) * (wtCnt(i) - cnt(i))) / (cnt(i) - 1)) * statCounter.variance

    math.sqrt(variance)
  }

  private def conditionalSumOneLoop(aggCol: String, conditionalCol: String, whereCondition: String,
      filterRow: (Row, Integer) => Boolean,
      printSum : Boolean, printDetailed : Boolean): Double = {
    val position = setupCounters(aggCol, conditionalCol, filterRow)
    val arr : Array[String] = qcsColumns.split(",")

    var qcs = "START"
    var i, l, count = 0
    var value : Double = 0
    val vari = new Array[Double](1000)

    var variFactor : Double = 0 // Add for all L ( Ni*(Ni-ni)/ni-1)* Vari
    var sumFactor : Double = 0 // Add for all L Ni*Yi-Bar
    var prevWt : Long = 0

    results_sampled_table.collect().foreach(t => {
      arr.foreach( a => { currQCS += t.get(l).toString; l += 1})
      l = 1

      var rowValue = t.getInt(0)
      if (filterRow(t, position)) {
        rowValue = 0
      }

      if (qcs.equals("START") || qcs == "" ||
          (qcs.equals(currQCS) && prevWt == t.getLong(pos))) {
        val avg = total(i) / cnt(i)
        value = value + (rowValue - avg) * (rowValue - avg)
        count += 1
        prevWt = t.getLong(pos)
        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1
      }
      else {
        vari(i) = value / cnt(i) // Si
        // (Ni*(Ni-ni)/(ni-1))*vari(i)
        if (printDetailed) {
          // scalastyle:off println
          println("right = %.2f".format(wtCnt(i))
              + " ,left = " + cnt(i)
              + " ,Variance = %.2f".format(vari(i))
              + " ,WeightCol = " + prevWt
              + " ,count = " + count
              + " : New")
          // scalastyle:on println
        }
        variFactor += ((wtCnt(i) * (wtCnt(i) - cnt(i))) / (cnt(i) - 1)) * vari(i)
        sumFactor += (total(i) / cnt(i)) * wtCnt(i) // Ni*Yi-Bar

        i = i + 1 // increment or set
        value = 0
        count = 0
        qcs = ""
        arr.foreach(a => {
          qcs += t.get(l).toString; l += 1
        })
        l = 1

        prevWt = t.getLong(pos)
        val avg = total(i) / cnt(i)
        value = value + (rowValue - avg) * (rowValue - avg)
        count += 1
      }
      currQCS = ""
    })

    vari(i) = value/cnt(i) // Si
    // (Ni*(Ni-ni)/(ni-1))*vari(i)
    variFactor +=  ((wtCnt(i)*(wtCnt(i) - cnt(i)))/(cnt(i) - 1))*vari(i)
    sumFactor +=  (total(i)/cnt(i)) * wtCnt (i) // Ni*Yi-Bar
    if (printDetailed) {
      // scalastyle:off println
      println("right = %.2f".format(wtCnt(i))
          + " ,left = " + cnt(i)
          + " ,Variance = %.2f".format(vari(i))
          + " ,WeightCol = " + prevWt
          + " ,count = " + count
          + " : New")
      // scalastyle:on println
    }
    if (printSum) {
      printDouble(s"SUM $whereCondition New", sumFactor)
    }

    var population : Double = 0
    wtCnt.foreach(t => population += t)
    //    val variance = variFactor/(population * population) // sigma sq theata
    val variance = variFactor // sigma sq theata
    math.sqrt(variance)
  }

  private def conditionalSumDirect(aggCol: String, whereCondition: String,
      printSum : Boolean): Double = {
    val conf: Double = new NormalDistribution().inverseCumulativeProbability(0.5 + 0.95 / 2.0)
    val query: String = s"SELECT SUM($aggCol) as SUM, absolute_error(SUM)" +
        s" FROM airlineSampled where $whereCondition"
    val sum_results_on_sample_table = snc.sql(query)
    var SD: Double = 0
    sum_results_on_sample_table.collect()
        .foreach(t => {
          if (printSum) {
            val sum: Double = t.getDouble(0)
            printDouble(s"SUM $whereCondition Direct", sum)
          }
          val error: Double = t.getDouble(1)
          SD = error / conf
        })
    SD
  }

  private def printDouble(des: String, value: Double): Boolean = {
    // scalastyle:off println
    println(des + " = %.5f".format(value))
    // scalastyle:on println
    false
  }

  private def printDouble(des: String, nomValue: Long, denomValue: Integer): Boolean = {
    var value : Double = nomValue
    value = value / (denomValue * 100).toDouble
    // scalastyle:off println
    println(des + " = %.5f".format(value))
    // scalastyle:on println
    false
  }

  private def printBigDecimal(des: String, nomValue: Long, denomValue: Integer): Boolean = {
    var value : BigDecimal = nomValue
    value = value / (denomValue * 100).toDouble
    // scalastyle:off println
    println(des + " = %.5f".format(value))
    // scalastyle:on println
    false
  }

  private def getWtCount (cellValue : Long) : Double = {
    val Ni = ((cellValue >> 8) & 0xffffffffL).toDouble
    val ni = ((cellValue >> 40) & 0xffffffffL).toDouble
    Ni / ni
  }

  ignore("test to debug conditional sum - year") {
    val aggCol: String = "ArrDelay"
    val conditionalCol: String = "YearI"
    val whereCondition: String = s" $conditionalCol = 2015 "
    conditionalSum(aggCol, conditionalCol, whereCondition,
      filterOnYear)
  }

  ignore("test to debug conditional sum - month") {
    val aggCol: String = "ArrDelay"
    val conditionalCol: String = "MonthI"
    val whereCondition: String = s" $conditionalCol < 3 "
    conditionalSum(aggCol, conditionalCol, whereCondition,
      filterOnMonth)
  }

  test("test for conditional sum queries - Year") {
    val aggCol: String = "ArrDelay"
    val conditionalCol: String = "YearI"
    val whereCondition: String = s" $conditionalCol = 2015 "
    val doPrint = false
    val doPrintDetailed = false
    val sdDirect = conditionalSumDirect(aggCol, whereCondition, doPrint)
    val newSD = conditionalSumOneLoop(aggCol, conditionalCol,
      whereCondition, filterOnYear, doPrint, doPrintDetailed)
    val resultsDifferent = Math.abs(sdDirect - newSD) < 1.0
    if (!newSD.isNaN && !resultsDifferent) {
      printDouble(s"Got SD $whereCondition Sum", sdDirect)
      printDouble(s"Expected SD $whereCondition Sum", newSD)
      fail()
    }

    // repeat once
    val sdDirect1 = conditionalSumDirect(aggCol, whereCondition, doPrint)
    val resultsDifferent1 = Math.abs(sdDirect - sdDirect1) < 1.0
    if (!resultsDifferent1) {
      printDouble(s"Got Repeat SD $whereCondition Sum", sdDirect1)
      printDouble(s"Expected SD $whereCondition Sum", sdDirect)
      fail()
    }

    // repeat twice
    val sdDirect2 = conditionalSumDirect(aggCol, whereCondition, doPrint)
    val resultsDifferent2 = Math.abs(sdDirect - sdDirect2) < 1.0
    if (!resultsDifferent2) {
      printDouble(s"Got Repeat SD $whereCondition Sum", sdDirect2)
      printDouble(s"Expected SD $whereCondition Sum", sdDirect)
      fail()
    }
  }

  test("test for conditional sum queries - Month") {
    val aggCol: String = "ArrDelay"
    val conditionalCol: String = "MonthI"
    val whereCondition: String = s" $conditionalCol < 3 "
    val doPrint = false
    val doPrintDetailed = false
    val sdDirect = conditionalSumDirect(aggCol, whereCondition, doPrint)
    val newSD = conditionalSumOneLoop(aggCol, conditionalCol,
      whereCondition, filterOnMonth, doPrint, doPrintDetailed)
    val resultsDifferent = Math.abs(sdDirect - newSD) < 1.0
    if (!newSD.isNaN && !resultsDifferent) {
      printDouble(s"Got SD $whereCondition Sum", sdDirect)
      printDouble(s"Expected SD $whereCondition Sum", newSD)
      fail()
    }


    // repeat once
    val sdDirect1 = conditionalSumDirect(aggCol, whereCondition, doPrint)
    val resultsDifferent1 = Math.abs(sdDirect - sdDirect1) < 1.0
    if (!resultsDifferent1) {
      printDouble(s"Got Repeat SD $whereCondition Sum", sdDirect1)
      printDouble(s"Expected SD $whereCondition Sum", sdDirect)
      fail()
    }

    // repeat twice
    val sdDirect2 = conditionalSumDirect(aggCol, whereCondition, doPrint)
    val resultsDifferent2 = Math.abs(sdDirect - sdDirect2) < 1.0
    if (!resultsDifferent2) {
      printDouble(s"Got Repeat SD $whereCondition Sum", sdDirect2)
      printDouble(s"Expected SD $whereCondition Sum", sdDirect)
      fail()
    }
  }

  def testConditionalCount( i: Int) {
    val aggCol: String = "1"
    val conditionalCol: String = "YearI"
    val whereCondition: String = s" $conditionalCol = 2015 "
    val doPrint = false
    val needError = true
    val sdDirect = conditionalCountDirect2(aggCol, whereCondition, needError, doPrint)
    val doDetailedPrint = false
    val returnSD = true
    val doPrintStrata = sdDirect.isNaN
    val newSD = conditionalCount(aggCol, conditionalCol, whereCondition,
      filterOnYear, 0, doPrint, doDetailedPrint, doPrintStrata, returnSD)
    val resultsDifferent = Math.abs(sdDirect - newSD) < 1.0
    // Expected value can be NaN: see comment in conditionalCount
    if (!newSD.isNaN && !resultsDifferent) {
      printDouble(s"$i Got SD $whereCondition Count", sdDirect)
      printDouble(s"$i Expected SD $whereCondition Count", newSD)
      fail()
    }
  }

  ignore("SNAP-739") {
    val n = 100
    // scalastyle:off println
    println(s"SNAP-739-$n :")
    // scalastyle:on println
    for (i <- 1 to n) {
      if (i > 1) writeSampleTable()
      testConditionalCount(i)
      deleteSampleTable()
      // scalastyle:off println
      print(s" $i ")
      // scalastyle:on println
    }
    // scalastyle:off println
    println(s"Done ")
    // scalastyle:on println
  }
}
