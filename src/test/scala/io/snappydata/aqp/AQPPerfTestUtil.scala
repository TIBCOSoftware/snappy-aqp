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
package io.snappydata.aqp

import java.io.PrintWriter

import org.apache.spark.AssertAQPAnalysis
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.{DataFrame, SaveMode, SnappySession, SnappyJobValid, SnappyJobValidation, SnappySQLJob}

import scala.util.{Sorting, Try}

object AQPPerfTestUtil {
  // scalastyle:off println
  val props = Map[String, String]()

  def createTables(sns: SnappySession, qcsParam1: String, qcsParam2: String,
      qcsParam3: String,
      sampleFraction: Double, dataLocation: String, redundancy: Int = 0,
      baseTableName: String = "airline",
      sampleTableName: String = "airline_sample", isDropTable: Boolean = true):
  Unit = {

    if (isDropTable) {
      sns.dropTable("STAGING_AIRLINE", ifExists = true)
      sns.dropTable(sampleTableName, ifExists = true)
      sns.dropTable(baseTableName, ifExists = true)
      sns.sql(s"CREATE EXTERNAL TABLE STAGING_AIRLINE USING parquet" +
          s" OPTIONS(path '$dataLocation')")
      sns.sql(s"CREATE TABLE $baseTableName USING column OPTIONS(partition_by 'DayOfMonth'," +
          s"redundancy '$redundancy',buckets '11')" +
          s" AS ( " +
          " SELECT Year AS Year_, Month AS Month_ , DayOfMonth," +
          " DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime," +
          " UniqueCarrier, FlightNum, TailNum, ActualElapsedTime," +
          " CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin," +
          " Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode," +
          " Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay," +
          " LateAircraftDelay, ArrDelaySlot" +
          " FROM STAGING_AIRLINE)")

      val tempTable = "tempcoltable"
      val n = 8 // 47
      var i: Double = 0
      while (i <= n) {
        val airlineDF1: DataFrame = sns.sql("select * from airline")
        airlineDF1.registerTempTable(tempTable)
        airlineDF1.write.format("column").mode(SaveMode.Append).saveAsTable("airline1")
        i = i + 1
        sns.dropTable(tempTable)
      }
    }
    sns.dropTable(sampleTableName, ifExists = true)
    val df = sns.sql("SELECT * from airline1")
    // val df = sns.sql(s"select * from $baseTableName")
    val sampledf = sns.createSampleTable(s"$sampleTableName",
      Some("airline1"), Map("qcs" -> s"$qcsParam1,$qcsParam2,$qcsParam3",
        "fraction" -> s"$sampleFraction", "strataReservoirSize" ->
            "25", "buckets" -> "57", "overflow" -> "false"),
      allowExisting = false)
    df.write.insertInto(s"$sampleTableName")
  }

  def runPerftest(numIter: Int, sns: SnappySession, pw: PrintWriter, queryArray: Array[String],
      skipTill: Integer, execTimeArray: Array[Double], isReplicated: Boolean = false,
      isPerfTest: Boolean = false, isJoinQuery: Boolean = false):
  Unit = {
    println("Inside runPerfTest task")
    val pw1 = new PrintWriter("AvgExecutiontime.txt")

    pw.println("PerfTest will run for " + numIter + " iterations and first " + skipTill + " " +
        "itertions will be skipped")
    pw.println()
    val actualIter = numIter - skipTill
    val tempQueryArray = Array.ofDim[Double](queryArray.length, numIter)
    for (i <- 1 to numIter) {
      sns.sql("set spark.sql.shuffle.partitions=6")
      pw.println("Starting iterration[" + i + "] and query length is " + queryArray.length)
      pw.println()
      for (j <- 0 to queryArray.length - 1) {
        val tempIterrArray = tempQueryArray(j)
        if (i < skipTill) {
          executeQuery(queryArray(j), 0, i, j, tempIterrArray, isReplicated,
            isPerfTest, isJoinQuery)
        } else {
          execTimeArray(j) = executeQuery(queryArray(j),
            execTimeArray(j), i, j, tempIterrArray,
            isReplicated, isPerfTest, isJoinQuery)
        }

      }
      pw.println()

    }

    def executeQuery(query: String, execTime: Double, iterr: Int, queryNo: Int, arr:
    Array[Double], isReplicated: Boolean, isPerfTest: Boolean, isJoinQuery: Boolean): Double = {
      val queryType = getQueryType(query)
      val start = System.currentTimeMillis
      sns.clearPlanCache()
      val actualResult = sns.sql(query)
      pw.println("=========================================================================" +
          "=======")
      val result = actualResult.collect()
      var excTime = execTime
      val totalTime1 = (System.currentTimeMillis - start)
      // closedFormAnalysis(sns.sqlContext)
      pw.println(s"Query${queryNo + 1}  ${queryType} on " +
          s"table took  = ${totalTime1}ms")
      pw.println("Query = " + query)
      result.foreach(rs => {
        pw.println(rs.toString)
      })
      excTime += totalTime1
      arr(iterr - 1) = totalTime1
      tempQueryArray(queryNo)(iterr - 1) = arr(iterr - 1)

      if (iterr == numIter) {
        pw1.println(queryType + "=" + query)
        pw1.println("Results are: ")
        result.foreach(rs => {
          pw1.println(rs.toString)
        })
      }
      if (!isPerfTest) {
        pw.println("PLAN is " + actualResult.queryExecution.sparkPlan)
      }
      if (isJoinQuery) {
        if (queryType.equals("SampleTable query") && isReplicated) {
          pw.println("SP1: Query is " + query)
          val aggrStr = "SUM"
          val analysisType = AssertAQPAnalysis.getAnalysisType(sns.sqlContext)
          // println(" Analysis Type is " + analysisType.toString)
          if (isReplicated && query.contains(aggrStr)) {
            if (analysisType.toString == "Some(Closedform)") {
              pw.println("Query = " + query + " Analysis Type is " +
                  analysisType.toString)
            }
            else {
              sys.error("Error Query = " + query + " Join query with one sample" +
                  " table & another ref table which is replicated row table," +
                  " should use closedform analysis for sum aggregate \n" +
                  " Analysis Type is " + analysisType.toString)
            }
          }
          else {
            if (analysisType.toString == "Some(Bootstrap)") {
              println("Query = " + query + " Analysis Type is " +
                  analysisType.toString)
            }
            else {
              sys.error("Error Query = " + query + " Analysis Type is " +
                  analysisType.toString)
            }
          }
        }
      }

      return excTime
    }

    def getQueryType(query: String): String = {
      var queryType = ""
      if (query.contains("with error")) {
        queryType = "SampleTable query"
      }
      else {
        queryType = "BaseTable query"
      }

      return queryType
    }

    def calculateMedian(arr: Array[Double]): Double = {
      // (n+1)/2
      Sorting.quickSort(arr)
      val n = arr.length
      println("Array length is " + n)
      val median = (n + 1) / 2
      println("median value is " + median)
      return arr(median - 1)
    }

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // Calculate avg time taken to execute a particular query
    pw1.println("\n The average query execution time are: ")

    for (i <- 0 to queryArray.length - 1) {
      val medianValue = calculateMedian(tempQueryArray(i))
      val meanQueryTime = execTimeArray(i) / (numIter - skipTill + 1)
      val queryType = getQueryType(queryArray(i))
      pw1.println(s"${queryType} ${queryArray(i)} took = ${meanQueryTime}ms" +
          s" as a average over" +
          s" ${actualIter} iterrations \n Median ${medianValue}")
      pw.println(s"${queryType} ${queryArray(i)} took = ${meanQueryTime}ms" +
          s" as a average over" +
          s" ${actualIter} iterrations \n Median ${medianValue}")
      pw.println()

    }
    pw1.close()
    pw.println()
    pw.println(s"=====================================================")
    pw.close()
  }

  // scalastyle:on println
}

