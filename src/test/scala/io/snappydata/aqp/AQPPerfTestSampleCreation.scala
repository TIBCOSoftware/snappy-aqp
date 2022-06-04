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
package io.snappydata.aqp

import java.io.PrintWriter

import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.{SaveMode, SnappySession, SnappyJobValid, SnappyJobValidation, SnappySQLJob}
import scala.util.{Failure, Success, Try}

object AQPPerfTestSampleCreation extends SnappySQLJob {

  override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
    var execTimeddl: Double = 0.0
    val execTimeSampleTable: Double = 0.0
    val dataFile: String = jobConfig.getString("dataLocation")
    val baseTable: String = jobConfig.getString("baseTable")
    val sampleTable: String = jobConfig.getString("sampleTable")
    val qcsCol: String = jobConfig.getString("qcsParam1")
    val fraction: Double = jobConfig.getString("fraction").toDouble
    val outFile = jobConfig.getString("outfile")
    val outPutFile = outFile + System.currentTimeMillis().toString()

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // scalastyle:off println

    val pw = new PrintWriter(s"${outPutFile}.txt")

    Try {
      val df = snc.read.load(dataFile)
      val stagingTable = s"$baseTable" + "_staging"
      snc.createExternalTable(stagingTable, "PARQUET", df.schema,
        Map.empty[String, String])
      df.write.insertInto(stagingTable)

      val baseTableDF = snc.createTable(baseTable, "column", df.schema,
        Map.empty[String, String])
      df.write.insertInto(baseTable)

      println("Finished creating airline table")
      createSampleTableUsingAPI()

      def createSampleTableUsingAPI(): Unit = {
        val sampleDF = snc.createSampleTable(sampleTable, Some(baseTable),
          Map("qcs" -> s"$qcsCol", "fraction" -> s"$fraction",
            "strataReservoirSize" -> "50"), allowExisting = false)
        println("Total row count " + sampleDF.count())
        val sampleCount = snc.sql("select count(*) sample_ from " + sampleTable)
        val result = sampleCount.collect()
        result.foreach(rs => {
          println("The sample  count is " + rs.toString)
        })
        executeQueriesUsingAPI()
      }

      def executeQueriesUsingAPI(): Unit = {
        val q1 = snc.table(baseTable).groupBy("hack_license").count().
            withError(.2).sort(col("count").desc).limit(10)
        println("Query1 =  " + q1.show())
        pw.println("Query 1 = " + q1.show())
        val q2 = snc.table(baseTable).groupBy("hack_license", "pickup_datetime").
            agg(Map("trip_distance" -> "sum")).alias("daily_trips").
            filter(year(col("pickup_datetime")).equalTo(2013)
                and month(col("pickup_datetime")).equalTo(9)).
            withError(0.2).sort(col("sum(trip_distance)").desc).limit(10)
        println("Query2 = " + q2.show())
        pw.println("Query 2 = " + q2.show())
      }

      // Calculate avg time taken to execute a particular query
      pw.println(s"Sample creation  took = ${execTimeSampleTable}ms as a")
      pw.println()
      pw.println(s"=====================================================")
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${outPutFile}.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation
  = SnappyJobValid()
}

