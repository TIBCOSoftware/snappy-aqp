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

import com.typesafe.config.Config
import org.apache.spark.sql.{SaveMode, _}

import scala.util.{Failure, Success, Try}

object AQPQueryRouting extends SnappySQLJob {
    override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
        val createTableExtension = jobConfig.getString("createTableExtension")
        val numIter = jobConfig.getString("numIter").toInt
        val skipTill = jobConfig.getString("warmup").toInt
        val queryFile: String = jobConfig.getString("queryFile");
        val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
        val execTimeArray = new Array[Double](queryArray.length)
        val sampleFraction: Double = jobConfig.getString("sampleFraction").toDouble
        val qcsParam1 = jobConfig.getString("qcsParam1")
        val redundancy = jobConfig.getString("redundancy").toInt
        val hfile = jobConfig.getString("dataLocation")

        // The Schema
        val ddlStr_1995To2007 = "(Year_ INTEGER NOT NULL ," +
                "Month_ INTEGER NOT NULL ," +
                "DayOfMonth INTEGER NOT NULL ," +
                "DayOfWeek INTEGER NOT NULL," +
                "UniqueCarrier VARCHAR(20) NOT NULL ," +
                "TailNum VARCHAR(20)," +
                "FlightNum INTEGER," +
                "Origin VARCHAR(20)," +
                "Dest VARCHAR(20)," +
                "CRSDepTime INTEGER," +
                "DepTime INTEGER," +
                "DepDelay INTEGER," +
                "TaxiOut INTEGER," +
                "TaxiIn INTEGER," +
                "CRSArrTime VARCHAR(20)," +
                "ArrTime VARCHAR(20)," +
                "ArrDelay INTEGER," +
                "Cancelled VARCHAR(20)," +
                "CancellationCode VARCHAR(20)," +
                "Diverted INTEGER," +
                "CRSElapsedTime INTEGER," +
                "ActualElapsedTime INTEGER," +
                "AirTime INTEGER," +
                " Distance INTEGER," +
                "CarrierDelay VARCHAR(20)," +
                "WeatherDelay VARCHAR(20)," +
                "NASDelay VARCHAR(20)," +
                "SecurityDelay VARCHAR(20)," +
                "LateAircraftDelay VARCHAR(20)," +
                "ArrDelaySlot VARCHAR(20))"

        val ddlStr = "(Year_ INTEGER NOT NULL ," +
                "Month_ INTEGER NOT NULL ," +
                "DayOfMonth INTEGER NOT NULL ," +
                "DayOfWeek INTEGER NOT NULL," +
                "DepTime INTEGER," +
                "CRSDepTime INTEGER," +
                "ArrTime INTEGER," +
                "CRSArrTime INTEGER," +
                "UniqueCarrier VARCHAR(20) NOT NULL ," +
                "FlightNum INTEGER,TailNum VARCHAR(20)," +
                "ActualElapsedTime INTEGER," +
                "CRSElapsedTime INTEGER," +
                "AirTime INTEGER," +
                "ArrDelay INTEGER," +
                "DepDelay INTEGER," +
                "Origin VARCHAR(20)," +
                "Dest VARCHAR(20)," +
                "Distance INTEGER," +
                "TaxiIn INTEGER," +
                "TaxiOut INTEGER," +
                "Cancelled INTEGER," +
                "CancellationCode VARCHAR(20)," +
                "Diverted INTEGER," +
                "CarrierDelay INTEGER," +
                "WeatherDelay INTEGER," +
                "NASDelay INTEGER," +
                "SecurityDelay INTEGER," +
                "LateAircraftDelay INTEGER," +
                "ArrDelaySlot INTEGER)"

        // scalastyle:off println
        def getCurrentDirectory = new java.io.File(".").getCanonicalPath

        val pw = new PrintWriter("AQPQueryRoutingTestSampleTable.out")
        val pw1 = new PrintWriter("AQPQueryRoutingTestBaseTable.out")
        val airlineDataFrame: DataFrame = snc.read.load(hfile)

        // Create an empty table with VARCHAR datatype instead of String
        snc.sql("DROP TABLE IF EXISTS AIRLINE")
        snc.sql(s"CREATE TABLE AIRLINE $ddlStr using $createTableExtension")

        // Populate the AIRLINE table as row table
        airlineDataFrame.write.format(createTableExtension).mode(SaveMode.Append).
                saveAsTable("AIRLINE")

        println("Created airline table")

        // Create an index on column 'UNIQUECARRIER' if row table
        if (createTableExtension.equals("row")) {
            snc.sql("CREATE INDEX UNIQUECARRIER_INDEX on AIRLINE(UNIQUECARRIER)")
        }

        println("Creating sample table")

        snc.createSampleTable("airline_Sample",
            Some("airline"), Map("qcs" -> s"$qcsParam1", "REDUNDANCY" -> s"$redundancy",
                "fraction" -> s"$sampleFraction", "strataReservoirSize" -> "25",
                "buckets" -> "57", "overflow" -> "false"),
            allowExisting = false)
        Try {
            AQPPerfTestUtil.runPerftest(numIter, snc, pw, queryArray, skipTill, execTimeArray)
        } match {
            case Success(v) => pw.close()
                s"See ${getCurrentDirectory}/AQPQueryRoutingTestSampleTable.out"
            case Failure(e) => pw.close();
                throw e;
        }
    }

    override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation =
        SnappyJobValid()
}
