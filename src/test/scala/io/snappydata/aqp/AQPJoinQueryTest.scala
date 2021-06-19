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
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object AQPJoinQueryTest extends SnappySQLJob {

    override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
        val numIter = jobConfig.getString("numIter").toInt
        val skipTill = jobConfig.getString("warmup").toInt
        val queryFile: String = jobConfig.getString("queryFile")
        val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
        val baseQueryFile: String = jobConfig.getString("baseQueryFile")
        val baseQueryArray = scala.io.Source.fromFile(baseQueryFile).getLines().mkString.split(";")
        val execBaseTimeArray = new Array[Double](baseQueryArray.length)
        val copartitioned: Boolean = jobConfig.getString("copartitioned").toBoolean
        val replicated: Boolean = jobConfig.getString("replicated").toBoolean
        val dataLocation = jobConfig.getString("dataLocation")
        val refDataLocation = jobConfig.getString("refDataLocation")
        val sampleFraction: Double = jobConfig.getString("sampleFraction").toDouble
        val qcsParam1 = jobConfig.getString("qcsParam1")
        var tableType = "column"
        val perfTest: Boolean = jobConfig.getString("perfTest").toBoolean
        val execTimeArray = new Array[Double](queryArray.length)
        val isJoinQuery: Boolean = jobConfig.getString("isJoinQuery").toBoolean
        // scalastyle:off println

        def getCurrentDirectory = new java.io.File(".").getCanonicalPath

        val prop = Map[String, String]()
        var props = Map[String, String]()
        if (copartitioned) {
            props = Map("PARTITION_BY" -> "FlighTieStr")
        }
        val i = 0
        if(i%1000 == 0){

        }

        if (replicated) {
            tableType = "row"
        }
        val pw = new PrintWriter("AQPJoinQueryTestSample.out")
        val pw1 = new PrintWriter("AQPJoinQueryTestBase.out")
        // Drop tables if exists
        snc.sql("DROP TABLE IF EXISTS FlightData_sample")
        snc.sql("DROP TABLE IF EXISTS STAGING_AIRLINE")
        snc.sql("DROP TABLE IF EXISTS AIRLINE")
        snc.sql("DROP TABLE IF EXISTS airlineRef")
        snc.sql("DROP TABLE IF EXISTS Flight")
        snc.sql("DROP TABLE IF EXISTS FlightData")
        snc.sql("DROP TABLE IF EXISTS TempTable")

        snc.sql(s"CREATE EXTERNAL TABLE STAGING_AIRLINE USING parquet " +
                s"OPTIONS(path '$dataLocation')")
        snc.sql(s"CREATE TABLE AIRLINE USING column OPTIONS(buckets '11') AS ( " +
                " SELECT Year AS Year_, Month AS Month_ , DayOfMonth," +
                " DayOfWeek ,UniqueCarrier ,TailNum,FlightNum,Origin,Dest," +
                "CRSDepTime,DepTime,DepDelay,TaxiOut,TaxiIn," +
                "CRSArrTime," +
                "ArrTime,ArrDelay,Cancelled,CancellationCode ," +
                "Diverted,CRSElapsedTime,ActualElapsedTime,AirTime,Distance" +
                " ,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay" +
                ",ArrDelaySlot  FROM STAGING_AIRLINE)")

        val df = snc.sql("select * from airline")
        println("Created AIRLINE Table")
        println("The total df count is " + df.count())
        var newdf: DataFrame = null

        newdf = df.withColumn("FlighTieStr", monotonically_increasing_id())

        val col0 = newdf.schema.fieldNames(0)
        val col1 = newdf.schema.fieldNames(1)
        val col2 = newdf.schema.fieldNames(2)
        val col3 = newdf.schema.fieldNames(3)
        val col4 = newdf.schema.fieldNames(4)
        val col10 = newdf.schema.fieldNames(10)
        val col16 = newdf.schema.fieldNames(16)
        val col7 = newdf.schema.fieldNames(7)
        val col8 = newdf.schema.fieldNames(8)
        val col5 = newdf.schema.fieldNames(30)

        println("newdf schema is " + newdf.printSchema())

        // Create temp df
        val tempdfSchema = StructType(Seq(StructField("Year_", IntegerType),
            StructField("Month_", IntegerType), StructField("DayOfMonth", IntegerType),
            StructField("DayOfWeek", IntegerType), StructField("UniqueCarrier", StringType),
            StructField("DepTime", IntegerType), StructField("ArrDelay", IntegerType),
            StructField("Origin", StringType), StructField("Dest", StringType),
            StructField("FlighTieStr", LongType)))

        val encoder = RowEncoder(tempdfSchema)

        newdf = newdf.na.fill(0.0, Seq("ArrDelay"))
        // newdf = newdf.na.fill("",Seq("Origin","Dest"))

        val tempdf = newdf.select(col0, col1, col2, col3, col4, col10, col16, col7,
            col8, col5).map(row => {
            var str: String = ""
            var cnt: Integer = -10 // change cnt default value to
            // -10 for creating different tieString

            val newRow = row.toSeq.map {
                case i: Integer => i
                case s: String => s
                case d: Double => d
                case l: Long => l
                case _ => 0

            }

            Row.fromSeq(newRow)
        })(encoder)

        println("tempdf schema is " + tempdf.printSchema())
        snc.createTable("tempTable", "column", tempdf.schema, props)
        tempdf.write.format("column").mode(SaveMode.Append).saveAsTable("tempTable")

        // Derive the columns from tempdf
        // Create Flight DataFrame.
        val dfFlightSchema = StructType(Seq(StructField("Year_", IntegerType),
            StructField("Month_", IntegerType), StructField("DayOfMonth", IntegerType),
            StructField("DayOfWeek", IntegerType), StructField("UniqueCarrier", StringType),
            StructField("FlighTieStr", LongType)))

        val encoder1 = RowEncoder(dfFlightSchema)

        val dfFlightTable = tempdf.select(col0, col1, col2, col3, col4, col5).map(row => {
            // var str: String = ""
            val newRow = row.toSeq.map {
                case i: Integer => i
                case s: String => s
                case d: Double => d
                case l: Long => l
                case _ =>
            }

            Row.fromSeq(newRow)
        })(encoder1)

        dfFlightTable.printSchema()
        newdf.printSchema()

        // Create Flight Base table
        snc.createTable("Flight", "column", dfFlightTable.schema, props)
        dfFlightTable.write.format("column").mode(SaveMode.Append).saveAsTable("Flight")

        val dfFlightDataSchema = StructType(Seq(StructField("ArrDelay", IntegerType),
            StructField("Origin", StringType), StructField("Dest", StringType),
            StructField("UniqueCarrier", StringType), StructField("FlighTieStr", LongType)))

        val encoder2 = RowEncoder(dfFlightDataSchema)
        val dfFlightDataTable = tempdf.select(col16, col7, col8, col4, col5).map { row =>
            val newRow = row.toSeq.map {
                case i: Integer => i
                case s: String => s
                case d: Double => d
                case l: Long => l
                case _ =>
            }
            Row.fromSeq(newRow)

        }(encoder2)

        dfFlightDataTable.show()

        // Create FlightData base table
        val flightDataDF = snc.createTable("FlightData", "column", dfFlightDataTable.schema, props)
        dfFlightDataTable.write.format("column").mode(SaveMode.Append).saveAsTable("FlightData")
        println("Created FlightData Table")

        // Create FlightDataSample table
        val sampledf = snc.createSampleTable("FlightData_sample",
            Some("FlightData"), Map("qcs" -> s"$qcsParam1",
                "fraction" -> s"$sampleFraction", "strataReservoirSize" -> "25",
                "buckets" -> "57", "overflow" -> "false"),
            allowExisting = false)
        println("Created FlightData_sample Table")


        // Create airlineRef table
        val airlineRefDF = snc.read.load(refDataLocation)
        snc.createTable("airlineRef", "row", airlineRefDF.schema, prop)
        airlineRefDF.write.format("row").mode(SaveMode.Append).saveAsTable("airlineRef")
        println("Created airlineRef Table")

        snc.sql("DROP TABLE IF EXISTS STAGING_AIRLINE")
        snc.sql("DROP TABLE IF EXISTS AIRLINE")
        snc.sql("DROP TABLE IF EXISTS TempTable")

        Try {
            AQPPerfTestUtil.runPerftest(numIter, snc, pw1, baseQueryArray,
                skipTill, execBaseTimeArray)
        }
        match {
            case Success(v) => pw1.close();
            case Failure(e) => pw1.close();
                throw e;
        }

        Try {
            AQPPerfTestUtil.runPerftest(numIter, snc, pw, queryArray, skipTill, execTimeArray,
                replicated, perfTest, isJoinQuery)
        } match {
            case Success(v) => pw.close()
                s"See ${getCurrentDirectory}/AQPJoinQueryTestSample.out"
            case Failure(e) => pw.close();
                throw e;
        }
    }

    override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation
    = SnappyJobValid()
}

