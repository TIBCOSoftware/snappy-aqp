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
import scala.util.{Failure, Success, Try}
import com.typesafe.config.Config
import org.apache.spark.sql._


/** This test will let us know the cost of sampling
  */
object AQPPerfTestSampleTableWOE extends SnappySQLJob {

    override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
        val numIter = jobConfig.getString("numIter").toInt
        val dataLocation = jobConfig.getString("dataLocation")
        val skipTill = jobConfig.getString("warmup").toInt
        val queryFile: String = jobConfig.getString("queryFile")
        val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
        val execTimeArray = new Array[Double](queryArray.length)

        def getCurrentDirectory = new java.io.File(".").getCanonicalPath

        val props = Map[String, String]()
        val pw = new PrintWriter("AQPPerfTestSampleTableWOE.out")

        // scalastyle:off println

        pw.println("Creating AIRLINE table")
        val df = snc.read.load(dataLocation)
        snc.createTable("airline", "column", df.schema, props)
        df.write.format("column").mode(SaveMode.Append).saveAsTable("airline")
        println("Created Airline base  table")

        pw.println("Creating AIRLINE_SAMPLE table")
        println("Creating AIRLINE_SAMPLE table")
        val sampledf = snc.createSampleTable("airline_sample",
            Some("airline"), Map("qcs" -> "UniqueCarrier ,Year ,Month",
                "fraction" -> "0.05", "strataReservoirSize" -> "25", "buckets" -> "57"),
            allowExisting = false)
        println(" Base table schema is " + df.printSchema())
        println(" Sample Table schema is " + sampledf.printSchema())


        println("Created AIRLINE_SAMPLE table")

        pw.println("Creating SampleTable_WOE table")
        println("Creating SampleTable_WOE table")
        val sampleWOE_df = snc.sql("Select * from airline_sample")
        snc.createTable("sampletable_WOE", "column", sampledf.schema, Map("eviction_by" -> "NONE"))
        sampleWOE_df.write.insertInto("sampletable_WOE")
        println("Created SampleTable_WOE table")

        val df1: DataFrame = snc.sql(s"select count(*) from sampletable_WOE")
        df1.collect()
        println("Finished table creation")

        Try {
            AQPPerfTestUtil.runPerftest(numIter, snc, pw, queryArray, skipTill, execTimeArray)
        } match {
            case Success(v) => pw.close()
                s"See ${getCurrentDirectory}/AQPPerfTestSampleTableWOE.out"
            case Failure(e) => pw.close()
                throw e;
        }
    }

    override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation =
      SnappyJobValid()
}


