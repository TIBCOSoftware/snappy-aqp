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

import java.io.{PrintWriter}
import scala.util.{Failure, Success, Try}
import com.typesafe.config.Config
import org.apache.spark.sql._

object AQPPerfTest extends SnappySQLJob {

    override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
        val numIter = jobConfig.getString("numIter").toInt
        val skipTill = jobConfig.getString("warmup").toInt
        val queryFile: String = jobConfig.getString("queryFile");
        val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
        val execTimeArray = new Array[Double](queryArray.length)
        val dataLocation = jobConfig.getString("dataLocation")
        /* "s3a:// **REMOVED**:
        **REMOVED**@zeppelindemo/1995_2015AirlineData" */
        val qcsParam1 = jobConfig.getString("qcsParam1")
        val qcsParam2 = jobConfig.getString("qcsParam2")
        val qcsParam3 = jobConfig.getString("qcsParam3")
        val sampleFraction: Double = jobConfig.getString("sampleFraction").toDouble
        val redundancy = jobConfig.getString("redundancy").toInt
        val perfTest: Boolean = jobConfig.getString("perfTest").toBoolean
        val dropTable: Boolean = jobConfig.getString("dropTable").toBoolean

        // scalastyle:off println
        def getCurrentDirectory = new java.io.File(".").getCanonicalPath

        val pw = new PrintWriter("AQPPerfResults.out")

        pw.println("Creating AIRLINE table")
        AQPPerfTestUtil.createTables(snc, qcsParam1, qcsParam2, qcsParam3,
            sampleFraction, dataLocation, redundancy, "airline", "airline_sample", dropTable)
        Try {
            AQPPerfTestUtil.runPerftest(numIter, snc, pw, queryArray, skipTill, execTimeArray,
                false, perfTest)
        } match {
            case Success(v) => pw.close()
                s"See ${getCurrentDirectory}/AQPPerfResults.out"
            case Failure(e) => pw.close();
                throw e;
        }
    }

    override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation
    = SnappyJobValid()
}


