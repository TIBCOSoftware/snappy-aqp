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
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import scala.util.{Failure, Success, Try}

object AQPExecuteQueryTest extends SnappySQLJob {

  override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
    // scalastyle:off println
    val numIter = jobConfig.getString("numIter").toInt
    val skipTill = jobConfig.getString("warmup").toInt
    val queryFile: String = jobConfig.getString("queryFile")
    println("SP: queryFile path is " + queryFile)
    val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")
    val execTimeArray = new Array[Double](queryArray.length)
    val outFile = jobConfig.getString("outfile")
    val outPutFile = outFile + System.currentTimeMillis().toString()


    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val pw = new PrintWriter(s"${outPutFile}.out")

    Try {
      AQPPerfTestUtil.runPerftest(numIter, snc, pw, queryArray, skipTill, execTimeArray)
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
