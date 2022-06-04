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
import org.apache.spark.sql.{DataFrame, SaveMode, SnappySession, SnappyJobValid, SnappyJobValidation, SnappySQLJob}

import scala.util.{Failure, Success, Try}

object PopulateDataJob extends SnappySQLJob {
    override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
        def getCurrentDirectory = new java.io.File(".").getCanonicalPath

        val dataLocation = jobConfig.getString("dataLocation")
        val pw = new PrintWriter("PopulateDataJob.out")
        Try {
            val tempTable = "tempcoltable"
            val numIter = 47
            var i: Double = 0
            while (i <= numIter) {
                val airlineDF1: DataFrame = snc.sql("select * from airline")
                airlineDF1.registerTempTable(tempTable)
                airlineDF1.write.format("column").mode(SaveMode.Append).saveAsTable("airline1")
                i = i + 1
                snc.dropTable(tempTable)
            }
            val df = snc.sql("SELECT * from airline1")
        } match {
            case Success(v) => pw.close()
                s"See ${getCurrentDirectory}/PopulateDataJob.out"
            case Failure(e) => pw.close();
                throw e;
        }
    }

    override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation
    = SnappyJobValid()

}

