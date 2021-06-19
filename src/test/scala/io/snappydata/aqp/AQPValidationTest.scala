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

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySession, SnappySQLJob}
import java.io.File
import sys.process._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object AQPValidationTest extends SnappySQLJob {

    override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
        val baseScriptDir = jobConfig.getString("baseScriptDir")
        val queryFile: String = jobConfig.getString("queryFile");
        val queryArray = scala.io.Source.fromFile(queryFile).getLines().mkString.split(";")

        // scalastyle:off println
        def getCurrentDirectory = new java.io.File(".").getCanonicalPath

        val path = getCurrentDirectory
        println("Path is : " + path)
        val folder = new File(path)
        Try {
            for (file <- folder.listFiles.filter(_.getName.endsWith(".out"))) {
                for (i <- 0 until queryArray.length) {
                    val startQueryRange = "Query" + (i + 1)
                    var endQueryRange = "Query" + (i + 2)
                    if (i.equals(queryArray.length - 1)) {
                        println("The value of i is " + i)
                        endQueryRange = "^$"
                    }

                    val inputFile = path + "/" + file.getName()
                    val resultFile = path + "/" + startQueryRange + ".txt"
                    val finalResult = path + "/FinalResult"
                    println("The parameters for the sortScript1.sh files are startQueryRange "
                            + startQueryRange + " endQueryRange" + endQueryRange + " inputFile"
                            + inputFile +" resultFile " + resultFile + " finalResult" + finalResult)
                    val command = s"./sortScript1.sh ${startQueryRange} ${endQueryRange} " +
                            s"${inputFile} ${resultFile} ${finalResult}"
                    println("The final command is " + command)
                    Process(command, new File(baseScriptDir)).!
                }
            }
        }
        match {
            case Success(v) => s"See results in ${getCurrentDirectory}"
            case Failure(e) => throw e;
        }
    }

    override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation
    = SnappyJobValid()
}
