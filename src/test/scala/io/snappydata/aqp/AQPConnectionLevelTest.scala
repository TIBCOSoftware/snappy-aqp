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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import scala.util.{Failure, Success, Try}
import io.snappydata.Constant

object AQPConnectionLevelTest extends SnappySQLJob {

    override def runSnappyJob(sns: SnappySession, jobConfig: Config): Any = {
        // scalastyle:off println
        val behaviorCheck = "BehaviorCheck"
        val errorCheck = "ErrorCheck"
        val confidenceCheck = "confidenceCheck"

        def getCurrentDirectory = new java.io.File(".").getCanonicalPath

        val pw = new PrintWriter("AQPPerfResults.out")
        Try {

            val q1 = "select uniquecarrier,sum(arrdelay) as x,absolute_error(x)" +
                    ",relative_error(x) from " +
                    "airline group by uniquecarrier order by uniquecarrier with error"

            // Check HAC beahvior
            sns.sql("set spark.sql.aqp.behavior=partial_run_on_base_table")
            executeQuery(q1, behaviorCheck, "partial_run_on_base_table")

            sns.sql("set spark.sql.aqp.behavior=local_omit")
            executeQuery(q1, behaviorCheck, "local_omit")

            // Check error clause
            sns.sql("set spark.sql.aqp.error=0.3")
            executeQuery(q1, errorCheck, "0.3")

            sns.sql("set spark.sql.aqp.error=0.2")
            executeQuery(q1, errorCheck, "0.2")

            // Check confidence interval
            sns.sql("set spark.sql.aqp.confidence=0.99")
            executeQuery(q1, confidenceCheck, "0.99")

            sns.sql("set spark.sql.aqp.confidence=0.96")
            executeQuery(q1, confidenceCheck, "0.96")

        } match {
            case Success(v) => pw.close()
                s"See ${getCurrentDirectory}/AQPPerfResults.out"
            case Failure(e) => pw.close();
                throw e;
        }

        def executeQuery(q1: String, statusCheck: String, value: String): Unit = {
            val actualResult = sns.sql(s"$q1")
            sns.clearPlanCache()
            val result = actualResult.collect()
            pw.println("==========================================================" +
                    "======================")
            pw.println("\nQuery = " + q1)
            pw.println("PLAN is " + actualResult.queryExecution.sparkPlan)
            val planStr = actualResult.queryExecution.sparkPlan
            result.foreach(rs => {
                pw.println(rs.toString)
            })
            validateQueryPlan(planStr, statusCheck, value)
        }

        def validateQueryPlan(planStr: SparkPlan, statusCheck: String, aqpProperty: String):
        Unit = {
            val plan: String = planStr.toString()
            pw.println("Value is " + aqpProperty)
            if (statusCheck.equals(behaviorCheck)) {
                val inputBehaviorStr = aqpProperty.toUpperCase match {
                    case Constant.BEHAVIOR_LOCAL_OMIT => "SPECIAL_SYMBOL"
                    case Constant.BEHAVIOR_PARTIAL_RUN_ON_BASE_TABLE => "PARTIAL_ROUTING"
                    case Constant.BEHAVIOR_RUN_ON_FULL_TABLE => "REROUTE_TO_BASE"
                    case Constant.BEHAVIOR_DO_NOTHING => "DO_NOTHING"
                    case _ => ""
                }
                if (plan.contains(inputBehaviorStr)) {
                    pw.println("\nPlan reflects correct HAC behavior ,check the logs to" +
                            " verify results")
                }
                else {
                    pw.println("Input behavior is = " + inputBehaviorStr)
                    pw.println("\nHAC behavior is not reflected")

                }
            }
            else if (statusCheck.equals(errorCheck)) {
                if (plan.contains(aqpProperty)) {
                    pw.println("\nError clause " + aqpProperty + " is correctly set ,check" +
                            " the logs " +
                            "to verify results")
                }
                else {
                    pw.println("Values is " + aqpProperty)
                    pw.println("\nError is not set properly")
                }
            }
            else {
                if (plan.contains(aqpProperty)) {
                    pw.println("\nConfidence Interval " + aqpProperty + " is correctly set ," +
                            "check the logs to verify results")
                }
                else {
                    pw.println("Values is " + aqpProperty)
                    pw.println("\nConfidence Interval is not set properly")
                }
                // do confidence validation
            }
        }
    }

    override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation
    = SnappyJobValid()
}
