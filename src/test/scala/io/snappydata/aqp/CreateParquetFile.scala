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
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import scala.util.Try

object CreateParquetFile extends SnappySQLJob {

    override def runSnappyJob(sns: SnappySession, jobConfig: Config): Any = {
        Try {
            // First need to correct the column names in the csv file  according to the columnnames
            // used in the create table script.
            val hfile = ""
            val saveLoc = ""
            val airlineDataFrame = sns.read
                    .format("com.databricks.spark.csv") // CSV to DF package
                    .option("header", "true") // Use first line of all files as header
                    .option("inferSchema", "true")// Automatically infer data types
                    .load(hfile)

            airlineDataFrame.write.parquet(saveLoc)
        }
    }

    override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation
    = SnappyJobValid()
}


