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

import com.typesafe.config.Config

import org.apache.spark.sql._

object AQPTableCreation extends SnappySQLJob {

  override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
    val dataLocation = jobConfig.getString("dataLocation")
    val qcsParam1 = jobConfig.getString("qcsParam1")
    val qcsParam2 = jobConfig.getString("qcsParam2")
    val qcsParam3 = jobConfig.getString("qcsParam3")
    val sampleFraction: Double = jobConfig.getDouble("sampleFraction")
    val redundancy = jobConfig.getString("redundancy").toInt

    AQPPerfTestUtil.createTables(snc, qcsParam1, qcsParam2, qcsParam3, sampleFraction,
      dataLocation, redundancy)
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation =
    SnappyJobValid()
}
