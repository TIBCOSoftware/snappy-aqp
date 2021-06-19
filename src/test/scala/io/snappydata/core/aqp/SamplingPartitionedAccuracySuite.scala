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
package io.snappydata.core.aqp

import org.apache.spark.sql.SaveMode

/**
  * Created by vivekb on 6/11/16.
  */
class SamplingPartitionedAccuracySuite extends SamplingAccuracySuite {

  protected val createTableOptions: String =
    s" using column options (partition_by 'DayOfMonth', Buckets '8') "

  override def setupData(): Unit = {
    snc.sql("set spark.sql.shuffle.partitions=6")

    snc.sql(s"drop table if exists airlineSampled")
    snc.sql(s"drop table if exists airline")

    snc.sql(s"create table if not exists airline $ddlStr " +
        s" $createTableOptions ")

    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val airlineDataFrame = snc.read.load(hfile)
    airlineDataFrame.write.insertInto(s"airline")

    val start1 = System.currentTimeMillis
    snc.sql(s"CREATE TABLE airlineSampled $ddlStr" +
        "USING column_sample " +
        "options " +
        "(" +
        s"qcs '$qcsColumns'," +
        "BUCKETS '8'," +
        s"fraction '$sampleRatio'," +
        s"strataReservoirSize '$strataReserviorSize', " +
        "baseTable 'airline')")

    val start2 = System.currentTimeMillis
  /*  airlineDataFrame.write.
        format("column_sample")
        .mode(SaveMode.Append)
        .saveAsTable("airlineSampled") */

   // val start3 = System.currentTimeMillis
    logInfo(s"Time sample ddl=${start2-start1}ms ")
  }

}
