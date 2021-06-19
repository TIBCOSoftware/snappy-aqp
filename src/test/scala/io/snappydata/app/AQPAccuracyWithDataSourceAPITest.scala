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
package io.snappydata.app

class AQPAccuracyWithDataSourceAPITest extends AQPAccuracyTest {

  val ddlStr: String = "(YearI INT," + // NOT NULL
    "MonthI INT," + // NOT NULL
    "DayOfMonth INT," + // NOT NULL
    "DayOfWeek INT," + // NOT NULL
    "DepTime INT," +
    "CRSDepTime INT," +
    "ArrTime INT," +
    "CRSArrTime INT," +
    "UniqueCarrier VARCHAR(20)," + // NOT NULL
    "FlightNum INT," +
    "TailNum VARCHAR(20)," +
    "ActualElapsedTime INT," +
    "CRSElapsedTime INT," +
    "AirTime INT," +
    "ArrDelay INT," +
    "DepDelay INT," +
    "Origin VARCHAR(20)," +
    "Dest VARCHAR(20)," +
    "Distance INT," +
    "TaxiIn INT," +
    "TaxiOut INT," +
    "Cancelled INT," +
    "CancellationCode VARCHAR(20)," +
    "Diverted INT," +
    "CarrierDelay INT," +
    "WeatherDelay INT," +
    "NASDelay INT," +
    "SecurityDelay INT," +
    "LateAircraftDelay INT," +
    "ArrDelaySlot INT)"

  override protected def loadSampleTable(): Unit = {
    snc.sql(s"CREATE SAMPLE TABLE $sampleTableName " +
      s"ON $tableName options " +
      "(" +
      "qcs 'UniqueCarrier,YearI,MonthI'," +
      "fraction '0.06'," +
      s"strataReservoirSize '1000') AS (select * from $tableName)")
    totalNumberOfRowsInSampleTable = snc.sql(s"select * from $sampleTableName").collect().length
  }

  /*
  override protected def loadSampleTable(): Unit = {

    snc.sql(s"CREATE SAMPLE TABLE $sampleTableName $ddlStr" +
      "options " +
      "(" +
      "qcs 'UniqueCarrier,YearI,MonthI'," +
      "fraction '0.06'," +
      "strataReservoirSize '1000', " +
      s"baseTable '$tableName')")

    airlineDataFrame.write.insertInto(sampleTableName)


    totalNumberOfRowsInSampleTable = snc.sql(s"select * from $sampleTableName").collect().length

  }
  */
}
