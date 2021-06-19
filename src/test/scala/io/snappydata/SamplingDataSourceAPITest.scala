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
package io.snappydata

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.{SaveMode, SnappyContext}

/**
 *  This test only tests API sanctity of sample data source APIs. We are
 *  not testing valid sampling as we are taking limited number of rows from
 *  the airline parquet files
 */
class SamplingDataSourceAPITest extends SnappyFunSuite
with Logging
with BeforeAndAfter
with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    System.setProperty(Constant.defaultBehaviorAsDO_NOTHING, "true")
  }

  override def afterAll(): Unit = {
    System.setProperty(Constant.defaultBehaviorAsDO_NOTHING, "false")
    super.afterAll()
  }

  after {
    snc.dropTable(tableName, ifExists = true)
  }

  val tableName: String = "SampleColumnTable"
  val tableName_1: String = "SampleColumnTable_1"
  val tableName_2: String = "SampleColumnTable_2"
  val SAMPLE_TABLE : String = "SampleColumnTable_3"
  val SAMPLE_TABLE_1 : String = "SampleColumnTable_4"

  val emptyProps = Map.empty[String, String]

    val opts = Map(
      "qcs" -> "UniqueCarrier,YearI,MonthI", "fraction" -> "0.03",
      "strataReservoirSize" -> "50")

  val ddlStr = "(YearI INT," + // NOT NULL
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


  var hfile: String = getClass.getResource("/2015.parquet").getPath

  test("Test Basic DataSource API") {

    val airlineDataFrame = snc.read.load(hfile)
    val limitedDF = airlineDataFrame.limit(50000)

    limitedDF.write.format("column").options(emptyProps)
        .mode(SaveMode.Append).saveAsTable(tableName)

    snc.sql(s"CREATE SAMPLE TABLE $tableName_1$ddlStr " +
        "options " +
        "(" +
        "qcs 'UniqueCarrier,YearI,MonthI'," +
        "fraction '0.03'," +
        "strataReservoirSize '50'" +
        ")")

   val coldf = snc.sql(s"select * from $tableName")


    coldf.write.mode(SaveMode.Append).saveAsTable(tableName_1)

    var result = snc.sql(s"select AVG(ArrDelay) as x, sum(ArrDelay) as y, absolute_error(y)," +
        s" UniqueCarrier, YearI, MonthI from $tableName_1 group by UniqueCarrier, YearI, MonthI")



    var r = result.collect
    logInfo(" r.length 1 :" + r.length)
    assert(r.length > 0)

    snc.sql(s"CREATE SAMPLE TABLE $tableName_2 " +
        "options " +
        "(" +
        "qcs 'UniqueCarrier,YearI,MonthI'," +
        "fraction '0.03'," +
        "strataReservoirSize '50')" +
        s"AS (select * from $tableName)")


    result = snc.sql(s"select AVG(ArrDelay),sum(ArrDelay) as y, absolute_error(y)," +
        s" UniqueCarrier, YearI, MonthI from $tableName_2 group by UniqueCarrier, YearI, MonthI")

    r = result.collect
    logInfo(" r.length 2 :" + r.length)
    assert(r.length > 0)

    coldf.write.mode(SaveMode.Overwrite).format(SnappyContext.SAMPLE_SOURCE)
        .options(opts).saveAsTable(tableName_2)

    result = snc.sql(s"select AVG(ArrDelay),sum(ArrDelay) as y, absolute_error(y)," +
        s" UniqueCarrier, YearI, MonthI from $tableName_2 group by UniqueCarrier, YearI, MonthI")

    r = result.collect
    logInfo(" r.length 3 :" + r.length)
    assert(r.length > 0)

//   The new column table is not treated as sample table hence queries with approx syntax
// will not be supported.
//    val sampleDF = snc.sql(s"select * from $tableName_2")
//
//    sampleDF.write.format("column").mode(SaveMode.Append).saveAsTable("NewColumnTable")
//
//    result = snc.sql(s"select AVG(ArrDelay), sum(ArrDelay) as y, absolute_error(y)," +
//      s" UniqueCarrier, YearI, MonthI from NewColumnTable group by UniqueCarrier, YearI, MonthI")
//
//    r = result.collect
//    logInfo(" r.length 4 :" + r.length)
//    assert(r.length > 0)

    snc.sql(s"CREATE SAMPLE TABLE $SAMPLE_TABLE$ddlStr " +
        "options " +
        "(" +
        "qcs 'UniqueCarrier,YearI,MonthI'," +
        "fraction '0.03'," +
        "strataReservoirSize '50'" +
        ")")


    coldf.write.insertInto(SAMPLE_TABLE)

    result = snc.sql(s"select AVG(ArrDelay), sum(ArrDelay) as y, absolute_error(y)," +
        s" UniqueCarrier, YearI, MonthI from $SAMPLE_TABLE group by UniqueCarrier, YearI, MonthI")

    r = result.collect
    logInfo(" r.length 5 :" + r.length)
    assert(r.length > 0)

    // Test without using option
    snc.sql(s"CREATE SAMPLE TABLE $SAMPLE_TABLE_1 " +
        "options " +
        "(" +
        "qcs 'UniqueCarrier,YearI,MonthI'," +
        "fraction '0.03'," +
        "strataReservoirSize '50')" +
        s"AS (select * from $tableName)")

    result = snc.sql(s"select AVG(ArrDelay),sum(ArrDelay) as y, absolute_error(y)," +
        s" UniqueCarrier, YearI, MonthI from $SAMPLE_TABLE_1 group by UniqueCarrier, YearI, MonthI")

    r = result.collect
    logInfo(" r.length 5 :" + r.length)
    assert(r.length > 0)


    // Test Drop table
    snc.sql(s"drop table $SAMPLE_TABLE_1")


    // Recreate the same table but now using explicit provider
    snc.sql(s"CREATE TABLE $SAMPLE_TABLE_1$ddlStr " +
        "USING column_sample " +
        "options " +
        "(" +
        "qcs 'UniqueCarrier,YearI,MonthI'," +
        "fraction '0.03'," +
        "strataReservoirSize '50'" +
        ")")


    result = snc.sql(s"select * from $SAMPLE_TABLE_1")


    r = result.collect
    logInfo(" r.length 6 :" + r.length)
    assert(r.length == 0) // Result should be zero


    coldf.write.mode(SaveMode.Append).saveAsTable(SAMPLE_TABLE_1)

    result = snc.sql(s"select AVG(ArrDelay),sum(ArrDelay) as y, absolute_error(y)," +
        s" UniqueCarrier, YearI, MonthI from $SAMPLE_TABLE_1 group by UniqueCarrier, YearI, MonthI")

    r = result.collect
    logInfo(" r.length 7 :" + r.length)
    assert(r.length > 0)

    snc.sql(s"truncate table $SAMPLE_TABLE_1")
    result = snc.sql(s"SELECT * FROM $SAMPLE_TABLE_1")

    r = result.collect
    logInfo(" r.length 8 :" + r.length)
    assert(r.length == 0) // Result should be zero

    snc.sql(s"drop table $tableName")
    snc.sql(s"drop table $tableName_2")

    logInfo("Successful")
  }
}
