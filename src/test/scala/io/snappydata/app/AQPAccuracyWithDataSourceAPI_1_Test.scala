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

import org.apache.spark.sql.SnappyContext

class AQPAccuracyWithDataSourceAPI_1_Test extends AQPAccuracyTest {

  override protected def initData(): Unit = {
    val conf = new org.apache.spark.SparkConf()
        .setAppName("AQPAccuracyTest")
        .set("spark.logConf", "true")
        .set("spark.scheduler.mode", "FAIR")
        .set("spark.master", "local[6]")
    // .set("spark.sql.unsafe.enabled", "false")
    // .set(Constants.closedFormEstimates, "false")


    if (setMaster != null) {
      conf.setMaster(setMaster)
    }

    if (setJars != null) {
      conf.setJars(Seq(setJars))
    }

    if (executorExtraClassPath != null) {
      conf.set("spark.executor.extraClassPath", executorExtraClassPath)
    }

    if (executorExtraJavaOptions != null) {
      conf.set("spark.executor.extraJavaOptions", executorExtraJavaOptions)
    }

    val sc = new org.apache.spark.SparkContext(conf)

    snContext = org.apache.spark.sql.SnappyContext(sc)
    logInfo("Started spark context =" + SnappyContext.globalSparkContext)
    snContext.sql("set spark.sql.shuffle.partitions=6")

    if (loadData) {
      // All these properties will default when using snappyContext in the release
      val props = Map[String, String]()
      snc.sql("DROP TABLE IF EXISTS airlineStaging")
      snc.sql(s"DROP TABLE IF EXISTS $tableName")
      snc.sql(s"DROP TABLE IF EXISTS $sampleTableName")
      snc.sql(s"CREATE EXTERNAL TABLE airlineStaging USING parquet OPTIONS(path '$hfile')")
      val airlineDataFrame = snc.sql(s"CREATE TABLE $tableName USING column OPTIONS(buckets '8') " +
          "AS (SELECT YearI,  MonthI , DayOfMonth,   DayOfWeek, DepTime, CRSDepTime, ArrTime, " +
          "CRSArrTime, UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime, " +
          "AirTime, ArrDelay, DepDelay, Origin, Dest, Distance, TaxiIn, TaxiOut, Cancelled, " +
          "CancellationCode,  Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, " +
          "LateAircraftDelay, ArrDelaySlot FROM airlineStaging)")

      logInfo(s"The $tableName table schema: " + airlineDataFrame.schema.treeString)
      // airlineDataFrame.show(10) // expensive extract of all partitions?

      totalNumberOfRowsInActualTable = snc.sql(s"select * from airlineStaging").collect().length

      assert(totalNumberOfRowsInActualTable > 0)
      // Now create the airline code row replicated table
      val codeTabledf = snContext.read
          .format("com.databricks.spark.csv") // CSV to DF package
          .option("header", "true") // Use first line of all files as header
          .option("inferSchema", "true") // Automatically infer data types
          .option("maxCharsPerColumn", "4096")
          .load(codetableFile)
      logInfo("The airline code table schema: " + codeTabledf.schema.treeString)
      logInfo(codeTabledf.collect().mkString("\n"))

      snContext.dropTable("airlineCode", ifExists = true)
      codeTabledf.write.format("row").options(props).saveAsTable("airlineCode")
      snContext.dropTable("airline_sampled", ifExists = true)
      loadSampleTable()
      results = snContext.sql("select avg(ArrDelay) as avg_delay, absolute_error (avg_delay) ," +
          " UniqueCarrier, YearI, MonthI  from AIRLINE_sampled group by UniqueCarrier, " +
          "YearI, MonthI with error 0.2")
      msg("printing test results")
      logInfo(results.collect().mkString("\n"))
    }
  }

  override protected def loadSampleTable(): Unit = {

    snc.sql(s"CREATE SAMPLE TABLE $sampleTableName " +
        "options (" +
        "qcs 'UniqueCarrier,YearI,MonthI'," +
        "fraction '0.06'," +
        "strataReservoirSize '1000', " +
        s"baseTable '$tableName') AS (SELECT YearI, MonthI , DayOfMonth," +
        "DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime," +
        "UniqueCarrier, FlightNum, TailNum, ActualElapsedTime," +
        "CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin," +
        " Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode," +
        " Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay," +
        s"LateAircraftDelay, ArrDelaySlot FROM $tableName)")

    totalNumberOfRowsInSampleTable = snc.sql(s"select * from $sampleTableName").collect().length
    assert(totalNumberOfRowsInSampleTable > 0)

  }
}
