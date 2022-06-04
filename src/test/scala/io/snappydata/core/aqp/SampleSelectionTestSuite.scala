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
package io.snappydata.core.aqp

import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sampling.ColumnFormatSamplingRelation
import org.apache.spark.sql.{DataFrame, SaveMode}

class SampleSelectionTestSuite extends SnappyFunSuite with BeforeAndAfterAll {

  val SAMPLE_TABLE_1 = "AIRLINE_SAMPLED_1"
  val SAMPLE_TABLE_2 = "AIRLINE_SAMPLED_2"
  val SAMPLE_TABLE_3 = "AIRLINE_SAMPLED"
  val SAMPLE_TABLE_4 = "AIRLINE_SAMPLED_ArrTime"

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

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    setupData()
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
  }

  override def afterAll(): Unit = {
    val snc = this.snc
    // cleanup metastore
    if (snc != null) {
      snc.clearCache()
    }
    super.afterAll()
    stopAll()
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "false")
  }

  protected override def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
    /**
     * Pls do not change the flag values of Property.TestDisableCodeGenFlag.name
     * and Property.UseOptimizedHashAggregateForSingleKey.name
     * They are meant to suppress CodegenFallback Plan so that optimized
     * byte buffer code path is tested & prevented from false passing.
     * If your test needs CodegenFallback, then override the newConf function
     * & clear the flag from the conf of the test locally.
     */
    new org.apache.spark.SparkConf().setAppName("ClosedFormEstimates")
        .setMaster("local[6]")
        .set("spark.logConf", "true")
        .set(Property.TestDisableCodeGenFlag.name , "true")
        .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")
  }

  def setupData(): Unit = {
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val snContext = this.snc
    snContext.sql("set spark.sql.shuffle.partitions=6")

    val airlineDataFrame: DataFrame = snContext.read.load(hfile)
    airlineDataFrame.createOrReplaceTempView("airline")

    snc.sql(s"CREATE TABLE $SAMPLE_TABLE_1 $ddlStr" +
        "USING column_sample " +
        "options " +
        "(" +
        "qcs 'UniqueCarrier, MonthI, YearI' ," +
        "fraction '0.03'," +
        "strataReservoirSize '50', " +
        "baseTable 'airline')")

    airlineDataFrame.write.format("column_sample").mode(SaveMode.Append).saveAsTable(SAMPLE_TABLE_1)

    snc.sql(s"CREATE TABLE $SAMPLE_TABLE_2 $ddlStr" +
        "USING column_sample " +
        "options " +
        "(" +
        "qcs 'UniqueCarrier, MonthI', " +
        "fraction '0.01'," +
        "strataReservoirSize '50', " +
        "baseTable 'airline')")

    airlineDataFrame.write.format("column_sample").mode(SaveMode.Append).saveAsTable(SAMPLE_TABLE_2)

    snc.sql(s"CREATE TABLE $SAMPLE_TABLE_3 $ddlStr" +
        "USING column_sample " +
        "options " +
        "(" +
        "qcs 'UniqueCarrier, MonthI, YearI'," +
        "fraction '0.05'," +
        "strataReservoirSize '50', " +
        "baseTable 'airline')")

    airlineDataFrame.write.format("column_sample").mode(SaveMode.Append).saveAsTable(SAMPLE_TABLE_3)


    snc.sql(s"CREATE TABLE $SAMPLE_TABLE_4 $ddlStr" +
        "USING column_sample " +
        "options " +
        "(" +
        "qcs 'tan(ArrTime)'," +
        "fraction '0.05'," +
        "strataReservoirSize '50', " +
        "baseTable 'airline')")

    airlineDataFrame.write.format("column_sample").mode(SaveMode.Append).saveAsTable(SAMPLE_TABLE_4)
  }

  test("Sample table selection test where queryQCS = tableQCS") {

    val results_sampled_table = snc.sql(
      """SELECT avg(ArrDelay) as T , lower_bound(T) LB, upper_bound(T),
        relative_error(T), absolute_error(T) as AE,
        UniqueCarrier FROM airline where MonthI > 2 GROUP BY
        UniqueCarrier WITH ERROR 0.12 confidence 0.9""")

    val plan = results_sampled_table.queryExecution.optimizedPlan
    var table: String = null

    plan.collectFirst {
      case sr: LogicalRelation => sr
    } match {
      case Some(sr) => table = sr.relation.asInstanceOf[ColumnFormatSamplingRelation].sampleTable
      case None => table = null
    }
    assert(table.equalsIgnoreCase(s"APP.$SAMPLE_TABLE_2"), table)
  }

  test("Largest sample size table selection test") {

    val results_sampled_table = snc.sql(
      """SELECT avg(ArrDelay) as T , lower_bound(T) LB, upper_bound(T),
        relative_error(T), absolute_error(T) as AE,
        UniqueCarrier FROM airline GROUP BY
        UniqueCarrier WITH ERROR 0.12 confidence 0.9""")

    val plan = results_sampled_table.queryExecution.optimizedPlan
    var table: String = null

    plan.collectFirst {
      case sr: LogicalRelation => sr
    } match {
      case Some(sr) => table = sr.relation.asInstanceOf[ColumnFormatSamplingRelation].sampleTable
      case None => table = null
    }
    assert(table.equalsIgnoreCase(s"APP.$SAMPLE_TABLE_3"), table)
  }

  test("Sample table selection test where tableQCS is subset of queryQCS") {

    val results_sampled_table = snc.sql(
      """SELECT avg(ArrDelay) as T , lower_bound(T) LB, upper_bound(T),
        relative_error(T), absolute_error(T) as AE,
        UniqueCarrier FROM airline where MonthI > 2 AND YearI = 2015
        AND ArrDelay > 0 GROUP BY
        UniqueCarrier WITH ERROR 0.12 confidence 0.9""")

    val plan = results_sampled_table.queryExecution.optimizedPlan
    var table: String = null

    plan.collectFirst {
      case sr: LogicalRelation => sr
    } match {
      case Some(sr) => table = sr.relation.asInstanceOf[ColumnFormatSamplingRelation].sampleTable
      case None => table = null
    }
    assert(table.equalsIgnoreCase(s"APP.$SAMPLE_TABLE_3"), table)
  }


  test("TableQCS has a function on attribute & query qcs is not exact match") {

    val results_sampled_table = snc.sql(
      """SELECT avg(ArrDelay) as T , lower_bound(T) LB, upper_bound(T),
        relative_error(T), absolute_error(T) as AE,
        Arrtime FROM airline where MonthI > 2 AND YearI = 2015
        AND ArrDelay > 0 GROUP BY
        ArrTime WITH ERROR """)

    val plan = results_sampled_table.queryExecution.optimizedPlan
    var table: String = null

    plan.collectFirst {
      case sr: LogicalRelation => sr
    } match {
      case Some(sr) => table = sr.relation.asInstanceOf[ColumnFormatSamplingRelation].sampleTable
      case None => table = null
    }
    assert(table.equalsIgnoreCase(s"APP.$SAMPLE_TABLE_3"), table)
  }

  test("TableQCS has a function on attribute and group by clause matches exactly with tableQCS") {

    val results_sampled_table = snc.sql(
      """SELECT avg(ArrDelay) as T , lower_bound(T) LB, upper_bound(T),
        relative_error(T), absolute_error(T) as AE,
        tan(Arrtime) FROM airline where MonthI > 2 AND YearI = 2015
        AND ArrDelay > 0 GROUP BY
        tan(ArrTime) WITH ERROR """)

    val plan = results_sampled_table.queryExecution.optimizedPlan
    var table: String = null

    plan.collectFirst {
      case sr: LogicalRelation => sr
    } match {
      case Some(sr) => table = sr.relation.asInstanceOf[ColumnFormatSamplingRelation].sampleTable
      case None => table = null
    }
    assert(table.equalsIgnoreCase(s"APP.$SAMPLE_TABLE_4"), table)
  }
}
