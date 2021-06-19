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

import com.gemstone.gemfire.internal.AvailablePort
import io.snappydata.SnappyFunSuite
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.junit.Assert._

import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.common.{AQPRules, AnalysisType}

/**
  * Created by vivekb on 15/6/16.
  */
class JoinQueryRoutingTestSuite extends SnappyFunSuite with Matchers with BeforeAndAfterAll {

  protected val createColumnTable: String = "  using column "
  protected val createRowTable: String = "  using row "

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
  }

  override def afterAll(): Unit = {
    val snc = this.snc
    if (snc != null) {
      snc.clearCache()
    }
    super.afterAll()
    stopAll()
  }

  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    val conf = new org.apache.spark.SparkConf()
        .setAppName("JoinQueryRoutingTestSuite")
        .setMaster("local[6]")
        .set("spark.sql.hive.metastore.sharedPrefixes",
          "com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc," +
              "com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity," +
              "com.mapr.fs.jni,org.apache.commons")
        .set("master.ui.port", "" + AvailablePort.getRandomAvailablePort(0))

    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  private def doPrint(str: String): Unit = {
    logInfo(str)
  }

  test("test join of two sample tables without aggregate") {
    val testNo = 1
    doPrint("test join of two sample tables without aggregate: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists sampled_order_line_2_$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createColumnTable " )

    snc.sql(s"create table order_line_2_$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createColumnTable " )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line_2_$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line_2_$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_NULL_VALS.csv").getPath
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.insertInto(s"order_line_2_$testNo")
    orderLineDF.write.insertInto(s"sampled_order_line$testNo")
    orderLineDF.write.insertInto(s"sampled_order_line_2_$testNo")
    // Insert 2nd time to differentiate from sample table
    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.insertInto(s"order_line_2_$testNo")

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM order_line_2_$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line_2_$testNo").collect()

    val sam_result_1 = snc.sql(s"SELECT main_tab.OL_W_ID AS first_num " +
        s" FROM order_line$testNo as main_tab " +
        s" join order_line_2_$testNo as sub_tab " +
        s" on sub_tab.OL_D_ID = main_tab.OL_D_ID  " +
        s" order by first_num " +
        s" with error .95 " +
        s" confidence .95 ")
    assert(sam_result_1.collect().length == 16)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists sampled_order_line_2_$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("test join of two sample tables without aggregate: end")
  }

  /**
    * TODO AQP-180
   */
  ignore("test join of two sample tables") {
    val testNo = 2
    doPrint("test join of two sample tables: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists sampled_order_line_2_$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createColumnTable " )

    snc.sql(s"create table order_line_2_$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createColumnTable " )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line_2_$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line_2_$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_NULL_VALS.csv").getPath
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.insertInto(s"order_line_2_$testNo")
    orderLineDF.write.insertInto(s"sampled_order_line$testNo")
    orderLineDF.write.insertInto(s"sampled_order_line_2_$testNo")

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM order_line_2_$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line_2_$testNo").collect()

    val sam_result_1 = snc.sql(s"SELECT SUM(main_tab.OL_D_ID) AS first_sum, " +
        s" absolute_error(first_sum) " +
        s" FROM order_line$testNo as main_tab " +
        s" join order_line_2_$testNo as sub_tab " +
        s" on sub_tab.OL_W_ID = main_tab.OL_W_ID  " +
        s" order by first_sum " +
        s" with error .95 " +
        s" confidence .95 ")
    sam_result_1.collect()
    val analysis_1 = DebugUtils.getAnalysisApplied(sam_result_1.queryExecution.executedPlan)
    assert(analysis_1 match {
      case Some(AnalysisType.Closedform) => true
      case _ => false
    })
    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists sampled_order_line_2_$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("test join of two sample tables: end")
  }

  test("test join of one sample table with another base table without aggregate") {
    val testNo = 3
    doPrint("test join of one sample table with another base table without aggregate: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createColumnTable " )

    snc.sql(s"create table order_line_2_$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createColumnTable " )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_NULL_VALS.csv").getPath
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.insertInto(s"order_line_2_$testNo")
    orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM order_line_2_$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val sam_result_1 = snc.sql(s"SELECT main_tab.OL_W_ID AS first_num " +
        s" FROM order_line$testNo as main_tab " +
        s" join order_line_2_$testNo as sub_tab " +
        s" on sub_tab.OL_D_ID = main_tab.OL_D_ID  " +
        s" order by first_num " +
        s" with error .95 " +
        s" confidence .95 ")
    assert(sam_result_1.collect().length == 4)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("test join of one sample table with another base table without aggregate: end")
  }

  test("no route: sample table created on column table join with a row table") {
    val testNo = 4
    doPrint("no route: sample table created on column table join with a row table: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createColumnTable " )

    snc.sql(s"create table order_line_2_$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createRowTable " )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_NULL_VALS.csv").getPath
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.insertInto(s"order_line_2_$testNo")
    orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM order_line_2_$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val sam_result_1 = snc.sql(s"SELECT SUM(main_tab.OL_D_ID) AS first_sum " +
        s" ,absolute_error(first_sum) " +
        s" ,lower_bound(first_sum) " +
        s" FROM order_line$testNo as main_tab " +
        s" join order_line_2_$testNo as sub_tab " +
        s" on sub_tab.OL_W_ID = main_tab.OL_W_ID  " +
        s" order by first_sum " +
        s" with error .95 " +
        s" confidence .95 ")
    // logInfo(sam_result_1.queryExecution.executedPlan)
    val analysis_1 = DebugUtils.getAnalysisApplied(sam_result_1.queryExecution.executedPlan)
    assert(analysis_1 match {
      case Some(AnalysisType.Closedform) => true
      case _ => false
    })
    val sam_result_1_row_1 = sam_result_1.collect()(0)
    assert(!sam_result_1_row_1.isNullAt(1))

    val sam_result_2 = snc.sql(s"SELECT AVG(main_tab.OL_D_ID) AS first_avg " +
        s" ,absolute_error(first_avg) " +
        s" ,lower_bound(first_avg) " +
        s" FROM order_line$testNo as main_tab " +
        s" join order_line_2_$testNo as sub_tab " +
        s" on sub_tab.OL_W_ID = main_tab.OL_W_ID  " +
        s" where main_tab.OL_W_ID < 10 " +
        s" order by first_avg " +
        s" with error .95 " +
        s" confidence .95 ")
    val analysis_2 = DebugUtils.getAnalysisApplied(sam_result_2.queryExecution.executedPlan)
    assert(analysis_2 match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val sam_result_2_row_1 = sam_result_2.collect()(0)
    assert(!sam_result_2_row_1.isNullAt(1))

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("no route: sample table created on column table join with a row table: end")
  }

  test("no route: sample table created on column table join with a column table") {
    val testNo = 5
    doPrint("no route: sample table created on column table join with a column table: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createColumnTable " )

    snc.sql(s"create table order_line_2_$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createColumnTable " )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_NULL_VALS.csv").getPath
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.insertInto(s"order_line_2_$testNo")
    orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM order_line_2_$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val sam_result_1 = snc.sql(s"SELECT SUM(main_tab.OL_D_ID) AS first_sum " +
        s" ,absolute_error(first_sum) " +
        s" ,lower_bound(first_sum) " +
        s" FROM order_line$testNo as main_tab " +
        s" join order_line_2_$testNo as sub_tab " +
        s" on sub_tab.OL_W_ID = main_tab.OL_W_ID  " +
        s" order by first_sum " +
        s" with error .95 " +
        s" confidence .95 ")
    // logInfo(sam_result_1.queryExecution.executedPlan)
    val analysis_1 = DebugUtils.getAnalysisApplied(sam_result_1.queryExecution.executedPlan)
    assert(analysis_1 match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val sam_result_1_row_1 = sam_result_1.collect()(0)
    assert(!sam_result_1_row_1.isNullAt(1))

    val sam_result_2 = snc.sql(s"SELECT AVG(main_tab.OL_D_ID) AS first_avg " +
        s" ,absolute_error(first_avg) " +
        s" ,lower_bound(first_avg) " +
        s" FROM order_line$testNo as main_tab " +
        s" join order_line_2_$testNo as sub_tab " +
        s" on sub_tab.OL_W_ID = main_tab.OL_W_ID  " +
        s" where main_tab.OL_W_ID < 10 " +
        s" order by first_avg " +
        s" with error .95 " +
        s" confidence .95 ")
    val analysis_2 = DebugUtils.getAnalysisApplied(sam_result_2.queryExecution.executedPlan)
    assert(analysis_2 match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val sam_result_2_row_1 = sam_result_2.collect()(0)
    assert(!sam_result_2_row_1.isNullAt(1))

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("no route: sample table created on column table join with a column table: end")
  }

  test("no route: sample table created on row table join with a row table") {
    val testNo = 6
    doPrint("no route: sample table created on row table join with a row table: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createRowTable " )

    snc.sql(s"create table order_line_2_$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createRowTable " )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_NULL_VALS.csv").getPath
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.insertInto(s"order_line_2_$testNo")
    orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM order_line_2_$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val sam_result_0 = snc.sql(s"SELECT SUM(main_tab.OL_D_ID) AS first_sum " +
        s" ,absolute_error(first_sum) " +
        s" ,lower_bound(first_sum) " +
        s" FROM order_line$testNo as main_tab " +
        s" order by first_sum " +
        s" with error .95 " +
        s" confidence .95 " +
        s" behavior 'DO_NOTHING' ")
    sam_result_0.collect()
    val analysis_1 = DebugUtils.getAnalysisApplied(sam_result_0.queryExecution.executedPlan)
    assert(analysis_1 match {
      case Some(AnalysisType.Closedform) => true
      case _ => false
    })
    val sam_result_1 = snc.sql(s"SELECT SUM(main_tab.OL_D_ID) AS first_sum " +
        s" ,absolute_error(first_sum) " +
        s" ,lower_bound(first_sum) " +
        s" FROM order_line$testNo as main_tab " +
        s" join order_line_2_$testNo as sub_tab " +
        s" on sub_tab.OL_W_ID = main_tab.OL_W_ID  " +
        s" order by first_sum " +
        s" with error .95 " +
        s" confidence .95 ")
    // logInfo(sam_result_1.queryExecution.executedPlan)
    val analysis_2 = DebugUtils.getAnalysisApplied(sam_result_1.queryExecution.executedPlan)
    assert(analysis_2 match {
      case Some(AnalysisType.Closedform) => true
      case _ => false
    })
    val sam_result_1_row_1 = sam_result_1.collect()(0)
    assert(!sam_result_1_row_1.isNullAt(1))

    val sam_result_2 = snc.sql(s"SELECT AVG(main_tab.OL_D_ID) AS first_avg " +
        s" ,absolute_error(first_avg) " +
        s" ,lower_bound(first_avg) " +
        s" FROM order_line$testNo as main_tab " +
        s" join order_line_2_$testNo as sub_tab " +
        s" on sub_tab.OL_W_ID = main_tab.OL_W_ID  " +
        s" where main_tab.OL_W_ID < 10 " +
        s" order by first_avg " +
        s" with error .95 " +
        s" confidence .95 ")
    val analysis_3 = DebugUtils.getAnalysisApplied(sam_result_2.queryExecution.executedPlan)
    assert(analysis_3 match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val sam_result_2_row_1 = sam_result_2.collect()(0)
    assert(!sam_result_2_row_1.isNullAt(1))

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("no route: sample table created on row table join with a row table: end")
  }

  test("no route: sample table created on row table join with a column table") {
    val testNo = 7
    doPrint("no route: sample table created on row table join with a column table: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createRowTable " )

    snc.sql(s"create table order_line_2_$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24))" +
        s" $createColumnTable " )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_NULL_VALS.csv").getPath
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.insertInto(s"order_line_2_$testNo")
    orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM order_line_2_$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val sam_result_1 = snc.sql(s"SELECT SUM(main_tab.OL_D_ID) AS first_sum " +
        s" ,absolute_error(first_sum) " +
        s" ,lower_bound(first_sum) " +
        s" FROM order_line$testNo as main_tab " +
        s" join order_line_2_$testNo as sub_tab " +
        s" on sub_tab.OL_W_ID = main_tab.OL_W_ID  " +
        s" order by first_sum " +
        s" with error .95 " +
        s" confidence .95 ")
    // logInfo(sam_result_1.queryExecution.executedPlan)
    val analysis_1 = DebugUtils.getAnalysisApplied(sam_result_1.queryExecution.executedPlan)
    assert(analysis_1 match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val sam_result_1_row_1 = sam_result_1.collect()(0)
    assert(!sam_result_1_row_1.isNullAt(1))

    val sam_result_2 = snc.sql(s"SELECT AVG(main_tab.OL_D_ID) AS first_avg " +
        s" ,absolute_error(first_avg) " +
        s" ,lower_bound(first_avg) " +
        s" FROM order_line$testNo as main_tab " +
        s" join order_line_2_$testNo as sub_tab " +
        s" on sub_tab.OL_W_ID = main_tab.OL_W_ID  " +
        s" where main_tab.OL_W_ID < 10 " +
        s" order by first_avg " +
        s" with error .95 " +
        s" confidence .95 ")
    val analysis_2 = DebugUtils.getAnalysisApplied(sam_result_2.queryExecution.executedPlan)
    assert(analysis_2 match {
      case Some(AnalysisType.Bootstrap) => true
      case _ => false
    })
    val sam_result_2_row_1 = sam_result_2.collect()(0)
    assert(!sam_result_2_row_1.isNullAt(1))

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("no route: sample table created on row table join with a column table: end")
  }

  test("no route: test semi join") {
    val testNo = 8
    doPrint("no route: test semi join: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_dist_info    varchar(24))" +
        s" $createColumnTable " )

    snc.sql(s"create table order_line_2_$testNo(" +
        "ol2_w_id         integer," +
        "ol2_d_id         integer," +
        "ol2_o_id         integer," +
        "ol2_number       integer," +
        "ol2_i_id         integer," +
        "ol2_amount       double," +
        "ol2_supply_w_id  integer," +
        "ol2_quantity     double," +
        "ol2_dist_info    varchar(24))" +
        s" $createRowTable " )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_5000.csv").getPath
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.insertInto(s"order_line_2_$testNo")
    orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM order_line_2_$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_1 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  main_tab.OL_W_ID " +
        s" FROM order_line$testNo as main_tab LEFT SEMI JOIN order_line_2_$testNo as sub_tab " +
        s" ON main_tab.OL_I_ID = sub_tab.OL2_D_ID " +
        s" AND sub_tab.OL2_SUPPLY_W_ID < 3000  " +
        s" group by main_tab.OL_W_ID " +
        s" order by sum_num " +
        s"")
    pop_result_1.collect()

    val sam_result_1 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  main_tab.OL_W_ID " +
        s" FROM order_line$testNo as main_tab LEFT SEMI JOIN order_line_2_$testNo as sub_tab " +
        s" ON main_tab.OL_I_ID = sub_tab.OL2_D_ID " +
        s" AND sub_tab.OL2_SUPPLY_W_ID < 3000  " +
        s" group by main_tab.OL_W_ID " +
        s" order by sum_num " +
        s" with error .95 " +
        s" confidence .95 " +
        s"")
    sam_result_1.collect()

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("no route: test semi join: end")
  }

  test("ENT-61: sample table on the inner table in an outer join query is not supported") {
    val testNo = 9
    doPrint("no route: test outer join: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")

    snc.sql(s"create table order_line$testNo(" +
      "ol_w_id         integer," +
      "ol_d_id         integer," +
      "ol_o_id         integer," +
      "ol_number       integer," +
      "ol_i_id         integer," +
      "ol_amount       double," +
      "ol_supply_w_id  integer," +
      "ol_quantity     double," +
      "ol_dist_info    varchar(24))" +
      s" $createColumnTable " )

    snc.sql(s"create table order_line_2_$testNo(" +
      "ol2_w_id         integer," +
      "ol2_d_id         integer," +
      "ol2_o_id         integer," +
      "ol2_number       integer," +
      "ol2_i_id         integer," +
      "ol2_amount       double," +
      "ol2_supply_w_id  integer," +
      "ol2_quantity     double," +
      "ol2_dist_info    varchar(24))" +
      s" $createRowTable " )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
      s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
      s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_5000.csv").getPath
    val orderLineDF: DataFrame = snc.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("maxCharsPerColumn", "4096")
      .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.insertInto(s"order_line_2_$testNo")
    orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM order_line_2_$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()



    val pop_result_2 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  main_tab.OL_W_ID " +
      s" FROM order_line$testNo as main_tab RIGHT OUTER JOIN order_line_2_$testNo as sub_tab " +
      s" ON main_tab.OL_I_ID = sub_tab.OL2_D_ID " +
      s" AND sub_tab.OL2_SUPPLY_W_ID < 3000  " +
      s" group by main_tab.OL_W_ID " +
      s" order by sum_num " +
      s"")
    pop_result_2.collect()
    assertTrue(DebugUtils.getAnalysisApplied(pop_result_2.queryExecution.executedPlan).isEmpty)

    val sam_result_6 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  sub_tab.ol2_w_id " +
      s" FROM order_line$testNo as main_tab RIGHT OUTER JOIN order_line_2_$testNo as sub_tab " +
      s" ON main_tab.OL_I_ID = sub_tab.OL2_D_ID " +
      s" AND sub_tab.OL2_SUPPLY_W_ID < 3000  " +
      s" group by sub_tab.ol2_w_id " +
      s" order by sum_num " +
      s" with error .95 " +
      s" confidence .95 " +
      s"")
    sam_result_6.collect()
    assertTrue(DebugUtils.getAnalysisApplied(sam_result_6.queryExecution.executedPlan).isEmpty)

    val sam_result_2 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  main_tab.OL_W_ID " +
      s" FROM order_line$testNo as main_tab RIGHT OUTER JOIN order_line_2_$testNo as sub_tab " +
      s" ON main_tab.OL_I_ID = sub_tab.OL2_D_ID " +
      s" AND sub_tab.OL2_SUPPLY_W_ID < 3000  " +
      s" group by main_tab.OL_W_ID " +
      s" order by sum_num " +
      s" with error .95 " +
      s" confidence .95 " +
      s"")
    sam_result_2.collect()
    assertTrue(DebugUtils.getAnalysisApplied(sam_result_2.queryExecution.executedPlan).isEmpty)

    val pop_result_3 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  main_tab.OL_W_ID " +
      s" FROM order_line$testNo as main_tab FULL OUTER JOIN order_line_2_$testNo as sub_tab " +
      s" ON main_tab.OL_I_ID = sub_tab.OL2_D_ID " +
      s" AND sub_tab.OL2_SUPPLY_W_ID < 3000  " +
      s" group by main_tab.OL_W_ID " +
      s" order by sum_num " +
      s"")
    pop_result_3.collect()
    assertTrue(DebugUtils.getAnalysisApplied(pop_result_3.queryExecution.executedPlan).isEmpty)


    val sam_result_3 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  main_tab.OL_W_ID " +
      s" FROM order_line$testNo as main_tab FULL OUTER JOIN order_line_2_$testNo as sub_tab " +
      s" ON main_tab.OL_I_ID = sub_tab.OL2_D_ID " +
      s" AND sub_tab.OL2_SUPPLY_W_ID < 3000  " +
      s" group by main_tab.OL_W_ID " +
      s" order by sum_num " +
      s" with error .95 " +
      s" confidence .95 " +
      s"")
    sam_result_3.collect()
    assertTrue(DebugUtils.getAnalysisApplied(sam_result_3.queryExecution.executedPlan).isEmpty)


    val sam_result_4 = snc.sql(s"SELECT avg(OL_NUMBER) AS sum_num,  main_tab.OL_W_ID " +
      s" FROM order_line$testNo as main_tab RIGHT OUTER JOIN order_line_2_$testNo as sub_tab " +
      s" ON main_tab.OL_I_ID = sub_tab.OL2_D_ID " +
      s" AND sub_tab.OL2_SUPPLY_W_ID < 3000  " +
      s" group by main_tab.OL_W_ID " +
      s" order by sum_num " +
      s" with error .95 " +
      s" confidence .95 " +
      s"")
    sam_result_4.collect()
    assertTrue(DebugUtils.getAnalysisApplied(sam_result_4.queryExecution.executedPlan).isEmpty)


    val sam_result_5 = snc.sql(s"SELECT count(OL_NUMBER) AS sum_num,  main_tab.OL_W_ID " +
      s" FROM order_line$testNo as main_tab RIGHT OUTER JOIN order_line_2_$testNo as sub_tab " +
      s" ON main_tab.OL_I_ID = sub_tab.OL2_D_ID " +
      s" AND sub_tab.OL2_SUPPLY_W_ID < 3000  " +
      s" group by main_tab.OL_W_ID " +
      s" order by sum_num " +
      s" with error .95 " +
      s" confidence .95 " +
      s"")
    sam_result_5.collect()
    assertTrue(DebugUtils.getAnalysisApplied(sam_result_5.queryExecution.executedPlan).isEmpty)


    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("no route: test outer join: end")
  }

  test("no route: test outer join") {
    val testNo = 10
    doPrint("no route: test outer join: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_dist_info    varchar(24))" +
        s" $createColumnTable " )

    snc.sql(s"create table order_line_2_$testNo(" +
        "ol2_w_id         integer," +
        "ol2_d_id         integer," +
        "ol2_o_id         integer," +
        "ol2_number       integer," +
        "ol2_i_id         integer," +
        "ol2_amount       double," +
        "ol2_supply_w_id  integer," +
        "ol2_quantity     double," +
        "ol2_dist_info    varchar(24))" +
        s" $createRowTable " )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_5000.csv").getPath
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.insertInto(s"order_line_2_$testNo")
    orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM order_line_2_$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_1 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  main_tab.OL_W_ID " +
        s" FROM order_line$testNo as main_tab LEFT OUTER JOIN order_line_2_$testNo as sub_tab " +
        s" ON main_tab.OL_I_ID = sub_tab.OL2_D_ID " +
        s" AND sub_tab.OL2_SUPPLY_W_ID < 3000  " +
        s" group by main_tab.OL_W_ID " +
        s" order by sum_num " +
        s"")
    pop_result_1.collect()

    val sam_result_1 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  main_tab.OL_W_ID " +
        s" FROM order_line$testNo as main_tab LEFT OUTER JOIN order_line_2_$testNo as sub_tab " +
        s" ON main_tab.OL_I_ID = sub_tab.OL2_D_ID " +
        s" AND sub_tab.OL2_SUPPLY_W_ID < 3000  " +
        s" group by main_tab.OL_W_ID " +
        s" order by sum_num " +
        s" with error .95 " +
        s" confidence .95 " +
        s"")
    sam_result_1.collect()


    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("no route: test outer join: end")
  }

}
