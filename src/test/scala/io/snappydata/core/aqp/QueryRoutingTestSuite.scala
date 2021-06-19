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
import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.{DataFrame, SaveMode}

/** AQP-10
  * Queries covered in this test class should not get executed on sample table
  * but should be routed to main table.
  *
  * Created by vivekb on 19/5/16.
  */
class QueryRoutingTestSuite extends SnappyFunSuite with Matchers with BeforeAndAfterAll{

  // All the base table has been created as "column table"
  protected val createTableExtension: String = "  using column "

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
        .setAppName("QueryRoutingTestSuite")
        .setMaster("local[6]")
        .set("spark.sql.hive.metastore.sharedPrefixes",
          "com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc," +
              "com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity," +
              "com.mapr.fs.jni,org.apache.commons")
        .set("master.ui.port", "" + AvailablePort.getRandomAvailablePort(0))
        .set(io.snappydata.Property.AqpDebugFixedSeed.name, "true")


    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  test("test count distinct") {
    val testNo = 1
    doPrint("test count distinct: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     varchar(24)," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension)


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
   // orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_cnt_0 = snc.sql(s"SELECT count(distinct(ol_number)) AS cnt_qty_null " +
        s" FROM order_line$testNo ")
    val pop_value_count_0 = pop_result_cnt_0.collect()(0).getLong(0)

    val pop_result_cnt_1 = snc.sql(s"SELECT count(ol_number) AS cnt_qty_null " +
        s" FROM order_line$testNo ")
    val pop_value_count_1 = pop_result_cnt_1.collect()(0).getLong(0)

    assert (pop_value_count_0 !== pop_value_count_1)

    val sam_result_cnt_0 = snc.sql(s"SELECT count(distinct(ol_number)) AS cnt_qty_null " +
        s" FROM order_line$testNo " +
        s" with error 0.2 confidence .95")
    val sam_value_count_0 = sam_result_cnt_0.collect()(0).getLong(0)

    assert (pop_value_count_0 === sam_value_count_0)

    val sam_result_cnt_1 = snc.sql(s"SELECT count(ol_number) AS cnt_qty_null " +
        s" FROM order_line$testNo " +
        s" with error 0.2 confidence .95")
    val sam_value_count_1 = sam_result_cnt_1.collect()(0).getLong(0)

    assert (pop_value_count_1 === sam_value_count_1)

    val sam_result_cnt_2 = snc.sql(s"SELECT count(distinct(ol_number)) AS cnt_qty_null, " +
        s" lower_bound(cnt_qty_null) LB, upper_bound(cnt_qty_null)," +
        s" relative_error(cnt_qty_null), absolute_error(cnt_qty_null) as AE" +
        s" FROM order_line$testNo " +
        s" with error 0.2 confidence .95 ")
    val sam_value_count_2_row_0 = sam_result_cnt_2.collect()(0)
    val sam_value_count_2 = sam_value_count_2_row_0.getLong(0)

    assert (pop_value_count_0 === sam_value_count_2)
    assert(sam_value_count_2_row_0.isNullAt(1))
    assert(sam_value_count_2_row_0.isNullAt(2))
    assert(sam_value_count_2_row_0.getDouble(3) === 0)
    assert(sam_value_count_2_row_0.getDouble(4) === 0)

    val sam_result_cnt_3 = snc.sql(s"SELECT count(distinct(ol_number)) AS cnt_qty_null, " +
        s" lower_bound(cnt_qty_null) LB, upper_bound(cnt_qty_null)," +
        s" relative_error(cnt_qty_null), absolute_error(cnt_qty_null) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 ")
    val sam_value_count_3 = sam_result_cnt_3.collect()(0).getLong(0)

    assert (pop_value_count_0 === sam_value_count_3)

    val sam_result_cnt_4 = snc.sql(s"SELECT count(distinct(ol_number)) AS cnt_qty_null, " +
        s" lower_bound(cnt_qty_null) LB, upper_bound(cnt_qty_null)," +
        s" relative_error(cnt_qty_null), absolute_error(cnt_qty_null) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 confidence .95 ")
    val sam_value_count_4 = sam_result_cnt_4.collect()(0).getLong(0)

    assert (pop_value_count_0 === sam_value_count_4)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test count distinct: end")
  }

  test("test max") {
    val testNo = 2
    doPrint("test max: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     varchar(24)," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension)


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
   // orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_max_1 = snc.sql(s"SELECT max(ol_number) AS max_num, " +
        s" avg(ol_number), sum(ol_number), count(ol_number)" +
        s" FROM order_line$testNo ")
    val pop_result_max_1_rows = pop_result_max_1.collect()
    val pop_value_max_1 = pop_result_max_1_rows(0).getInt(0)
    val pop_value_avg_1 = pop_result_max_1_rows(0).getDouble(1)
    val pop_value_sum_1 = pop_result_max_1_rows(0).getLong(2)
    val pop_value_count_1 = pop_result_max_1_rows(0).getLong(3)

    val sam_result_max_1 = snc.sql(s"SELECT max(ol_number) AS max_num " +
        s" FROM order_line$testNo " +
        s" confidence .95 ")
    val sam_value_max_1 = sam_result_max_1.collect()(0).getInt(0)

    assert (pop_value_max_1 === sam_value_max_1)

    val sam_result_max_2 = snc.sql(s"SELECT max(ol_number) AS max_num " +
        s" FROM order_line$testNo " +
        s" with error .95 ")
    val sam_value_max_2 = sam_result_max_2.collect()(0).getInt(0)

    assert (pop_value_max_1 === sam_value_max_2)

    val sam_result_max_3 = snc.sql(s"SELECT max(ol_number) AS max_num " +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 ")
    val sam_value_max_3 = sam_result_max_3.collect()(0).getInt(0)

    assert (pop_value_max_1 === sam_value_max_3)

    val sam_result_max_4 = snc.sql(s"SELECT max(ol_number) AS max_num, " +
        s" lower_bound(max_num) LB, upper_bound(max_num)," +
        s" relative_error(max_num), absolute_error(max_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 ")

    val sam_value_max_4_row_0 = sam_result_max_4.collect()(0)
    val sam_value_max_4 = sam_value_max_4_row_0.getInt(0)

    assert (pop_value_max_1 === sam_value_max_4)
    assert(sam_value_max_4_row_0.isNullAt(1))
    assert(sam_value_max_4_row_0.isNullAt(2))
    assert(sam_value_max_4_row_0.getDouble(3) === 0)
    assert(sam_value_max_4_row_0.getDouble(4) === 0)

    val sam_result_max_5 = snc.sql(s"SELECT max(ol_number) AS max_num, " +
        s" lower_bound(max_num) LB, upper_bound(max_num)," +
        s" relative_error(max_num), absolute_error(max_num) as AE" +
        s" FROM order_line$testNo " +
        s" confidence .95 ")

    val sam_value_max_5 = sam_result_max_5.collect()(0).getInt(0)
    assert (pop_value_max_1 === sam_value_max_5)

    val sam_result_max_6 = snc.sql(s"SELECT max(ol_number) AS max_num, " +
        s" lower_bound(max_num) LB, upper_bound(max_num)," +
        s" relative_error(max_num), absolute_error(max_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 ")

    val sam_value_max_6 = sam_result_max_6.collect()(0).getInt(0)
    assert (pop_value_max_1 === sam_value_max_6)

    val sam_result_max_7 = snc.sql(s"SELECT max(ol_number) AS max_num, " +
        s" cast(max(ol_number) AS LONG)," +
        s" avg(ol_number), sum(ol_number), count(ol_number)," +
        s" lower_bound(max_num) LB, upper_bound(max_num)" +
        s" FROM order_line$testNo " +
        s" with error .95 ")
    val sam_value_max_7_row_0 = sam_result_max_7.collect()(0)
    val sam_value_max_7 = sam_value_max_7_row_0.getInt(0)
    val sam_value_max_7_2 = sam_value_max_7_row_0.getLong(1)
    val sam_value_avg_7 = sam_value_max_7_row_0.getDouble(2)
    val sam_value_sum_7 = sam_value_max_7_row_0.getLong(3)
    val sam_value_count_7 = sam_value_max_7_row_0.getLong(4)
    assert (pop_value_max_1 === sam_value_max_7)
    assert (sam_value_max_7 === sam_value_max_7_2)
    assert (pop_value_avg_1 === sam_value_avg_7)
    assert (pop_value_sum_1 === sam_value_sum_7)
    assert (pop_value_count_1 === sam_value_count_7)
    assert(sam_value_max_7_row_0.isNullAt(5))
    assert(sam_value_max_7_row_0.isNullAt(6))

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test max: end")
  }

  test("test min empty table") {
    val testNo = 3
    doPrint("test min empty table: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     varchar(24)," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension)


    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_min_1 = snc.sql(s"SELECT min(ol_number) AS min_num, " +
        s" avg(ol_number), sum(ol_number), count(ol_number)" +
        s" FROM order_line$testNo ")
    val pop_result_min_1_rows = pop_result_min_1.collect()
    assert (pop_result_min_1_rows(0).isNullAt(0))
    assert (pop_result_min_1_rows(0).isNullAt(1))
    assert (pop_result_min_1_rows(0).isNullAt(2))
    val pop_value_count_1 = pop_result_min_1_rows(0).getLong(3)

    val sam_result_min_1 = snc.sql(s"SELECT min(ol_number) AS min_num " +
        s" FROM order_line$testNo " +
        s" confidence .95 ")
    assert (sam_result_min_1.collect()(0).isNullAt(0))

    val sam_result_min_2 = snc.sql(s"SELECT min(ol_number) AS min_num " +
        s" FROM order_line$testNo " +
        s" with error .95 ")
    assert (sam_result_min_2.collect()(0).isNullAt(0))

    val sam_result_min_3 = snc.sql(s"SELECT min(ol_number) AS min_num " +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 ")
    assert (sam_result_min_3.collect()(0).isNullAt(0))

    val sam_result_min_4 = snc.sql(s"SELECT min(ol_number) AS min_num, " +
        s" lower_bound(min_num) LB, upper_bound(min_num)," +
        s" relative_error(min_num), absolute_error(min_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 ")

    val sam_value_min_4_row_0 = sam_result_min_4.collect()(0)
    assert (sam_value_min_4_row_0.isNullAt(0))
    assert(sam_value_min_4_row_0.isNullAt(1))
    assert(sam_value_min_4_row_0.isNullAt(2))
    assert(sam_value_min_4_row_0.getDouble(3) === 0)
    assert(sam_value_min_4_row_0.getDouble(4) === 0)

    val sam_result_min_5 = snc.sql(s"SELECT min(ol_number) AS min_num, " +
        s" lower_bound(min_num) LB, upper_bound(min_num)," +
        s" relative_error(min_num), absolute_error(min_num) as AE" +
        s" FROM order_line$testNo " +
        s" confidence .95 ")
    assert (sam_result_min_5.collect()(0).isNullAt(0))

    val sam_result_min_6 = snc.sql(s"SELECT min(ol_number) AS min_num, " +
        s" lower_bound(min_num) LB, upper_bound(min_num)," +
        s" relative_error(min_num), absolute_error(min_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 ")
    assert (sam_result_min_6.collect()(0).isNullAt(0))

    val sam_result_min_7 = snc.sql(s"SELECT min(ol_number) AS min_num, " +
        s" cast(min(ol_number) AS LONG)," +
        s" avg(ol_number), sum(ol_number), count(ol_number)," +
        s" lower_bound(min_num) LB, upper_bound(min_num)" +
        s" FROM order_line$testNo " +
        s" with error .95 ")
    val sam_value_min_7_row_0 = sam_result_min_7.collect()(0)
    assert (sam_value_min_7_row_0.isNullAt(0))
    assert (sam_value_min_7_row_0.isNullAt(1))
    assert (sam_value_min_7_row_0.isNullAt(2))
    assert (sam_value_min_7_row_0.isNullAt(3))
    val sam_value_count_7 = sam_value_min_7_row_0.getLong(4)
    assert (pop_value_count_1 === sam_value_count_7)
    assert(sam_value_min_7_row_0.isNullAt(5))
    assert(sam_value_min_7_row_0.isNullAt(6))

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test min empty table: end")
  }

  private def doPrint(str: String): Unit = {
    logInfo(str)
  }

  test("test query without aggregate") {
    val testNo = 4
    doPrint("test query without aggregate: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     varchar(24)," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension)


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
   // orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_1 = snc.sql(s"SELECT OL_D_ID AS num " +
        s" FROM order_line$testNo " +
        s" order by num ")
    val pop_result_1_rows = pop_result_1.collect()
    val pop_value_1_row_0 = pop_result_1_rows(0).getInt(0)
    val pop_value_1_row_1 = pop_result_1_rows(1).getInt(0)
    val pop_value_1_row_2 = pop_result_1_rows(2).getInt(0)
    val pop_value_1_row_3 = pop_result_1_rows(3).getInt(0)

    val sam_result_1 = snc.sql(s"SELECT OL_D_ID AS num " +
        s" FROM order_line$testNo " +
        s" order by num " +
        s" confidence .95 ")
    val sam_result_1_rows = sam_result_1.collect()
    val sam_value_1_row_0 = sam_result_1_rows(0).getInt(0)
    val sam_value_1_row_1 = sam_result_1_rows(1).getInt(0)
    val sam_value_1_row_2 = sam_result_1_rows(2).getInt(0)
    val sam_value_1_row_3 = sam_result_1_rows(3).getInt(0)
    assert (pop_value_1_row_0 === sam_value_1_row_0)
    assert (pop_value_1_row_1 === sam_value_1_row_1)
    assert (pop_value_1_row_2 === sam_value_1_row_2)
    assert (pop_value_1_row_3 === sam_value_1_row_3)

    val sam_result_2 = snc.sql(s"SELECT OL_D_ID AS num " +
        s" FROM order_line$testNo " +
        s" order by num " +
        s" with error .95 ")
    val sam_result_2_rows = sam_result_2.collect()
    val sam_value_2_row_0 = sam_result_2_rows(0).getInt(0)
    val sam_value_2_row_1 = sam_result_2_rows(1).getInt(0)
    val sam_value_2_row_2 = sam_result_2_rows(2).getInt(0)
    val sam_value_2_row_3 = sam_result_2_rows(3).getInt(0)
    assert (pop_value_1_row_0 === sam_value_2_row_0)
    assert (pop_value_1_row_1 === sam_value_2_row_1)
    assert (pop_value_1_row_2 === sam_value_2_row_2)
    assert (pop_value_1_row_3 === sam_value_2_row_3)

    val sam_result_3 = snc.sql(s"SELECT OL_D_ID AS num " +
        s" FROM order_line$testNo " +
        s" order by num " +
        s" with error .95 " +
        s" confidence .95 ")
    val sam_result_3_rows = sam_result_3.collect()
    val sam_value_3_row_0 = sam_result_3_rows(0).getInt(0)
    val sam_value_3_row_1 = sam_result_3_rows(1).getInt(0)
    val sam_value_3_row_2 = sam_result_3_rows(2).getInt(0)
    val sam_value_3_row_3 = sam_result_3_rows(3).getInt(0)
    assert (pop_value_1_row_0 === sam_value_3_row_0)
    assert (pop_value_1_row_1 === sam_value_3_row_1)
    assert (pop_value_1_row_2 === sam_value_3_row_2)
    assert (pop_value_1_row_3 === sam_value_3_row_3)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test query without aggregate: end")
  }

  test("test HAC reroute with error constraint") {
    val testNo = 5
    doPrint("test HAC reroute with error constraint: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension)


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
   /* orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo") */

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty " +
        s" FROM order_line$testNo where OL_O_ID < 300 ")
    val pop_value_sum_0 = pop_result_sum_0.collect()(0).getLong(0)

    val sam_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE" +
        s" FROM order_line$testNo where OL_O_ID < 300 " +
        s" with error 0.00001 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    // TODO : AQP-192
    // val x = sam_result_sum_0.map(row => row.get(1))
    // val arrVal = x.collect()
    // logInfo("array of values = " + arrVal)
    val sam_row_sum_0 = sam_result_sum_0.collect()(0)
    val sam_value_sum_0 = sam_row_sum_0.getLong(0)

    assert (pop_value_sum_0 === sam_value_sum_0)
    assert(sam_row_sum_0.isNullAt(1))
    assert(sam_row_sum_0.isNullAt(2))
    assert(sam_row_sum_0.getDouble(3) === 0)
    assert(sam_row_sum_0.getDouble(4) === 0)

    // Test Default behavior
    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE" +
        s" FROM order_line$testNo where OL_O_ID < 300 " +
        s" with error 0.00001 confidence .95")
    val sam_row_sum_1 = sam_result_sum_1.collect()(0)
    val sam_value_sum_1 = sam_row_sum_1.getLong(0)

    assert (sam_value_sum_1 === sam_value_sum_0)
    assert(sam_row_sum_1.isNullAt(1))
    assert(sam_row_sum_1.isNullAt(2))
    assert(sam_row_sum_1.getDouble(3) === 0)
    assert(sam_row_sum_1.getDouble(4) === 0)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test HAC reroute with error constraint: end")
  }

  test("test HAC reroute with error constraint bootstrap") {
    val testNo = 6
    doPrint("test HAC reroute with error constraint bootstrap: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension)


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
   /* orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo") */

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty " +
        s" FROM order_line$testNo where OL_O_ID < 300 ")
    val pop_value_avg_0 = pop_result_avg_0.collect()(0).getDouble(0)

    val sam_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE" +
        s" FROM order_line$testNo where OL_O_ID < 300 " +
        s" with error 0.00001 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    val sam_row_avg_0 = sam_result_avg_0.collect()(0)
    val sam_value_avg_0 = sam_row_avg_0.getDouble(0)

    assert (pop_value_avg_0 === sam_value_avg_0)
    assert(sam_row_avg_0.isNullAt(1))
    assert(sam_row_avg_0.isNullAt(2))
    assert(sam_row_avg_0.getDouble(3) === 0)
    assert(sam_row_avg_0.getDouble(4) === 0)

    // Test Default behavior
    val sam_result_avg_1 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE" +
        s" FROM order_line$testNo where OL_O_ID < 300 " +
        s" with error 0.00001 confidence .95")
    val sam_row_avg_1 = sam_result_avg_1.collect()(0)
    val sam_value_avg_1 = sam_row_avg_1.getDouble(0)

    assert (sam_value_avg_0 === sam_value_avg_1)
    assert(sam_row_avg_1.isNullAt(1))
    assert(sam_row_avg_1.isNullAt(2))
    assert(sam_row_avg_1.getDouble(3) === 0)
    assert(sam_row_avg_1.getDouble(4) === 0)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test HAC reroute with error constraint bootstrap: end")
  }

  test("test no routing even with HAC reroute") {
    val testNo = 7
    doPrint("test no routing even with HAC reroute: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     varchar(24)," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension)


    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '1'," +
        s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_NULL_VALS.csv").getPath
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
   /* orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo") */

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_cnt_0 = snc.sql(s"SELECT count(ol_number) AS cnt_qty " +
        s" FROM order_line$testNo where ol_number < 4 ")
    val pop_value_count_0 = pop_result_cnt_0.collect()(0).getLong(0)

    val sam_result_cnt_0 = snc.sql(s"SELECT count(ol_number) AS cnt_qty, " +
        s" lower_bound(cnt_qty) LB, upper_bound(cnt_qty)," +
        s" relative_error(cnt_qty), absolute_error(cnt_qty) as AE" +
        s" FROM order_line$testNo where ol_number < 4  " +
        s" with error .95 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    val sam_value_row_0 = sam_result_cnt_0.collect()(0)
    val sam_value_count_0 = sam_value_row_0.getLong(0)

    assert (pop_value_count_0 >= sam_value_count_0)
    assert(!sam_value_row_0.isNullAt(1))
    assert(!sam_value_row_0.isNullAt(2))
    assert(sam_value_row_0.getDouble(3) === 0)
    assert(sam_value_row_0.getDouble(4) === 0)

    // Test Default behavior
    val sam_result_cnt_1 = snc.sql(s"SELECT count(ol_number) AS cnt_qty, " +
        s" lower_bound(cnt_qty) LB, upper_bound(cnt_qty)," +
        s" relative_error(cnt_qty), absolute_error(cnt_qty) as AE" +
        s" FROM order_line$testNo where ol_number < 4  " +
        s" with error .95 confidence .95")
    val sam_value_row_1 = sam_result_cnt_1.collect()(0)
    val sam_value_count_1 = sam_value_row_1.getLong(0)

    assert (sam_value_count_1 === sam_value_count_0)
    assert(!sam_value_row_1.isNullAt(1))
    assert(!sam_value_row_1.isNullAt(2))
    assert(sam_value_row_1.getDouble(3) === 0)
    assert(sam_value_row_1.getDouble(4) === 0)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test no routing even with HAC reroute: end")
  }

  test("test no routing with HAC error constraint on sample table") {
    val testNo = 8
    doPrint("test no routing with HAC error constraint on sample table: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension)


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
   /* orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo") */

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty " +
        s" FROM order_line$testNo where OL_O_ID < 300 ")
    val pop_value_sum_0 = pop_result_sum_0.collect()(0).getLong(0)

    val sam_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE" +
        s" FROM sampled_order_line$testNo" +
        s" where OL_O_ID < 300 " +
        s" with error 0.5 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    val sam_row_sum_0 = sam_result_sum_0.collect()(0)
    val sam_value_sum_0 = sam_row_sum_0.getLong(0)
    assert(!sam_row_sum_0.isNullAt(1))
    assert(!sam_row_sum_0.isNullAt(2))

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE" +
        s" FROM sampled_order_line$testNo" +
        s" where OL_O_ID < 300 " +
        s" with error 0.5 confidence .95")
    val sam_row_sum_1 = sam_result_sum_1.collect()(0)
    val sam_value_sum_1 = sam_row_sum_1.getLong(0)

    assert (sam_value_sum_1 === sam_value_sum_0)
    assert(sam_row_sum_1.getDouble(1) === sam_row_sum_0.getDouble(1))
    assert(sam_row_sum_1.getDouble(2) === sam_row_sum_0.getDouble(2))
    assert(sam_row_sum_1.getDouble(3) === sam_row_sum_0.getDouble(3))
    assert(sam_row_sum_1.getDouble(4) === sam_row_sum_0.getDouble(4))

    // ---------------- without where clause--------------------------

    val pop_2_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty " +
        s" FROM order_line$testNo where OL_O_ID < 300 ")
    val pop_2_value_sum_0 = pop_2_result_sum_0.collect()(0).getLong(0)

    val sam_2_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE" +
        s" FROM sampled_order_line$testNo" +
        s" with error 0.5 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    val sam_2_row_sum_0 = sam_2_result_sum_0.collect()(0)
    val sam_2_value_sum_0 = sam_2_row_sum_0.getLong(0)
    assert(!sam_2_row_sum_0.isNullAt(1))
    assert(!sam_2_row_sum_0.isNullAt(2))

    val sam_2_result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE" +
        s" FROM sampled_order_line$testNo" +
        s" with error 0.5 confidence .95")
    val sam_2_row_sum_1 = sam_2_result_sum_1.collect()(0)
    val sam_2_value_sum_1 = sam_2_row_sum_1.getLong(0)

    assert (sam_2_value_sum_1 === sam_2_value_sum_0)
    assert(sam_2_row_sum_1.getDouble(1) === sam_2_row_sum_0.getDouble(1))
    assert(sam_2_row_sum_1.getDouble(2) === sam_2_row_sum_0.getDouble(2))
    assert(sam_2_row_sum_1.getDouble(3) === sam_2_row_sum_0.getDouble(3))
    assert(sam_2_row_sum_1.getDouble(4) === sam_2_row_sum_0.getDouble(4))

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test no routing with HAC error constraint on sample table: end")
  }

  test("test no routing with HAC error constraint on sample table by bootstrap") {
    val testNo = 9
    doPrint("test no routing with HAC error constraint on sample table by bootstrap: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension)


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
   /* orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo") */

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty " +
        s" FROM order_line$testNo where OL_O_ID < 300 ")
    val pop_value_avg_0 = pop_result_avg_0.collect()(0).getDouble(0)

    val sam_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE" +
        s" FROM sampled_order_line$testNo" +
        s" where OL_O_ID < 300 " +
        s" with error 0.5 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    val sam_row_avg_0 = sam_result_avg_0.collect()(0)
    val sam_value_avg_0 = sam_row_avg_0.getDouble(0)

    assert(!sam_row_avg_0.isNullAt(1))
    assert(!sam_row_avg_0.isNullAt(2))

    val sam_result_avg_1 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE" +
        s" FROM sampled_order_line$testNo" +
        s" where OL_O_ID < 300 " +
        s" with error 0.5 confidence .95")
    val sam_row_avg_1 = sam_result_avg_1.collect()(0)
    val sam_value_avg_1 = sam_row_avg_1.getDouble(0)

    assert (sam_value_avg_0 === sam_value_avg_1)
    assert(sam_row_avg_1.getDouble(1) === sam_row_avg_0.getDouble(1))
    assert(sam_row_avg_1.getDouble(2) === sam_row_avg_0.getDouble(2))
    assert(sam_row_avg_1.getDouble(3) === sam_row_avg_0.getDouble(3))
    assert(sam_row_avg_1.getDouble(4) === sam_row_avg_0.getDouble(4))

    // ---------------- without where clause--------------------------
    val pop_2_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty " +
        s" FROM order_line$testNo ")
    val pop_2_value_avg_0 = pop_2_result_avg_0.collect()(0).getDouble(0)

    val sam_2_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE" +
        s" FROM sampled_order_line$testNo" +
        s" with error 0.5 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    val sam_2_row_avg_0 = sam_2_result_avg_0.collect()(0)
    val sam_2_value_avg_0 = sam_2_row_avg_0.getDouble(0)

    assert(!sam_2_row_avg_0.isNullAt(1))
    assert(!sam_2_row_avg_0.isNullAt(2))

    val sam_2_result_avg_1 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE" +
        s" FROM sampled_order_line$testNo" +
        s" with error 0.5 confidence .95")
    val sam_2_row_avg_1 = sam_2_result_avg_1.collect()(0)
    val sam_2_value_avg_1 = sam_2_row_avg_1.getDouble(0)

    assert (sam_2_value_avg_0 === sam_2_value_avg_1)
    assert(sam_2_row_avg_1.getDouble(1) === sam_2_row_avg_0.getDouble(1))
    assert(sam_2_row_avg_1.getDouble(2) === sam_2_row_avg_0.getDouble(2))
    assert(sam_2_row_avg_1.getDouble(3) === sam_2_row_avg_0.getDouble(3))
    assert(sam_2_row_avg_1.getDouble(4) === sam_2_row_avg_0.getDouble(4))

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test no routing with HAC error constraint on sample table by bootstrap: end")
  }

  test("test query on base table without sampletable should route") {
    val testNo = 10
    doPrint("test query on base table without sampletable should route: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     varchar(24)," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension)


    val orderLineCSV = getClass.getResource("/ORDER_LINE_NULL_VALS.csv").getPath
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()

    val result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE" +
        s" FROM order_line$testNo where OL_O_ID < 14 " +
        s" with error 0.00001 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    val row_sum_0 = result_sum_0.collect()(0)
    val value_sum_0 = row_sum_0.getLong(0)

    assert(row_sum_0.isNullAt(1))
    assert(row_sum_0.isNullAt(2))
    assert(row_sum_0.getDouble(3) === 0)
    assert(row_sum_0.getDouble(4) === 0)

    val result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE" +
        s" FROM order_line$testNo where OL_O_ID < 14 ")
    val row_sum_1 = result_sum_1.collect()(0)
    val value_sum_1 = row_sum_1.getLong(0)

    assert (value_sum_0 === value_sum_1)
    assert(row_sum_1.isNullAt(1))
    assert(row_sum_1.isNullAt(2))
    assert(row_sum_1.getDouble(3) === 0)
    assert(row_sum_1.getDouble(4) === 0)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test query on base table without sampletable should route: end")
  }

  test("test no routing even with HAC reroute on string") {
    val testNo = 11
    doPrint("test no routing even with HAC reroute on string: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     varchar(24)," +
        "ol_quantity_mix double," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '1'," +
        s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_NULL_VALS.csv").getPath
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096")
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
   /* orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo") */

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_cnt_0 = snc.sql(s"SELECT count(ol_dist_info) AS cnt_qty " +
        s" FROM order_line$testNo where ol_number < 4 ")
    val pop_value_count_0 = pop_result_cnt_0.collect()(0).getLong(0)

    val sam_result_cnt_0 = snc.sql(s"SELECT count(ol_dist_info) AS cnt_qty, " +
        s" lower_bound(cnt_qty) LB, upper_bound(cnt_qty)," +
        s" relative_error(cnt_qty), absolute_error(cnt_qty) as AE" +
        s" FROM order_line$testNo where ol_number < 4  " +
        s" with error .95 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    val sam_value_row_0 = sam_result_cnt_0.collect()(0)
    val sam_value_count_0 = sam_value_row_0.getLong(0)

    assert (pop_value_count_0 >= sam_value_count_0)
    assert(!sam_value_row_0.isNullAt(1))
    assert(!sam_value_row_0.isNullAt(2))
    assert(sam_value_row_0.getDouble(3) === 0)
    assert(sam_value_row_0.getDouble(4) === 0)

    // Test Default behavior
    val sam_result_cnt_1 = snc.sql(s"SELECT count(ol_dist_info) AS cnt_qty, " +
        s" lower_bound(cnt_qty) LB, upper_bound(cnt_qty)," +
        s" relative_error(cnt_qty), absolute_error(cnt_qty) as AE" +
        s" FROM order_line$testNo where ol_number < 4  " +
        s" with error .95 confidence .95")
    val sam_value_row_1 = sam_result_cnt_1.collect()(0)
    val sam_value_count_1 = sam_value_row_1.getLong(0)

    assert (sam_value_count_1 === sam_value_count_0)
    assert(!sam_value_row_1.isNullAt(1))
    assert(!sam_value_row_1.isNullAt(2))
    assert(sam_value_row_1.getDouble(3) === 0)
    assert(sam_value_row_1.getDouble(4) === 0)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test no routing even with HAC reroute on string: end")
  }

  test("test empty table with behaviour involving routing") {
    val testNo = 12
    doPrint("test sum empty table: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

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
        "ol_dist_info    varchar(24)) " +
        createTableExtension )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_num, " +
        s" lower_bound(sum_num) LB, upper_bound(sum_num)," +
        s" relative_error(sum_num), absolute_error(sum_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95  ")

    val sam_value_sum_1_row_0 = sam_result_sum_1.collect()(0)
    assert (sam_value_sum_1_row_0.isNullAt(0))
    assert(sam_value_sum_1_row_0.isNullAt(1))
    assert(sam_value_sum_1_row_0.isNullAt(2))
    assert(sam_value_sum_1_row_0.getDouble(3) == 0)
    assert(sam_value_sum_1_row_0.getDouble(4) == 0)

    val sam_result_sum_2 = snc.sql(s"SELECT sum(ol_number) AS sum_num, " +
        s" lower_bound(sum_num) LB, upper_bound(sum_num)," +
        s" relative_error(sum_num), absolute_error(sum_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 behavior 'RUN_ON_FULL_TABLE'")

    val sam_value_sum_2_row_0 = sam_result_sum_2.collect()(0)
    assert (sam_value_sum_2_row_0.isNullAt(0))
    assert(sam_value_sum_2_row_0.isNullAt(1))
    assert(sam_value_sum_2_row_0.isNullAt(2))
    assert(sam_value_sum_2_row_0.getDouble(3) == 0)
    assert(sam_value_sum_2_row_0.getDouble(4) == 0)

    val sam_result_sum_3 = snc.sql(s"SELECT sum(ol_number) AS sum_num, " +
        s" lower_bound(sum_num) LB, upper_bound(sum_num)," +
        s" relative_error(sum_num), absolute_error(sum_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_value_sum_3_row_0 = sam_result_sum_3.collect()(0)
    assert (sam_value_sum_3_row_0.isNullAt(0))
    assert(sam_value_sum_3_row_0.isNullAt(1))
    assert(sam_value_sum_3_row_0.isNullAt(2))
    assert(sam_value_sum_3_row_0.getDouble(3) == 0)
    assert(sam_value_sum_3_row_0.getDouble(4) == 0)

    val sam_result_avg_1 = snc.sql(s"SELECT avg(ol_number) AS avg_num, " +
        s" lower_bound(avg_num) LB, upper_bound(avg_num)," +
        s" relative_error(avg_num), absolute_error(avg_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 ")

    val sam_value_avg_1_row_0 = sam_result_avg_1.collect()(0)
    assert (sam_value_avg_1_row_0.isNullAt(0))
    assert(sam_value_avg_1_row_0.isNullAt(1))
    assert(sam_value_avg_1_row_0.isNullAt(2))
    assert(sam_value_avg_1_row_0.getDouble(3) == 0)
    assert(sam_value_avg_1_row_0.getDouble(4) == 0)

    val sam_result_avg_2 = snc.sql(s"SELECT avg(ol_number) AS avg_num, " +
        s" lower_bound(avg_num) LB, upper_bound(avg_num)," +
        s" relative_error(avg_num), absolute_error(avg_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 behavior 'RUN_ON_FULL_TABLE'")

    val sam_value_avg_2_row_0 = sam_result_avg_2.collect()(0)
    assert (sam_value_avg_2_row_0.isNullAt(0))
    assert(sam_value_avg_2_row_0.isNullAt(1))
    assert(sam_value_avg_2_row_0.isNullAt(2))
    assert(sam_value_avg_2_row_0.getDouble(3) == 0)
    assert(sam_value_avg_2_row_0.getDouble(4) == 0)

    val sam_result_avg_3 = snc.sql(s"SELECT avg(ol_number) AS avg_num, " +
        s" lower_bound(avg_num) LB, upper_bound(avg_num)," +
        s" relative_error(avg_num), absolute_error(avg_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_value_avg_3_row_0 = sam_result_avg_3.collect()(0)
    assert (sam_value_avg_3_row_0.isNullAt(0))
    assert(sam_value_avg_3_row_0.isNullAt(1))
    assert(sam_value_avg_3_row_0.isNullAt(2))
    assert(sam_value_avg_3_row_0.getDouble(3) == 0)
    assert(sam_value_avg_3_row_0.getDouble(4) == 0)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test empty table: end")
  }

  test("test empty table with behaviour do nothing") {
    val testNo = 14
    doPrint("test sum empty table: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

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
        "ol_dist_info    varchar(24)) " +
        createTableExtension )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_num, " +
        s" lower_bound(sum_num) LB, upper_bound(sum_num)," +
        s" relative_error(sum_num), absolute_error(sum_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 behavior 'Do_Nothing' ")

    val sam_value_sum_1_row_0 = sam_result_sum_1.collect()(0)
    assert (sam_value_sum_1_row_0.isNullAt(0))
    assert(sam_value_sum_1_row_0.isNullAt(1))
    assert(sam_value_sum_1_row_0.isNullAt(2))
    assert(sam_value_sum_1_row_0.isNullAt(3))
    assert(sam_value_sum_1_row_0.isNullAt(4))

    val sam_result_sum_2 = snc.sql(s"SELECT sum(ol_number) AS sum_num, " +
        s" lower_bound(sum_num) LB, upper_bound(sum_num)," +
        s" relative_error(sum_num), absolute_error(sum_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 behavior 'DO_Nothing'")

    val sam_value_sum_2_row_0 = sam_result_sum_2.collect()(0)
    assert (sam_value_sum_2_row_0.isNullAt(0))
    assert(sam_value_sum_2_row_0.isNullAt(1))
    assert(sam_value_sum_2_row_0.isNullAt(2))
    assert(sam_value_sum_2_row_0.isNullAt(3))
    assert(sam_value_sum_2_row_0.isNullAt(4))

    val sam_result_sum_3 = snc.sql(s"SELECT sum(ol_number) AS sum_num, " +
        s" lower_bound(sum_num) LB, upper_bound(sum_num)," +
        s" relative_error(sum_num), absolute_error(sum_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 behavior 'Do_Nothing'")

    val sam_value_sum_3_row_0 = sam_result_sum_3.collect()(0)
    assert (sam_value_sum_3_row_0.isNullAt(0))
    assert(sam_value_sum_3_row_0.isNullAt(1))
    assert(sam_value_sum_3_row_0.isNullAt(2))
    assert(sam_value_sum_3_row_0.isNullAt(3))
    assert(sam_value_sum_3_row_0.isNullAt(4))

    val sam_result_avg_1 = snc.sql(s"SELECT avg(ol_number) AS avg_num, " +
        s" lower_bound(avg_num) LB, upper_bound(avg_num)," +
        s" relative_error(avg_num), absolute_error(avg_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 behavior 'Do_Nothing'")

    val sam_value_avg_1_row_0 = sam_result_avg_1.collect()(0)
    assert (sam_value_avg_1_row_0.isNullAt(0))
    assert(sam_value_avg_1_row_0.isNullAt(1))
    assert(sam_value_avg_1_row_0.isNullAt(2))
    assert(sam_value_avg_1_row_0.isNullAt(3))
    assert(sam_value_avg_1_row_0.isNullAt(4))

    val sam_result_avg_2 = snc.sql(s"SELECT avg(ol_number) AS avg_num, " +
        s" lower_bound(avg_num) LB, upper_bound(avg_num)," +
        s" relative_error(avg_num), absolute_error(avg_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 behavior 'Do_Nothing'")

    val sam_value_avg_2_row_0 = sam_result_avg_2.collect()(0)
    assert (sam_value_avg_2_row_0.isNullAt(0))
    assert(sam_value_avg_2_row_0.isNullAt(1))
    assert(sam_value_avg_2_row_0.isNullAt(2))
    assert(sam_value_avg_2_row_0.isNullAt(3))
    assert(sam_value_avg_2_row_0.isNullAt(4))

    val sam_result_avg_3 = snc.sql(s"SELECT avg(ol_number) AS avg_num, " +
        s" lower_bound(avg_num) LB, upper_bound(avg_num)," +
        s" relative_error(avg_num), absolute_error(avg_num) as AE" +
        s" FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 behavior 'DO_Nothing'")

    val sam_value_avg_3_row_0 = sam_result_avg_3.collect()(0)
    assert (sam_value_avg_3_row_0.isNullAt(0))
    assert(sam_value_avg_3_row_0.isNullAt(1))
    assert(sam_value_avg_3_row_0.isNullAt(2))
    assert(sam_value_avg_3_row_0.isNullAt(3))
    assert(sam_value_avg_3_row_0.isNullAt(4))

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test empty table: end")
  }

  test("test from table with alias") {
    val testNo = 13
    doPrint("test from table with alias: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       double," +
        "ol_supply_w_id  integer," +
        "ol_quantity     double," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension)

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
   /* orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo") */

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty " +
        s" FROM order_line$testNo where OL_O_ID < 300 ")
    val pop_value_avg_0 = pop_result_avg_0.collect()(0).getDouble(0)

    val sam_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE" +
        s" FROM order_line$testNo ol where OL_O_ID < 300 " +
        s" with error 0.00001 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    val sam_row_avg_0 = sam_result_avg_0.collect()(0)
    val sam_value_avg_0 = sam_row_avg_0.getDouble(0)

    assert(pop_value_avg_0 === sam_value_avg_0)
    assert(sam_row_avg_0.isNullAt(1))
    assert(sam_row_avg_0.isNullAt(2))
    assert(sam_row_avg_0.getDouble(3) === 0)
    assert(sam_row_avg_0.getDouble(4) === 0)

    val sam_result_avg_1 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE" +
        s" FROM sampled_order_line$testNo ol " +
        s" where OL_O_ID < 300 " +
        s" with error 0.5 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    val sam_row_avg_1 = sam_result_avg_1.collect()(0)
    val sam_value_avg_1 = sam_row_avg_1.getDouble(0)
    assert(!sam_row_avg_1.isNullAt(1))
    assert(!sam_row_avg_1.isNullAt(2))

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test from table with alias: end")
  }

}
