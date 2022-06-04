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

import com.gemstone.gemfire.internal.AvailablePort
import io.snappydata.{SnappyFunSuite, Constant => Constants}
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.common.AQPRules

/**
  * Created by vivekb on 14/6/16.
  */
class SubQueryRoutingTestSuite extends SnappyFunSuite with Matchers with BeforeAndAfterAll {

  protected val createFirstTable: String = "  using column "
  protected val createSecondTable: String = "  using row "

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
  }

  override def afterAll(): Unit = {
    val snc = this.snc
    if (snc != null) {
      snc.clearCache()
    }
    super.afterAll()
    stopAll()
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "false")
  }

  protected override def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
    val conf = new org.apache.spark.SparkConf()
        .setAppName("SubQueryRoutingTestSuite")
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

  test("test subquery in where clause on another table") {
    val testNo = 1
    doPrint("test subquery in where clause on another table: start")

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
        s" $createFirstTable " )

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
        s" $createSecondTable " )

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

    val pop_result_1 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  OL_W_ID " +
        s" FROM order_line$testNo as main_tab " +
        s" where OL_I_ID in  " +
        s" (" +
        s" SELECT OL2_D_ID AS sub_num " +
        s" FROM order_line_2_$testNo as sub_tab " +
        s" where sub_tab.OL2_DIST_INFO = main_tab.OL_DIST_INFO  " +
        s" ) " +
        s" group by OL_W_ID " +
        s" order by sum_num " +
        s"")
    pop_result_1.collect()

    val sam_result_1 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  OL_W_ID " +
        s" FROM order_line$testNo as main_tab " +
        s" where OL_I_ID in  " +
        s" (" +
        s" SELECT OL2_D_ID AS sub_num " +
        s" FROM order_line_2_$testNo as sub_tab " +
        s" where OL2_SUPPLY_W_ID < 3000  " +
        s" ) " +
        s" group by OL_W_ID " +
        s" order by sum_num " +
        s" with error .95 " +
        s" confidence .95 " +
        s"")
    sam_result_1.collect()

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("test subquery in where clause on another table: end")
  }

  test("without aggregate: test subquery in from clause") {
    val testNo = 2
    doPrint("without aggregate: test subquery in from clause: start")

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
        "ol_dist_info    varchar(24))" +
        s" $createFirstTable " )

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
    orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_1 = snc.sql(s"SELECT OL_D_ID AS num " +
        s" FROM (" +
        s" Select * FROM order_line$testNo " +
        s"  ) as subT " +
        s" order by num ")
    val pop_result_1_rows = pop_result_1.collect()
    val pop_value_1_row_0 = pop_result_1_rows(0).getInt(0)
    val pop_value_1_row_1 = pop_result_1_rows(1).getInt(0)
    val pop_value_1_row_2 = pop_result_1_rows(2).getInt(0)
    val pop_value_1_row_3 = pop_result_1_rows(3).getInt(0)

    val sam_result_1 = snc.sql(s"SELECT OL_D_ID AS num " +
        s" FROM (" +
        s" Select * FROM order_line$testNo " +
        s"  ) as subT " +
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
        s" FROM (" +
        s" Select * FROM order_line$testNo " +
        s"  ) as subT " +
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
        s" FROM (" +
        s" Select * FROM order_line$testNo " +
        s"  ) as subT " +
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

    val sam_result_4 = snc.sql(s"SELECT OL_D_ID AS num " +
        s" FROM (" +
        s" Select * FROM order_line$testNo " +
        s" confidence .95 " +
        s"  ) as subT " +
        s" order by num ")
    val sam_result_4_rows = sam_result_4.collect()
    val sam_value_4_row_0 = sam_result_4_rows(0).getInt(0)
    val sam_value_4_row_1 = sam_result_4_rows(1).getInt(0)
    val sam_value_4_row_2 = sam_result_4_rows(2).getInt(0)
    val sam_value_4_row_3 = sam_result_4_rows(3).getInt(0)
    assert (pop_value_1_row_0 === sam_value_4_row_0)
    assert (pop_value_1_row_1 === sam_value_4_row_1)
    assert (pop_value_1_row_2 === sam_value_4_row_2)
    assert (pop_value_1_row_3 === sam_value_4_row_3)

    val sam_result_5 = snc.sql(s"SELECT OL_D_ID AS num " +
        s" FROM (" +
        s" Select * FROM order_line$testNo " +
        s" with error .95 " +
        s"  ) as subT " +
        s" order by num ")
    val sam_result_5_rows = sam_result_5.collect()
    val sam_value_5_row_0 = sam_result_5_rows(0).getInt(0)
    val sam_value_5_row_1 = sam_result_5_rows(1).getInt(0)
    val sam_value_5_row_2 = sam_result_5_rows(2).getInt(0)
    val sam_value_5_row_3 = sam_result_5_rows(3).getInt(0)
    assert (pop_value_1_row_0 === sam_value_5_row_0)
    assert (pop_value_1_row_1 === sam_value_5_row_1)
    assert (pop_value_1_row_2 === sam_value_5_row_2)
    assert (pop_value_1_row_3 === sam_value_5_row_3)

    val sam_result_6 = snc.sql(s"SELECT OL_D_ID AS num " +
        s" FROM (" +
        s" Select * FROM order_line$testNo " +
        s" with error .95 " +
        s" confidence .95 " +
        s"  ) as subT " +
        s" order by num ")
    val sam_result_6_rows = sam_result_6.collect()
    val sam_value_6_row_0 = sam_result_6_rows(0).getInt(0)
    val sam_value_6_row_1 = sam_result_6_rows(1).getInt(0)
    val sam_value_6_row_2 = sam_result_6_rows(2).getInt(0)
    val sam_value_6_row_3 = sam_result_6_rows(3).getInt(0)
    assert (pop_value_1_row_0 === sam_value_6_row_0)
    assert (pop_value_1_row_1 === sam_value_6_row_1)
    assert (pop_value_1_row_2 === sam_value_6_row_2)
    assert (pop_value_1_row_3 === sam_value_6_row_3)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("without aggregate: test subquery in from clause: end")
  }

  test("test aggregate subquery with error constraint in inner query") {
    val testNo = 3
    doPrint("test aggregate subquery that should not route to base table: start")

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
        "ol_dist_info    varchar(24))" +
        s" $createFirstTable " )

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
    orderLineDF.write.insertInto(s"sampled_order_line$testNo")

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val sam_result_1 = snc.sql(s"SELECT SUM(OL_D_ID) as SUM, absolute_error(SUM) " +
        s" FROM order_line$testNo" +
        s" with error .95 " +
        s" confidence .95 ")
    val sam_value_1 = sam_result_1.collect()(0)
    val sam_value_1_sum = sam_value_1.getLong(0)
    val sam_value_1_ae = sam_value_1.getLong(0)

    // This do not work - Thas syntax now.
//    val sam_result_2 = snc.sql(s"SELECT SUM, AE " +
//        s" FROM (" +
//        s" SELECT SUM(OL_D_ID) as SUM, absolute_error(SUM) as AE" +
//        s" FROM order_line$testNo" +
//        s"  ) as subT " +
//        s" with error .95 " +
//        s"")
//    val sam_value_2 = sam_result_2.collect()(0)
//    val sam_value_2_sum = sam_value_2.getLong(0)
//    val sam_value_2_ae = sam_value_2.getLong(0)
//    assert (sam_value_1_sum === sam_value_2_sum)
//    assert (sam_value_1_ae === sam_value_2_ae)

    val sam_result_3 = snc.sql(s"SELECT SUM, AE " +
        s" FROM (" +
        s" SELECT SUM(OL_D_ID) as SUM, absolute_error(SUM) as AE" +
        s" FROM order_line$testNo" +
        s" with error .95 " +
        s"  ) as subT ")
    val sam_value_3 = sam_result_3.collect()(0)
    val sam_value_3_sum = sam_value_3.getLong(0)
    val sam_value_3_ae = sam_value_3.getLong(0)
    assert (sam_value_1_sum === sam_value_3_sum)
    assert (sam_value_1_ae === sam_value_3_ae)

    val sam_result_4 = snc.sql(s"SELECT SUM, AE " +
        s" FROM (" +
        s" SELECT SUM(OL_D_ID) as SUM, absolute_error(SUM) as AE" +
        s" FROM order_line$testNo" +
        s" with error .95 " +
        s" confidence .95 " +
        s"  ) as subT ")
    val sam_value_4 = sam_result_4.collect()(0)
    val sam_value_4_sum = sam_value_4.getLong(0)
    val sam_value_4_ae = sam_value_4.getLong(0)
    assert (sam_value_1_sum === sam_value_4_sum)
    assert (sam_value_1_ae === sam_value_4_ae)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test aggregate subquery that should not route to base table: end")
  }

  test("with aggregate: test with subquery in from clause") {
    val testNo = 4
    doPrint("with aggregate: test with subquery in from clause: start")

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
        "ol_dist_info    varchar(24))" +
        s" $createFirstTable " )

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

    snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_1 = snc.sql(s"SELECT sum(OL_D_ID) AS sum_num " +
        s" , avg(OL_D_ID) AS avg_num " +
        s" FROM (" +
        s" Select * FROM order_line$testNo " +
        s"  ) as subT " +
        s" group by OL_NUMBER " +
        s" order by sum_num " +
        s"")
    val pop_result_1_rows = pop_result_1.collect()
    val pop_sum_value_row_0 = pop_result_1_rows(0).getLong(0)
    val pop_avg_value_row_0 = pop_result_1_rows(0).getDouble(1)
    val pop_sum_value_row_1 = pop_result_1_rows(1).getLong(0)
    val pop_avg_value_row_1 = pop_result_1_rows(1).getDouble(1)

    val sum_result_1 = snc.sql(s"SELECT sum(OL_D_ID) AS sum_num " +
        s" ,absolute_error(sum_num) " +
        s" ,lower_bound(sum_num)" +
        s" FROM (" +
        s" Select * FROM order_line$testNo " +
        s"  ) as subT " +
        s" group by OL_NUMBER " +
        s" order by sum_num " +
        s" with error .95 ")
    val sum_result_1_rows = sum_result_1.collect()
    val sum_value_1_row_0 = sum_result_1_rows(0).getLong(0)
    val sum_value_1_row_1 = sum_result_1_rows(1).getLong(0)
    assert (pop_sum_value_row_0 === sum_value_1_row_0)
    assert (pop_sum_value_row_1 === sum_value_1_row_1)

    val avg_result_1 = snc.sql(s"SELECT avg(OL_D_ID) AS avg_num " +
        s" ,absolute_error(avg_num) " +
        s" ,lower_bound(avg_num)" +
        s" FROM (" +
        s" Select * FROM order_line$testNo " +
        s"  ) as subT " +
        s" group by OL_NUMBER " +
        s" order by avg_num " +
        s" with error .95 ")
    val avg_result_1_rows = avg_result_1.collect()
    val avg_value_1_row_0 = avg_result_1_rows(0).getDouble(0)
    val avg_value_1_row_1 = avg_result_1_rows(1).getDouble(0)
    assert ((pop_avg_value_row_0 - avg_value_1_row_0).abs < 0.001)
    assert ((pop_avg_value_row_1 - avg_value_1_row_1).abs < 0.001)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("with aggregate: test with subquery in from clause: end")
  }

  test("test with join query in from clause") {
    val testNo = 5
    doPrint("test with join query in from clause: start")

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
        s" $createFirstTable " )

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
        s" $createSecondTable " )

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

    val pop_result_1 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num" +
        s" , OL_W_ID " +
        s" FROM (" +
        s" Select left_tab.OL_NUMBER " +
        s" ,left_tab.OL_W_ID  " +
        s" ,left_tab.OL_SUPPLY_W_ID  " +
        s" FROM order_line$testNo as left_tab " +
        s" join order_line_2_$testNo as right_tab " +
        s" on left_tab.OL_I_ID = right_tab.OL2_D_ID  " +
        s"  ) as subT " +
        s" where OL_SUPPLY_W_ID < 3000  " +
        s" group by OL_W_ID " +
        s" order by sum_num " +
        s"")
    pop_result_1.collect()

    val sam_result_1 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num" +
        s" , OL_W_ID " +
        s" FROM (" +
        s" Select left_tab.OL_NUMBER " +
        s" ,left_tab.OL_W_ID  " +
        s" ,left_tab.OL_SUPPLY_W_ID  " +
        s" FROM order_line$testNo as left_tab " +
        s" join order_line_2_$testNo as right_tab " +
        s" on left_tab.OL_I_ID = right_tab.OL2_D_ID  " +
        s"  ) as subT " +
        s" where OL_SUPPLY_W_ID < 3000  " +
        s" group by OL_W_ID " +
        s" order by sum_num " +
        s" with error .95 " +
        s" confidence .95 " +
        s"")
    sam_result_1.collect()

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("test with join query in from clause: end")
  }

  test("test subquery in where clause with exists clause") {
    val testNo = 6
    doPrint("test subquery in where clause with exists clause: start")

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
        s" $createFirstTable " )

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
        s" $createSecondTable " )

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

    val pop_result_1 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  OL_W_ID " +
        s" FROM order_line$testNo " +
        s" where exists  " +
        s" (" +
        s" SELECT OL2_D_ID AS sub_num " +
        s" FROM order_line_2_$testNo " +
        s" where OL2_DIST_INFO = OL_DIST_INFO  " +
        s" ) " +
        s" group by OL_W_ID " +
        s" order by sum_num " +
        s"")
    pop_result_1.collect()

    val sam_result_1 = snc.sql(s"SELECT SUM(OL_NUMBER) AS sum_num,  OL_W_ID " +
        s" FROM order_line$testNo " +
        s" where exists  " +
        s" (" +
        s" SELECT OL2_D_ID AS sub_num " +
        s" FROM order_line_2_$testNo " +
        s" where OL2_DIST_INFO = OL_DIST_INFO  " +
        s" ) " +
        s" group by OL_W_ID " +
        s" order by sum_num " +
        s" with error .95 " +
        s" confidence .95 " +
        s"")
    // Do not remove this show , as it tests a fixed bug, which shows only with show method
    sam_result_1.show()

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    snc.sql(s"drop table if exists order_line_2_$testNo")
    doPrint("test subquery in where clause with exists clause: end")
  }

}
