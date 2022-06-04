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
import io.snappydata.{Property, SnappyFunSuite}
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.junit.Assert._

import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.types.{DecimalType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SnappyContext}

/**
  * Created by vivekb on 19/5/16.
  */
class PartialQueryRoutingTestSuite extends SnappyFunSuite with Matchers with BeforeAndAfterAll {

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

  protected override def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
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

  private def doPrint(str: String): Unit = {
    logInfo(str)
  }

  test("test with group by closedform") {
    val testNo = 1
    doPrint("test with group by closedform: start")

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
    createTableExtension )

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
    orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s"")
    val pop_value_sum0 = pop_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Long](pop_result_sum_0, pop_value_sum0, _.getLong(0))

    val sam_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.6 confidence .95 behavior 'DO_NOTHING'")
    val sam_row_sum0 = sam_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_0, sam_row_sum0, _.getLong(0))
    assert(sam_row_sum0.length == pop_value_sum0.length)

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
       s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum1 = sam_result_sum_1.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_1, sam_row_sum1, _.getLong(0))
    assert(sam_row_sum0.length == sam_row_sum1.length)
    var lastValue: Long = 100000
    var sam_row_sum0_outliers = 0
    var sam_row_sum1_outliers = 0
    sam_row_sum0.indices foreach (i => {
      assert (sam_row_sum1(i).getLong(0) <= lastValue)
      lastValue = sam_row_sum1(i).getLong(0)
      if (sam_row_sum0(i).getDouble(3) > 0.6) {
        sam_row_sum0_outliers = sam_row_sum0_outliers + 1
      }
      if (sam_row_sum1(i).getDouble(3) > 0) {
        assert(sam_row_sum0.exists(row => row.getLong(0) == sam_row_sum1(i).getLong(0)))
        assert(!sam_row_sum1(i).isNullAt(1))
        assert(!sam_row_sum1(i).isNullAt(2))
        assert(sam_row_sum1(i).getDouble(3) !== 0)
        assert(sam_row_sum1(i).getDouble(4) !== 0)
      } else {
        sam_row_sum1_outliers = sam_row_sum1_outliers + 1
        assert(pop_value_sum0.exists(row => row.getLong(0) == sam_row_sum1(i).getLong(0)))
        assert(sam_row_sum1(i).isNullAt(1))
        assert(sam_row_sum1(i).isNullAt(2))
        assert(sam_row_sum1(i).getDouble(3) === 0)
        assert(sam_row_sum1(i).getDouble(4) === 0)
      }
    })
    assert(sam_row_sum0_outliers == sam_row_sum1_outliers)

    val sam_result_sum_2 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum2 = sam_result_sum_2.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_2, sam_row_sum2, _.getLong(0))
    assert(sam_row_sum2.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getLong(0) == sam_row_sum2(i).getLong(0))
    })

    val sam_result_sum_3 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum3 = sam_result_sum_3.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_3, sam_row_sum3, _.getLong(0))
    assert(sam_row_sum3.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getLong(0) == sam_row_sum3(sam_row_sum3.length -1 - i).getLong(0))
    })

    val sam_result_sum_4 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum4 = sam_result_sum_4.collect()
    AQPTestUtils.compareShowAndCollectNoOrder[Long](sam_result_sum_4, sam_row_sum4, _.getLong(0))
    assert(sam_row_sum3.length == sam_row_sum4.length)
    sam_row_sum4.indices foreach (i => {
      assert(sam_row_sum3.exists(row => row.getLong(0) == sam_row_sum4(i).getLong(0)))
    })

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test with group by closedform: end")
  }

  test("test decimal with group by closedform") {
    val testNo = 2
    doPrint("test decimal with group by closedform: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       Decimal(10, 4)," +
        "ol_supply_w_id  integer," +
        "ol_quantity     Decimal(4, 2)," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension )

    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_5000.csv").getPath
    val customSchema = StructType(Array(
      StructField("ol_w_id", IntegerType, true),
      StructField("ol_d_id", IntegerType, true),
      StructField("ol_o_id", IntegerType, true),
      StructField("ol_number", IntegerType, true),
      StructField("ol_i_id", IntegerType, true),
      StructField("ol_amount", DecimalType(10, 4), true),
      StructField("ol_supply_w_id", IntegerType, true),
      StructField("ol_quantity", DecimalType(4, 2), true),
      StructField("ol_dist_info", StringType, true)))
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("maxCharsPerColumn", "4096")
        .schema(customSchema)
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_sum_0 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s"")
    val pop_value_sum0 = pop_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[BigDecimal](pop_result_sum_0,
      pop_value_sum0, _.getDecimal(0))


    val sam_result_sum_0 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.6 confidence .95 behavior 'DO_NOTHING'")
    val sam_row_sum0 = sam_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[BigDecimal](sam_result_sum_0, sam_row_sum0, _.getDecimal(0))
    assert(sam_row_sum0.length == pop_value_sum0.length)

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum1 = sam_result_sum_1.collect()
    AQPTestUtils.compareShowAndCollect[BigDecimal](sam_result_sum_1, sam_row_sum1, _.getDecimal(0))
    assert(sam_row_sum0.length == sam_row_sum1.length)
    var lastValue: Long = 100000000
    var sam_row_sum0_outliers = 0
    var sam_row_sum1_outliers = 0
    sam_row_sum0.indices foreach (i => {
      assert (sam_row_sum1(i).getDecimal(0).longValue() <= lastValue)
      lastValue = sam_row_sum1(i).getDecimal(0).longValue()
      if (sam_row_sum0(i).getDouble(3) > 0.6) {
        sam_row_sum0_outliers = sam_row_sum0_outliers + 1
      }
      if (sam_row_sum1(i).getDouble(3) > 0) {
        assert(sam_row_sum0.exists(row => row.getDecimal(0) == sam_row_sum1(i).getDecimal(0)))
        assert(!sam_row_sum1(i).isNullAt(1))
        assert(!sam_row_sum1(i).isNullAt(2))
        assert(sam_row_sum1(i).getDouble(3) !== 0)
        assert(sam_row_sum1(i).getDouble(4) !== 0)
      } else {
        sam_row_sum1_outliers = sam_row_sum1_outliers + 1
        assert(pop_value_sum0.exists(row => row.getDecimal(0) == sam_row_sum1(i).getDecimal(0)))
        assert(sam_row_sum1(i).isNullAt(1))
        assert(sam_row_sum1(i).isNullAt(2))
        assert(sam_row_sum1(i).getDouble(3) === 0)
        assert(sam_row_sum1(i).getDouble(4) === 0)
      }
    })
    assert(sam_row_sum0_outliers == sam_row_sum1_outliers)

    val sam_result_sum_2 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum2 = sam_result_sum_2.collect()
    AQPTestUtils.compareShowAndCollect[BigDecimal](sam_result_sum_2, sam_row_sum2, _.getDecimal(0))
    assert(sam_row_sum2.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getDecimal(0) == sam_row_sum2(i).getDecimal(0))
    })

    val sam_result_sum_3 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum3 = sam_result_sum_3.collect()
    AQPTestUtils.compareShowAndCollect[BigDecimal](sam_result_sum_3, sam_row_sum3, _.getDecimal(0))
    assert(sam_row_sum3.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getDecimal(0) ==
          sam_row_sum3(sam_row_sum3.length -1 - i).getDecimal(0))
    })

    val sam_result_sum_4 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum4 = sam_result_sum_4.collect()
    AQPTestUtils.compareShowAndCollectNoOrder[BigDecimal](sam_result_sum_4, sam_row_sum4,
      _.getDecimal(0))
    assert(sam_row_sum3.length == sam_row_sum4.length)
    sam_row_sum4.indices foreach (i => {
      assert(sam_row_sum3.exists(row => row.getDecimal(0) == sam_row_sum4(i).getDecimal(0)))
    })

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test decimal with group by closedform: end")
  }

  test("test with group by bootstrap") {
    val testNo = 3
    doPrint("test with group by bootstrap: start")

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
        "ol_dist_info    varchar(24))" +
        createTableExtension )

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
    orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s"")
    val pop_value_avg0 = pop_result_avg_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](pop_result_avg_0, pop_value_avg0, _.getDouble(0))

    val sam_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 0.3 confidence .95 behavior 'DO_NOTHING'")
    val sam_row_avg0 = sam_result_avg_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_0, sam_row_avg0, _.getDouble(0))
    assert(sam_row_avg0.length == pop_value_avg0.length)

    val sam_result_avg_1 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 0.3 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_avg1 = sam_result_avg_1.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_1, sam_row_avg1, _.getDouble(0))
    assert(sam_row_avg0.length == sam_row_avg1.length)
    var lastValue: Double = 100000
    var sam_row_avg0_outliers = 0
    var sam_row_avg1_outliers = 0
    sam_row_avg0.indices foreach (i => {
      assert (sam_row_avg1(i).getDouble(0) <= lastValue)
      lastValue = sam_row_avg1(i).getDouble(0)
      if (sam_row_avg0(i).getDouble(3) > 0.3) {
        sam_row_avg0_outliers = sam_row_avg0_outliers + 1
      }
      if (sam_row_avg1(i).getDouble(3) > 0) {
        assert(sam_row_avg0.exists(row => row.getDouble(0) == sam_row_avg1(i).getDouble(0)))
        assert(!sam_row_avg1(i).isNullAt(1))
        assert(!sam_row_avg1(i).isNullAt(2))
        assert(sam_row_avg1(i).getDouble(3) !== 0)
        assert(sam_row_avg1(i).getDouble(4) !== 0)
      } else {
        sam_row_avg1_outliers = sam_row_avg1_outliers + 1
        assert(pop_value_avg0.exists(row => row.getDouble(0) == sam_row_avg1(i).getDouble(0)))
        assert(sam_row_avg1(i).isNullAt(1))
        assert(sam_row_avg1(i).isNullAt(2))
        assert(sam_row_avg1(i).getDouble(3) === 0)
        assert(sam_row_avg1(i).getDouble(4) === 0)
      }
    })
    assert(sam_row_avg0_outliers == sam_row_avg1_outliers)

    val sam_result_avg_2 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 0.3 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_avg2 = sam_result_avg_2.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_2, sam_row_avg2, _.getDouble(0))
    assert(sam_row_avg2.length == sam_row_avg1.length)
    sam_row_avg1.indices foreach (i => {
      assert(sam_row_avg1(i).getDouble(0) == sam_row_avg2(i).getDouble(0))
    })

    val sam_result_avg_3 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty " +
        s" with error 0.3 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_avg3 = sam_result_avg_3.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_3, sam_row_avg3, _.getDouble(0))
    assert(sam_row_avg3.length == sam_row_avg1.length)
    sam_row_avg1.indices foreach (i => {
      assert(sam_row_avg1(i).getDouble(0) == sam_row_avg3(sam_row_avg3.length -1 - i).getDouble(0))
    })

    val sam_result_avg_4 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" with error 0.3 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_avg4 = sam_result_avg_4.collect()
    AQPTestUtils.compareShowAndCollectNoOrder[Double](sam_result_avg_4, sam_row_avg4,
      _.getDouble(0))
    assert(sam_row_avg3.length == sam_row_avg4.length)
    sam_row_avg4.indices foreach (i => {
      assert(sam_row_avg3.exists(row => row.getDouble(0) == sam_row_avg4(i).getDouble(0)))
    })

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test with group by bootstrap: end")
  }

  test("test without group by closedform") {
    val testNo = 4
    doPrint("test without group by closedform: start")

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
    orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" order by sum_qty desc " +
        s"")
    val pop_value_sum0 = pop_result_sum_0.collect()
    val pop_value_sum_0 = pop_value_sum0(0).getLong(0)

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum1 = sam_result_sum_1.collect()
    val sam_row_sum1_0 = sam_row_sum1(0)
    val sam_row_sum1_0_0 = sam_row_sum1_0.getLong(0)
    assert(!sam_row_sum1_0.isNullAt(1))
    assert(!sam_row_sum1_0.isNullAt(2))
    assert(sam_row_sum1_0.getDouble(3) !== 0)
    assert(sam_row_sum1_0.getDouble(4) !== 0)

    val sam_result_sum_2 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" with error 0.0001 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum2 = sam_result_sum_2.collect()
    val sam_row_sum2_0 = sam_row_sum2(0)
    val sam_row_sum2_0_0 = sam_row_sum2_0.getLong(0)
    assert (pop_value_sum_0 === sam_row_sum2_0_0)
    assert(sam_row_sum2_0.isNullAt(1))
    assert(sam_row_sum2_0.isNullAt(2))
    assert(sam_row_sum2_0.getDouble(3) === 0)
    assert(sam_row_sum2_0.getDouble(4) === 0)

    val sam_result_sum_3 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" order by sum_qty desc " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum3 = sam_result_sum_3.collect()
    val sam_row_sum3_0 = sam_row_sum3(0)
    val sam_row_sum3_0_0 = sam_row_sum3_0.getLong(0)
    assert (sam_row_sum1_0_0 === sam_row_sum3_0_0)
    assert(!sam_row_sum3_0.isNullAt(1))
    assert(!sam_row_sum3_0.isNullAt(2))
    assert(sam_row_sum3_0.getDouble(3) !== 0)
    assert(sam_row_sum3_0.getDouble(4) !== 0)

    val sam_result_sum_4 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" order by sum_qty desc " +
        s" with error 0.0001 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum4 = sam_result_sum_4.collect()
    val sam_row_sum4_0 = sam_row_sum4(0)
    val sam_row_sum4_0_0 = sam_row_sum4_0.getLong(0)
    assert (pop_value_sum_0 === sam_row_sum4_0_0)
    assert(sam_row_sum4_0.isNullAt(1))
    assert(sam_row_sum4_0.isNullAt(2))
    assert(sam_row_sum4_0.getDouble(3) === 0)
    assert(sam_row_sum4_0.getDouble(4) === 0)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test without group by closedform: end")
  }

  test("test without group by bootstrap") {
    val testNo = 5
    doPrint("test without group by bootstrap: start")

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
        createTableExtension )

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
    orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" order by avg_qty desc " +
        s"")
    val pop_value_avg0 = pop_result_avg_0.collect()
    val pop_value_avg_0 = pop_value_avg0(0).getDouble(0)

    val sam_result_avg_1 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_avg1 = sam_result_avg_1.collect()
    val sam_row_avg1_0 = sam_row_avg1(0)
    val sam_row_avg1_0_0 = sam_row_avg1_0.getDouble(0)
    assert(!sam_row_avg1_0.isNullAt(1))
    assert(!sam_row_avg1_0.isNullAt(2))
    assert(sam_row_avg1_0.getDouble(3) !== 0)
    assert(sam_row_avg1_0.getDouble(4) !== 0)

    val sam_result_avg_2 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" with error 0.0001 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_avg2 = sam_result_avg_2.collect()
    val sam_row_avg2_0 = sam_row_avg2(0)
    val sam_row_avg2_0_0 = sam_row_avg2_0.getDouble(0)
    assert (pop_value_avg_0 === sam_row_avg2_0_0)
    assert(sam_row_avg2_0.isNullAt(1))
    assert(sam_row_avg2_0.isNullAt(2))
    assert(sam_row_avg2_0.getDouble(3) === 0)
    assert(sam_row_avg2_0.getDouble(4) === 0)

    val sam_result_avg_3 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" order by avg_qty desc " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_avg3 = sam_result_avg_3.collect()
    val sam_row_avg3_0 = sam_row_avg3(0)
    val sam_row_avg3_0_0 = sam_row_avg3_0.getDouble(0)
    assert (sam_row_avg1_0_0 === sam_row_avg3_0_0)
    assert(!sam_row_avg3_0.isNullAt(1))
    assert(!sam_row_avg3_0.isNullAt(2))
    assert(sam_row_avg3_0.getDouble(3) !== 0)
    assert(sam_row_avg3_0.getDouble(4) !== 0)

    val sam_result_avg_4 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" order by avg_qty desc " +
        s" with error 0.0001 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_avg4 = sam_result_avg_4.collect()
    val sam_row_avg4_0 = sam_row_avg4(0)
    val sam_row_avg4_0_0 = sam_row_avg4_0.getDouble(0)
    assert (pop_value_avg_0 === sam_row_avg4_0_0)
    assert(sam_row_avg4_0.isNullAt(1))
    assert(sam_row_avg4_0.isNullAt(2))
    assert(sam_row_avg4_0.getDouble(3) === 0)
    assert(sam_row_avg4_0.getDouble(4) === 0)

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test without group by bootstrap: end")
  }

  test("test double with group by closedform") {
    val testNo = 6
    doPrint("test double with group by closedform: start")

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
        createTableExtension )

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
    orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_sum_0 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s"")
    val pop_value_sum0 = pop_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](pop_result_sum_0, pop_value_sum0, _.getDouble(0))

    val sam_result_sum_0 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.6 confidence .95 behavior 'DO_NOTHING'")
    val sam_row_sum0 = sam_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_sum_0, sam_row_sum0, _.getDouble(0))
    assert(sam_row_sum0.length == pop_value_sum0.length)

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum1 = sam_result_sum_1.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_sum_1, sam_row_sum1, _.getDouble(0))
    assert(sam_row_sum0.length == sam_row_sum1.length)
    var lastValue: Double = 100000000
    var sam_row_sum0_outliers = 0
    var sam_row_sum1_outliers = 0
    sam_row_sum0.indices foreach (i => {
      assert (sam_row_sum1(i).getDouble(0) <= lastValue)
      lastValue = sam_row_sum1(i).getDouble(0)
      if (sam_row_sum0(i).getDouble(3) > 0.6) {
        sam_row_sum0_outliers = sam_row_sum0_outliers + 1
      }
      if (sam_row_sum1(i).getDouble(3) > 0) {
        assert(sam_row_sum0.exists(row => row.getDouble(0) == sam_row_sum1(i).getDouble(0)))
        assert(!sam_row_sum1(i).isNullAt(1))
        assert(!sam_row_sum1(i).isNullAt(2))
        assert(sam_row_sum1(i).getDouble(3) !== 0)
        assert(sam_row_sum1(i).getDouble(4) !== 0)
      } else {
        sam_row_sum1_outliers = sam_row_sum1_outliers + 1
        assert(pop_value_sum0.exists(row => row.getDouble(0) == sam_row_sum1(i).getDouble(0)))
        assert(sam_row_sum1(i).isNullAt(1))
        assert(sam_row_sum1(i).isNullAt(2))
        assert(sam_row_sum1(i).getDouble(3) === 0)
        assert(sam_row_sum1(i).getDouble(4) === 0)
      }
    })
    assert(sam_row_sum0_outliers == sam_row_sum1_outliers)

    val sam_result_sum_2 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum2 = sam_result_sum_2.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_sum_2, sam_row_sum2, _.getDouble(0))
    assert(sam_row_sum2.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getDouble(0) == sam_row_sum2(i).getDouble(0))
    })

    val sam_result_sum_3 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum3 = sam_result_sum_3.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_sum_3, sam_row_sum3, _.getDouble(0))
    assert(sam_row_sum3.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getDouble(0) == sam_row_sum3(sam_row_sum3.length -1 - i).getDouble(0))
    })

    val sam_result_sum_4 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum4 = sam_result_sum_4.collect()
    AQPTestUtils.compareShowAndCollectNoOrder[Double](sam_result_sum_4, sam_row_sum4,
      _.getDouble(0))
    assert(sam_row_sum3.length == sam_row_sum4.length)
    sam_row_sum4.indices foreach (i => {
      assert(sam_row_sum3.exists(row => row.getDouble(0) == sam_row_sum4(i).getDouble(0)))
    })

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test double with group by closedform: end")
  }

  protected def createBaseTable(snc: SnappyContext, testNo: Int): Unit = {
    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       float," +
        "ol_supply_w_id  integer," +
        "ol_quantity     float," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension )
  }

  test("test float with group by closedform") {
    val testNo = 7
    doPrint("test float with group by closedform: start")

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

    createBaseTable(snc, testNo)
    snc.sql(s"CREATE SAMPLE TABLE sampled_order_line$testNo" +
        s" OPTIONS(qcs 'ol_number', fraction '0.01', strataReservoirSize '50'," +
        s" baseTable 'order_line$testNo')")

    val orderLineCSV = getClass.getResource("/ORDER_LINE_5000.csv").getPath
    val customSchema = StructType(Array(
      StructField("ol_w_id", IntegerType, true),
      StructField("ol_d_id", IntegerType, true),
      StructField("ol_o_id", IntegerType, true),
      StructField("ol_number", IntegerType, true),
      StructField("ol_i_id", IntegerType, true),
      StructField("ol_amount", FloatType, true),
      StructField("ol_supply_w_id", IntegerType, true),
      StructField("ol_quantity", FloatType, true),
      StructField("ol_dist_info", StringType, true)))
    val orderLineDF: DataFrame = snc.read
        .format("com.databricks.spark.csv")
        .option("maxCharsPerColumn", "4096")
        .schema(customSchema)
        .load(orderLineCSV)

    orderLineDF.write.insertInto(s"order_line$testNo")
    orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_sum_0 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s"")
    val pop_value_sum0 = pop_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](pop_result_sum_0, pop_value_sum0, _.getDouble(0))

    val sam_result_sum_0 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.6 confidence .95 behavior 'DO_NOTHING'")
    val sam_row_sum0 = sam_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_sum_0, sam_row_sum0, _.getDouble(0))
    assert(sam_row_sum0.length == pop_value_sum0.length)

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum1 = sam_result_sum_1.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_sum_1, sam_row_sum1, _.getDouble(0))
    assert(sam_row_sum0.length == sam_row_sum1.length)
    var lastValue: Double = 100000000
    var sam_row_sum0_outliers = 0
    var sam_row_sum1_outliers = 0
    sam_row_sum0.indices foreach (i => {
      assert (sam_row_sum1(i).getDouble(0) <= lastValue)
      lastValue = sam_row_sum1(i).getDouble(0)
      if (sam_row_sum0(i).getDouble(3) > 0.6) {
        sam_row_sum0_outliers = sam_row_sum0_outliers + 1
      }
      if (sam_row_sum1(i).getDouble(3) > 0) {
        assert(sam_row_sum0.exists(row => row.getDouble(0) == sam_row_sum1(i).getDouble(0)))
        assert(!sam_row_sum1(i).isNullAt(1))
        assert(!sam_row_sum1(i).isNullAt(2))
        assert(sam_row_sum1(i).getDouble(3) !== 0)
        assert(sam_row_sum1(i).getDouble(4) !== 0)
      } else {
        sam_row_sum1_outliers = sam_row_sum1_outliers + 1
        assert(pop_value_sum0.exists(row => row.getDouble(0) == sam_row_sum1(i).getDouble(0)))
        assert(sam_row_sum1(i).isNullAt(1))
        assert(sam_row_sum1(i).isNullAt(2))
        assert(sam_row_sum1(i).getDouble(3) === 0)
        assert(sam_row_sum1(i).getDouble(4) === 0)
      }
    })
    assert(sam_row_sum0_outliers == sam_row_sum1_outliers)

    val sam_result_sum_2 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum2 = sam_result_sum_2.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_sum_2, sam_row_sum2, _.getDouble(0))
    assert(sam_row_sum2.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getDouble(0) == sam_row_sum2(i).getDouble(0))
    })

    val sam_result_sum_3 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum3 = sam_result_sum_3.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_sum_3, sam_row_sum3, _.getDouble(0))
    assert(sam_row_sum3.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getDouble(0) == sam_row_sum3(sam_row_sum3.length -1 - i).getDouble(0))
    })

    val sam_result_sum_4 = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" with error 0.6 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum4 = sam_result_sum_4.collect()
    AQPTestUtils.compareShowAndCollectNoOrder[Double](sam_result_sum_4, sam_row_sum4,
      _.getDouble(0))
    assert(sam_row_sum3.length == sam_row_sum4.length)
    sam_row_sum4.indices foreach (i => {
      assert(sam_row_sum3.exists(row => row.getDouble(0) == sam_row_sum4(i).getDouble(0)))
    })

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test float with group by closedform: end")
  }

  test("test where clause on same column as group by closedform") {
    val testNo = 8
    doPrint("test where clause on same column as group by closedform: start")

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
        createTableExtension )

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
    orderLineDF.write.format("column_sample").mode(SaveMode.Append).
        saveAsTable(s"sampled_order_line$testNo")

    // snc.sql(s"SELECT *  FROM order_line$testNo").collect()
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").collect()

    val pop_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where ol_w_id > 3 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s"")
    val pop_value_sum0 = pop_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Long](pop_result_sum_0, pop_value_sum0, _.getLong(0))

    val sam_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where ol_w_id > 3 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.2 confidence .95 behavior 'DO_NOTHING'")
    val sam_row_sum0 = sam_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_0, sam_row_sum0, _.getLong(0))
    assert(sam_row_sum0.length == pop_value_sum0.length)

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id, " +
        s" max(ol_number) as Maxx FROM order_line$testNo " +
        s" where ol_w_id > 3 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.2 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")
    val sam_row_sum1 = sam_result_sum_1.collect()

    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_1, sam_row_sum1, _.getLong(0))

    val sam_result_max = snc.sql(s"SELECT ol_w_id, max(ol_number) as Maxx " +
      s" FROM order_line$testNo " +
      s" where ol_w_id > 3  group by ol_w_id ").collect()
    val maxRsMap = sam_result_max.map(row => (row.getInt(0), row.getInt(1))).toMap
    assert(sam_row_sum0.length == sam_row_sum1.length)
    var lastValue: Long = 100000
    var sam_row_sum0_outliers = 0
    var sam_row_sum1_outliers = 0
    sam_row_sum0.indices foreach (i => {
      assert (sam_row_sum1(i).getLong(0) <= lastValue)
      lastValue = sam_row_sum1(i).getLong(0)
      if (sam_row_sum0(i).getDouble(3) > 0.2) {
        sam_row_sum0_outliers = sam_row_sum0_outliers + 1
      }
      if (sam_row_sum1(i).getDouble(3) > 0) {
        assert(sam_row_sum0.exists(row => row.getLong(0) == sam_row_sum1(i).getLong(0)))
        assert(!sam_row_sum1(i).isNullAt(1))
        assert(!sam_row_sum1(i).isNullAt(2))
        assert(sam_row_sum1(i).getDouble(3) !== 0)
        assert(sam_row_sum1(i).getDouble(4) !== 0)
      } else {
        sam_row_sum1_outliers = sam_row_sum1_outliers + 1
        assert(pop_value_sum0.exists(row => row.getLong(0) == sam_row_sum1(i).getLong(0)))
        assert(sam_row_sum1(i).isNullAt(1))
        assert(sam_row_sum1(i).isNullAt(2))
        assert(sam_row_sum1(i).getDouble(3) === 0)
        assert(sam_row_sum1(i).getDouble(4) === 0)
      }
    })
    assert(sam_row_sum0_outliers == sam_row_sum1_outliers)

    sam_row_sum1.foreach(row => assertEquals(maxRsMap.get(row.getInt(5)).get, row.getInt(6)))
    val sam_result_sum_2 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)" +
        s" FROM order_line$testNo " +
        s" where ol_w_id > 3 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.2 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum2 = sam_result_sum_2.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_2, sam_row_sum2, _.getLong(0))
    assert(sam_row_sum2.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getLong(0) == sam_row_sum2(i).getLong(0))
    })

    val sam_result_sum_3 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where ol_w_id > 3 " +
        s" group by ol_w_id " +
        s" order by sum_qty " +
        s" with error 0.2 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum3 = sam_result_sum_3.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_3, sam_row_sum3, _.getLong(0))
    assert(sam_row_sum3.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getLong(0) == sam_row_sum3(sam_row_sum3.length -1 - i).getLong(0))
    })

    val sam_result_sum_4 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where ol_w_id > 3 " +
        s" group by ol_w_id " +
        s" with error 0.2 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")

    val sam_row_sum4 = sam_result_sum_4.collect()
    AQPTestUtils.compareShowAndCollectNoOrder[Long](sam_result_sum_4, sam_row_sum4, _.getLong(0))
    assert(sam_row_sum3.length == sam_row_sum4.length)
    sam_row_sum4.indices foreach (i => {
      assert(sam_row_sum3.exists(row => row.getLong(0) == sam_row_sum4(i).getLong(0)))
    })

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test where clause on same column as group by closedform: end")
  }

  test("Bug AQP-201 order by") {
    val testNo = 9
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
      createTableExtension )

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
    orderLineDF.write.format("column_sample").mode(SaveMode.Append).
      saveAsTable(s"sampled_order_line$testNo")
    val df = snc.sql(s"SELECT sum(ol_amount) AS sum_qty, lower_bound(sum_qty) LB, " +
      s"upper_bound(sum_qty), relative_error(sum_qty), absolute_error(sum_qty) as AE" +
      s" FROM order_line$testNo    group by ol_w_id order by ol_w_id" +
      s" with error 0.3 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")
    df.collect()
    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
  }

  test("Bug ENT-60") {
    val testNo = 10
    doPrint("test where clause on same column as group by closedform: start")

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
      createTableExtension )

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
    orderLineDF.write.format("column_sample").mode(SaveMode.Append).
      saveAsTable(s"sampled_order_line$testNo")

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
      s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
      s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id, " +
      s" max(ol_number) as Maxx FROM order_line$testNo " +
      s" where ol_w_id > 3 " +
      s" group by ol_w_id " +
      s" order by sum_qty desc " +
      s" with error 0.2 confidence .95 behavior 'PARTIAL_RUN_ON_BASE_TABLE'")
    val sam_row_sum1 = sam_result_sum_1.collect()

    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_1, sam_row_sum1, _.getLong(0))

    val sam_result_max = snc.sql(s"SELECT ol_w_id, max(ol_number) as Maxx " +
      s" FROM order_line$testNo " +
      s" where ol_w_id > 3  group by ol_w_id ").collect()

    sam_result_sum_1.toDF().take(5)

    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_1, sam_row_sum1, _.getLong(0))

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")

  }
}
