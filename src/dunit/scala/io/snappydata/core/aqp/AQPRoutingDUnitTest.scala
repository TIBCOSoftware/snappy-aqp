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

import java.sql.{Connection, DriverManager}

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper
import org.scalatest.Matchers

import org.apache.spark.sql.{DataFrame, SaveMode, SnappyContext}

class AQPRoutingDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) with Matchers {

  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE

  // All the base table has been created as "column table"
  protected val createTableExtension: String = "  using column "

  def setupData(snc : SnappyContext): Unit = {
    snc.sql("set spark.sql.shuffle.partitions=6")
  }

  override def beforeClass(): Unit = {
    // stop any running lead first to update the "maxErrorAllowed" property
    ClusterManagerTestBase.stopSpark()
    bootProps.setProperty(io.snappydata.Property.MaxErrorAllowed.name, "2")
    super.beforeClass()
  }

  override def afterClass(): Unit = {
    super.afterClass()
    // force restart with default properties in subsequent tests
    ClusterManagerTestBase.stopSpark()
  }

  override def tearDown2(): Unit = {
    // reset the chunk size on lead node
    setDMLMaxChunkSize(default_chunk_size)
    super.tearDown2()
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }

  def getANetConnection(netPort: Int): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Class.forName(driver).newInstance //scalastyle:ignore
    val url = s"jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  private def doPrint(str: String): Boolean = {
    // scalastyle:off println
    println(str)
    // scalastyle:on println
    false
  }

  /**
    * TODO: Also see the results printed to see they are in order
    * i.e. output of df.show
    */
  def test_NoRouting_1(): Unit = {
    val testNo = 1
    doPrint("test_NoRouting_1 with closedform: start")

    val snc = SnappyContext(sc)
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    setupData(snc)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()

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

    // snc.sql(s"SELECT *  FROM order_line$testNo").show
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").show

    val pop_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s"")
    pop_result_sum_0.show()
    val pop_value_sum0 = pop_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Long](pop_result_sum_0, pop_value_sum0, _.getLong(0))
    pop_value_sum0.foreach(r => doPrint(r.toString()))

    val sam_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 1.9 confidence .95 behavior 'DO_NOTHING'")
    sam_result_sum_0.show()
    val sam_row_sum0 = sam_result_sum_0.collect()
    // Fail for AQPRoutingOnRowTableDUnitTest, and not AQPRoutingDUnitTest
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_0, sam_row_sum0, _.getLong(0))
    sam_row_sum0.foreach(r => doPrint(r.toString()))
    assert(sam_row_sum0.length == pop_value_sum0.length)

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 1.9 confidence .95 behavior 'STRICT'")
    sam_result_sum_1.show()
    val sam_row_sum1 = sam_result_sum_1.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_1, sam_row_sum1, _.getLong(0))
    sam_row_sum1.foreach(r => doPrint(r.toString()))
    assert(sam_row_sum0.length == sam_row_sum1.length)
    var lastValue: Long = 100000
    sam_row_sum0.indices foreach (i => {
      assert(sam_row_sum1(i).getLong(0) <= lastValue)
      lastValue = sam_row_sum1(i).getLong(0)
      assert(sam_row_sum0(i).getDouble(3) <= 1.9)
      assert(sam_row_sum0.exists(row => row.getLong(0) == sam_row_sum1(i).getLong(0)))
      assert(!sam_row_sum1(i).isNullAt(1))
      assert(!sam_row_sum1(i).isNullAt(2))
      assert(sam_row_sum1(i).getDouble(3) !== 0)
      assert(sam_row_sum1(i).getDouble(4) !== 0)
    })

    val sam_result_sum_2 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 1.9 confidence .95 behavior 'STRICT'")
    sam_result_sum_2.show()
    val sam_row_sum2 = sam_result_sum_2.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_2, sam_row_sum2, _.getLong(0))
    sam_row_sum2.foreach(r => doPrint(r.toString()))
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
        s" with error 1.9 confidence .95 behavior 'STRICT'")
    sam_result_sum_3.show()
    val sam_row_sum3 = sam_result_sum_3.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_3, sam_row_sum3, _.getLong(0))
    sam_row_sum3.foreach(r => doPrint(r.toString()))
    assert(sam_row_sum3.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getLong(0)
          == sam_row_sum3(sam_row_sum3.length - 1 - i).getLong(0),
        s"sam_row_sum1($i).getSum(0)=" + sam_row_sum1(i).getLong(0) +
            s"sam_row_sum3(" + (sam_row_sum3.length - 1 - i) + s").getLong(0)=" +
            sam_row_sum3(sam_row_sum3.length - 1 - i).getLong(0))
    })

    val sam_result_sum_4 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" with error 1.9 confidence .95 behavior 'STRICT'")
    sam_result_sum_4.show()
    val sam_row_sum4 = sam_result_sum_4.collect()
    AQPTestUtils.compareShowAndCollectNoOrder[Long](sam_result_sum_4, sam_row_sum4, _.getLong(0))
    sam_row_sum4.foreach(r => doPrint(r.toString()))
    assert(sam_row_sum3.length == sam_row_sum4.length)
    sam_row_sum4.indices foreach (i => {
      assert(sam_row_sum3.exists(row => row.getLong(0) == sam_row_sum4(i).getLong(0)))
    })

    val sam_result_sum_2_rs = s.executeQuery(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 1.9 confidence .95 behavior 'STRICT'")
    sam_row_sum2.indices foreach (i => {
      sam_result_sum_2_rs.next()
      assert((sam_row_sum2(i).getLong(0) - sam_result_sum_2_rs.getLong(1)).abs < 2)
    })
    assert(!sam_result_sum_2_rs.next())

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test_NoRouting_1 with closedform: end")
  }

  /**
    * TODO: Also see the results printed to see they are in order
    * i.e. output of df.show
    */
  def test_NoRouting_2(): Unit = {
    val testNo = 2
    doPrint("test_NoRouting_2 with bootstrap: start")

    val snc = SnappyContext(sc)
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    setupData(snc)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()

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

    // snc.sql(s"SELECT *  FROM order_line$testNo").show
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").show

    val pop_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s"")
    pop_result_avg_0.show()
    val pop_value_avg0 = pop_result_avg_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](pop_result_avg_0, pop_value_avg0, _.getDouble(0))
    pop_value_avg0.foreach(r => doPrint(r.toString()))

    val sam_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 1.9 confidence .95 behavior 'DO_NOTHING'")
    sam_result_avg_0.show()
    val sam_row_avg0 = sam_result_avg_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_0, sam_row_avg0, _.getDouble(0))
    sam_row_avg0.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg0.length == pop_value_avg0.length)

    val sam_result_avg_1 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 1.9 confidence .95 behavior 'STRICT'")
    sam_result_avg_1.show()
    val sam_row_avg1 = sam_result_avg_1.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_1, sam_row_avg1, _.getDouble(0))
    sam_row_avg1.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg0.length == sam_row_avg1.length)
    var lastValue: Double = 100000
    sam_row_avg0.indices foreach (i => {
      assert(sam_row_avg1(i).getDouble(0) <= lastValue)
      lastValue = sam_row_avg1(i).getDouble(0)
      assert(sam_row_avg0(i).getDouble(3) <= 1.9)
      assert(sam_row_avg0(i).getDouble(0) - sam_row_avg1(i).getDouble(0) < 0.001)
      assert(!sam_row_avg1(i).isNullAt(1))
      assert(!sam_row_avg1(i).isNullAt(2))
      assert(sam_row_avg1(i).getDouble(3) !== 0)
      assert(sam_row_avg1(i).getDouble(4) !== 0)
    })

    val sam_result_avg_2 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 1.9 confidence .95 behavior 'STRICT'")
    sam_result_avg_2.show()
    val sam_row_avg2 = sam_result_avg_2.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_2, sam_row_avg2, _.getDouble(0))
    sam_row_avg2.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg2.length == sam_row_avg1.length)
    sam_row_avg1.indices foreach (i => {
      assert((sam_row_avg1(i).getDouble(0) - sam_row_avg2(i).getDouble(0)).abs < 0.001,
        "sam_row_avg1(i).getDouble(0)=" + sam_row_avg1(i).getDouble(0) +
            "sam_row_avg2(i).getDouble(0)=" + sam_row_avg2(i).getDouble(0))
    })

    val sam_result_avg_3 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty " +
        s" with error 1.9 confidence .95 behavior 'STRICT'")
    sam_result_avg_3.show()
    val sam_row_avg3 = sam_result_avg_3.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_3, sam_row_avg3, _.getDouble(0))
    sam_row_avg3.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg3.length == sam_row_avg1.length)
    sam_row_avg1.indices foreach (i => {
      assert((sam_row_avg1(i).getDouble(0)
          - sam_row_avg3(sam_row_avg3.length - 1 - i).getDouble(0)).abs < 0.001,
        s"sam_row_avg1($i).getDouble(0)=" + sam_row_avg1(i).getDouble(0) +
            s"sam_row_avg3(" + (sam_row_avg3.length - 1 - i) + s").getDouble(0)=" +
            sam_row_avg3(sam_row_avg3.length - 1 - i).getDouble(0))
    })

    val sam_result_avg_4 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" with error 1.9 confidence .95 behavior 'STRICT'")
    sam_result_avg_4.show()
    val sam_row_avg4 = sam_result_avg_4.collect()
    AQPTestUtils.compareShowAndCollectNoOrder[Double](sam_result_avg_4, sam_row_avg4,
      _.getDouble(0))
    sam_row_avg4.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg3.length == sam_row_avg4.length)
    sam_row_avg4.indices foreach (i => {
      assert(sam_row_avg3.exists(row => (row.getDouble(0)
          - sam_row_avg4(i).getDouble(0)).abs < 0.001),
        "sam_row_avg3=" + sam_row_avg3 +
            "sam_row_avg4(i).getDouble(0)=" + sam_row_avg4(i).getDouble(0))
    })

    val sam_result_avg_2_rs = s.executeQuery(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 1.9 confidence .95 behavior 'STRICT'")
    sam_row_avg2.indices foreach (i => {
      sam_result_avg_2_rs.next()
      assert((sam_row_avg2(i).getDouble(0) - sam_result_avg_2_rs.getDouble(1)).abs < 0.001)
    })
    assert(!sam_result_avg_2_rs.next())

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test_NoRouting_2 with bootstrap: end")
  }

  /**
    * TODO: Also see the results printed to see they are in order
    * i.e. output of df.show
    */
  def test_FullRouting_1(): Unit = {
    val testNo = 3
    doPrint("test_FullRouting_1 with no routing closedform: start")

    val snc = SnappyContext(sc)
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    setupData(snc)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()

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

    // snc.sql(s"SELECT *  FROM order_line$testNo").show
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").show

    val pop_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s"")
    pop_result_sum_0.show()
    val pop_value_sum0 = pop_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Long](pop_result_sum_0, pop_value_sum0, _.getLong(0))
    pop_value_sum0.foreach(r => doPrint(r.toString()))

    val sam_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 1.9 confidence .95 behavior 'DO_NOTHING'")
    sam_result_sum_0.show()
    val sam_row_sum0 = sam_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_0, sam_row_sum0, _.getLong(0))
    sam_row_sum0.foreach(r => doPrint(r.toString()))
    assert(sam_row_sum0.length == pop_value_sum0.length)

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 1.9 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_sum_1.show()
    val sam_row_sum1 = sam_result_sum_1.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_1, sam_row_sum1, _.getLong(0))
    sam_row_sum1.foreach(r => doPrint(r.toString()))
    assert(sam_row_sum0.length == sam_row_sum1.length)
    var lastValue: Long = 100000
    sam_row_sum0.indices foreach (i => {
      assert(sam_row_sum1(i).getLong(0) <= lastValue)
      lastValue = sam_row_sum1(i).getLong(0)
      assert(sam_row_sum0(i).getDouble(3) <= 1.9)
      assert(sam_row_sum0.exists(row => row.getLong(0) == sam_row_sum1(i).getLong(0)))
      assert(!sam_row_sum1(i).isNullAt(1))
      assert(!sam_row_sum1(i).isNullAt(2))
      assert(sam_row_sum1(i).getDouble(3) !== 0)
      assert(sam_row_sum1(i).getDouble(4) !== 0)
    })

    val sam_result_sum_2 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 1.9 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_sum_2.show()
    val sam_row_sum2 = sam_result_sum_2.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_2, sam_row_sum2, _.getLong(0))
    sam_row_sum2.foreach(r => doPrint(r.toString()))
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
        s" with error 1.9 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_sum_3.show()
    val sam_row_sum3 = sam_result_sum_3.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_3, sam_row_sum3, _.getLong(0))
    sam_row_sum3.foreach(r => doPrint(r.toString()))
    assert(sam_row_sum3.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getLong(0)
          == sam_row_sum3(sam_row_sum3.length - 1 - i).getLong(0),
        s"sam_row_sum1($i).getSum(0)=" + sam_row_sum1(i).getLong(0) +
            s"sam_row_sum3(" + (sam_row_sum3.length - 1 - i) + s").getLong(0)=" +
            sam_row_sum3(sam_row_sum3.length - 1 - i).getLong(0))
    })

    val sam_result_sum_4 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" with error 1.9 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_sum_4.show()
    val sam_row_sum4 = sam_result_sum_4.collect()
    AQPTestUtils.compareShowAndCollectNoOrder[Long](sam_result_sum_4, sam_row_sum4, _.getLong(0))
    sam_row_sum4.foreach(r => doPrint(r.toString()))
    assert(sam_row_sum3.length == sam_row_sum4.length)
    sam_row_sum4.indices foreach (i => {
      assert(sam_row_sum3.exists(row => row.getLong(0) == sam_row_sum4(i).getLong(0)))
    })

    val sam_result_sum_2_rs = s.executeQuery(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 1.9 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_row_sum2.indices foreach (i => {
      sam_result_sum_2_rs.next()
      assert((sam_row_sum2(i).getLong(0) - sam_result_sum_2_rs.getLong(1)).abs < 2)
    })
    assert(!sam_result_sum_2_rs.next())

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test_FullRouting_1 with no routing closedform: end")
  }

  /**
    * TODO: Also see the results printed to see they are in order
    * i.e. output of df.show
    */
  def test_FullRouting_2(): Unit = {
    val testNo = 4
    doPrint("test_FullRouting_2 with routing closedform: start")

    val snc = SnappyContext(sc)
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    setupData(snc)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()

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

    // snc.sql(s"SELECT *  FROM order_line$testNo").show
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").show

    val pop_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s"")
    pop_result_sum_0.show()
    val pop_value_sum0 = pop_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Long](pop_result_sum_0, pop_value_sum0, _.getLong(0))
    pop_value_sum0.foreach(r => doPrint(r.toString()))

    val sam_result_sum_0 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.1 confidence .95 behavior 'DO_NOTHING'")
    sam_result_sum_0.show()
    val sam_row_sum0 = sam_result_sum_0.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_0, sam_row_sum0, _.getLong(0))
    sam_row_sum0.foreach(r => doPrint(r.toString()))
    assert(sam_row_sum0.length == pop_value_sum0.length)

    val sam_result_sum_1 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.1 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_sum_1.show()
    val sam_row_sum1 = sam_result_sum_1.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_1, sam_row_sum1, _.getLong(0))
    sam_row_sum1.foreach(r => doPrint(r.toString()))
    assert(sam_row_sum0.length == sam_row_sum1.length)
    var lastValue: Long = 100000
    sam_row_sum0.indices foreach (i => {
      assert(sam_row_sum1(i).getLong(0) <= lastValue)
      lastValue = sam_row_sum1(i).getLong(0)
      assert(pop_value_sum0(i).getLong(0) == sam_row_sum1(i).getLong(0))
      assert(sam_row_sum1(i).isNullAt(1))
      assert(sam_row_sum1(i).isNullAt(2))
      assert(sam_row_sum1(i).getDouble(3) === 0)
      assert(sam_row_sum1(i).getDouble(4) === 0)
    })

    val sam_result_sum_2 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +

        s" with error 0.1 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_sum_2.show()
    val sam_row_sum2 = sam_result_sum_2.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_2, sam_row_sum2, _.getLong(0))
    sam_row_sum2.foreach(r => doPrint(r.toString()))
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
        s" with error 0.1 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_sum_3.show()
    val sam_row_sum3 = sam_result_sum_3.collect()
    AQPTestUtils.compareShowAndCollect[Long](sam_result_sum_3, sam_row_sum3, _.getLong(0))
    sam_row_sum3.foreach(r => doPrint(r.toString()))
    assert(sam_row_sum3.length == sam_row_sum1.length)
    sam_row_sum1.indices foreach (i => {
      assert(sam_row_sum1(i).getLong(0)
          == sam_row_sum3(sam_row_sum3.length - 1 - i).getLong(0),
        s"sam_row_sum1($i).getSum(0)=" + sam_row_sum1(i).getLong(0) +
            s"sam_row_sum3(" + (sam_row_sum3.length - 1 - i) + s").getLong(0)=" +
            sam_row_sum3(sam_row_sum3.length - 1 - i).getLong(0))
    })

    val sam_result_sum_4 = snc.sql(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty)," +
        s" relative_error(sum_qty), absolute_error(sum_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" with error 0.1 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_sum_4.show()
    val sam_row_sum4 = sam_result_sum_4.collect()
    AQPTestUtils.compareShowAndCollectNoOrder[Long](sam_result_sum_4, sam_row_sum4, _.getLong(0))
    sam_row_sum4.foreach(r => doPrint(r.toString()))
    assert(sam_row_sum3.length == sam_row_sum4.length)
    sam_row_sum4.indices foreach (i => {
      assert(sam_row_sum3.exists(row => row.getLong(0) == sam_row_sum4(i).getLong(0)))
    })

    val sam_result_sum_2_rs = s.executeQuery(s"SELECT sum(ol_number) AS sum_qty, " +
        s" lower_bound(sum_qty) LB, upper_bound(sum_qty), " +
        s" relative_error(sum_qty), absolute_error(sum_qty) " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by sum_qty desc " +
        s" with error 0.1 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_row_sum2.indices foreach (i => {
      sam_result_sum_2_rs.next()
      assert((sam_row_sum2(i).getLong(0) - sam_result_sum_2_rs.getLong(1)).abs < 2)
      assert(sam_result_sum_2_rs.getString(2) == null)
      assert(sam_result_sum_2_rs.getString(3) == null)
      assert(sam_result_sum_2_rs.getDouble(4) === 0)
      assert(sam_result_sum_2_rs.getDouble(5) === 0)
    })
    assert(!sam_result_sum_2_rs.next())

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test_FullRouting_2 with routing closedform: end")
  }

  /**
    * TODO: Also see the results printed to see they are in order
    * i.e. output of df.show
    */
  def test_FullRouting_3(): Unit = {
    val testNo = 5
    doPrint("test_FullRouting_3 with no routing bootstrap: start")

    val snc = SnappyContext(sc)
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    setupData(snc)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()

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

    // snc.sql(s"SELECT *  FROM order_line$testNo").show
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").show

    val pop_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s"")
    pop_result_avg_0.show()
    val pop_value_avg0 = pop_result_avg_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](pop_result_avg_0, pop_value_avg0, _.getDouble(0))
    pop_value_avg0.foreach(r => doPrint(r.toString()))

    val sam_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 1.9 confidence .95 behavior 'DO_NOTHING'")
    sam_result_avg_0.show()
    val sam_row_avg0 = sam_result_avg_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_0, sam_row_avg0, _.getDouble(0))
    sam_row_avg0.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg0.length == pop_value_avg0.length)

    val sam_result_avg_1 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 1.9 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_avg_1.show()
    val sam_row_avg1 = sam_result_avg_1.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_1, sam_row_avg1, _.getDouble(0))
    sam_row_avg1.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg0.length == sam_row_avg1.length)
    var lastValue: Double = 100000
    sam_row_avg0.indices foreach (i => {
      assert(sam_row_avg1(i).getDouble(0) <= lastValue)
      lastValue = sam_row_avg1(i).getDouble(0)
      assert(sam_row_avg0(i).getDouble(3) <= 1.9)
      assert(sam_row_avg0(i).getDouble(0) - sam_row_avg1(i).getDouble(0) < 0.001)
      assert(!sam_row_avg1(i).isNullAt(1))
      assert(!sam_row_avg1(i).isNullAt(2))
      assert(sam_row_avg1(i).getDouble(3) !== 0)
      assert(sam_row_avg1(i).getDouble(4) !== 0)
    })

    val sam_result_avg_2 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 1.9 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_avg_2.show()
    val sam_row_avg2 = sam_result_avg_2.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_2, sam_row_avg2, _.getDouble(0))
    sam_row_avg2.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg2.length == sam_row_avg1.length)
    sam_row_avg1.indices foreach (i => {
      assert((sam_row_avg1(i).getDouble(0) - sam_row_avg2(i).getDouble(0)).abs < 0.001,
        "sam_row_avg1(i).getDouble(0)=" + sam_row_avg1(i).getDouble(0) +
            "sam_row_avg2(i).getDouble(0)=" + sam_row_avg2(i).getDouble(0))
    })

    val sam_result_avg_3 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty " +
        s" with error 1.9 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_avg_3.show()
    val sam_row_avg3 = sam_result_avg_3.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_3, sam_row_avg3, _.getDouble(0))
    sam_row_avg3.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg3.length == sam_row_avg1.length)
    sam_row_avg1.indices foreach (i => {
      assert((sam_row_avg1(i).getDouble(0)
          - sam_row_avg3(sam_row_avg3.length - 1 - i).getDouble(0)).abs < 0.001,
        s"sam_row_avg1($i).getDouble(0)=" + sam_row_avg1(i).getDouble(0) +
            s"sam_row_avg3(" + (sam_row_avg3.length - 1 - i) + s").getDouble(0)=" +
            sam_row_avg3(sam_row_avg3.length - 1 - i).getDouble(0))
    })

    val sam_result_avg_4 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" with error 1.9 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_avg_4.show()
    val sam_row_avg4 = sam_result_avg_4.collect()
    AQPTestUtils.compareShowAndCollectNoOrder[Double](sam_result_avg_4, sam_row_avg4,
      _.getDouble(0))
    sam_row_avg4.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg3.length == sam_row_avg4.length)
    sam_row_avg4.indices foreach (i => {
      assert(sam_row_avg3.exists(row => (row.getDouble(0)
          - sam_row_avg4(i).getDouble(0)).abs < 0.001),
        "sam_row_avg3=" + sam_row_avg3 +
            "sam_row_avg4(i).getDouble(0)=" + sam_row_avg4(i).getDouble(0))
    })

    val sam_result_avg_2_rs = s.executeQuery(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 1.9 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_row_avg2.indices foreach (i => {
      sam_result_avg_2_rs.next()
      assert((sam_row_avg2(i).getDouble(0) - sam_result_avg_2_rs.getDouble(1)).abs < 0.001)
    })
    assert(!sam_result_avg_2_rs.next())

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test_FullRouting_3 with no routing bootstrap: end")
  }

  /**
    * TODO: Also see the results printed to see they are in order
    * i.e. output of df.show
    */
  def test_FullRouting_4(): Unit = {
    val testNo = 6
    doPrint("test_FullRouting_4 with routing bootstrap: start")

    val snc = SnappyContext(sc)
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    setupData(snc)
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()

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

    // snc.sql(s"SELECT *  FROM order_line$testNo").show
    // snc.sql(s"SELECT *  FROM sampled_order_line$testNo").show

    val pop_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, ol_w_id " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s"")
    pop_result_avg_0.show()
    val pop_value_avg0 = pop_result_avg_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](pop_result_avg_0, pop_value_avg0, _.getDouble(0))
    pop_value_avg0.foreach(r => doPrint(r.toString()))

    val sam_result_avg_0 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 0.1 confidence .95 behavior 'DO_NOTHING'")
    sam_result_avg_0.show()
    val sam_row_avg0 = sam_result_avg_0.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_0, sam_row_avg0, _.getDouble(0))
    sam_row_avg0.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg0.length == pop_value_avg0.length)

    val sam_result_avg_1 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 0.1 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_avg_1.show()
    val sam_row_avg1 = sam_result_avg_1.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_1, sam_row_avg1, _.getDouble(0))
    sam_row_avg1.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg0.length == sam_row_avg1.length)
    var lastValue: Double = 100000
    sam_row_avg0.indices foreach (i => {
      assert(sam_row_avg1(i).getDouble(0) <= lastValue)
      lastValue = sam_row_avg1(i).getDouble(0)
      assert(pop_value_avg0(i).getDouble(0) == sam_row_avg1(i).getDouble(0))
      assert(sam_row_avg1(i).isNullAt(1))
      assert(sam_row_avg1(i).isNullAt(2))
      assert(sam_row_avg1(i).getDouble(3) == 0)
      assert(sam_row_avg1(i).getDouble(4) == 0)
    })

    val sam_result_avg_2 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 0.1 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_avg_2.show()
    val sam_row_avg2 = sam_result_avg_2.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_2, sam_row_avg2, _.getDouble(0))
    sam_row_avg2.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg2.length == sam_row_avg1.length)
    sam_row_avg1.indices foreach (i => {
      assert((sam_row_avg1(i).getDouble(0) - sam_row_avg2(i).getDouble(0)).abs < 0.001,
        "sam_row_avg1(i).getDouble(0)=" + sam_row_avg1(i).getDouble(0) +
            "sam_row_avg2(i).getDouble(0)=" + sam_row_avg2(i).getDouble(0))
    })

    val sam_result_avg_3 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty " +
        s" with error 0.1 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_avg_3.show()
    val sam_row_avg3 = sam_result_avg_3.collect()
    AQPTestUtils.compareShowAndCollect[Double](sam_result_avg_3, sam_row_avg3, _.getDouble(0))
    sam_row_avg3.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg3.length == sam_row_avg1.length)
    sam_row_avg1.indices foreach (i => {
      assert((sam_row_avg1(i).getDouble(0)
          - sam_row_avg3(sam_row_avg3.length - 1 - i).getDouble(0)).abs < 0.001,
        s"sam_row_avg1($i).getDouble(0)=" + sam_row_avg1(i).getDouble(0) +
            s"sam_row_avg3(" + (sam_row_avg3.length - 1 - i) + s").getDouble(0)=" +
            sam_row_avg3(sam_row_avg3.length - 1 - i).getDouble(0))
    })

    val sam_result_avg_4 = snc.sql(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) as AE, ol_w_id" +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" with error 0.1 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_result_avg_4.show()
    val sam_row_avg4 = sam_result_avg_4.collect()
    AQPTestUtils.compareShowAndCollectNoOrder[Double](sam_result_avg_4, sam_row_avg4,
      _.getDouble(0))
    sam_row_avg4.foreach(r => doPrint(r.toString()))
    assert(sam_row_avg3.length == sam_row_avg4.length)
    sam_row_avg4.indices foreach (i => {
      assert(sam_row_avg3.exists(row => (row.getDouble(0)
          - sam_row_avg4(i).getDouble(0)).abs < 0.001),
        "sam_row_avg3=" + sam_row_avg3 +
            "sam_row_avg4(i).getDouble(0)=" + sam_row_avg4(i).getDouble(0))
    })

    val sam_result_avg_2_rs = s.executeQuery(s"SELECT avg(ol_number) AS avg_qty, " +
        s" lower_bound(avg_qty) LB, upper_bound(avg_qty)," +
        s" relative_error(avg_qty), absolute_error(avg_qty) " +
        s" FROM order_line$testNo " +
        s" where OL_O_ID < 300 " +
        s" group by ol_w_id " +
        s" order by avg_qty desc " +
        s" with error 0.1 confidence .95 behavior 'RUN_ON_FULL_TABLE'")
    sam_row_avg2.indices foreach (i => {
      sam_result_avg_2_rs.next()
      assert((sam_row_avg2(i).getDouble(0) - sam_result_avg_2_rs.getDouble(1)).abs < 0.001)
      assert(sam_result_avg_2_rs.getString(2) == null)
      assert(sam_result_avg_2_rs.getString(3) == null)
      assert(sam_result_avg_2_rs.getDouble(4) === 0)
      assert(sam_result_avg_2_rs.getDouble(5) === 0)
    })
    assert(!sam_result_avg_2_rs.next())

    snc.sql(s"drop table if exists sampled_order_line$testNo")
    snc.sql(s"drop table if exists order_line$testNo")
    doPrint("test_FullRouting_4 with routing bootstrap: end")
  }

}
