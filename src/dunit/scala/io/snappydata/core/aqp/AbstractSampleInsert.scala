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

import java.sql.{Connection, DriverManager, Statement}

import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper
import org.junit.Assert.assertEquals
import io.snappydata.{Constant => Constants}

import org.apache.spark.AssertAQPAnalysis
import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SnappyContext}

abstract class AbstractSampleInsert(val s: String) extends ClusterManagerTestBase(s) {
  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE

  val orderLineTable = "orderline"
  val orderLineExtTable = "orderlineExt"
  val orderLineSampleTable1 = "orderlineSample1"
  var snc: SnappyContext = _
  var orderLineExtDf: DataFrame = _
  var netPort1: Int = _
  var conn: Connection = _
  bootProps.put(io.snappydata.Property.ClosedFormEstimates.name, "true")

  bootProps.put(Constants.defaultBehaviorAsDO_NOTHING, "true")
  System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
  bootProps.put("spark.sql.shuffle.partitions", "6")

  val tableType: String
  override def tearDown2(): Unit = {
    // reset the chunk size on lead node
    conn.close()
    TestUtil.stopNetServer()
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

  override def setUp(): Unit = {
    super.setUp()
    snc = SnappyContext(sc)
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    conn = getANetConnection(netPort1)
    this.setupData(snc)
    this.createOrderTable(snc)

  }

  def setupData(snc: SnappyContext): Unit = {
    val orderLineCSV = getClass.getResource("/ORDER_LINE.csv").getPath
    snc.sql("set spark.sql.shuffle.partitions=6")
    val schema = StructType(Seq(
      StructField("col1", IntegerType, nullable = false),
      StructField("col2", IntegerType, nullable = false),
      StructField("col3", IntegerType, nullable = false),
      StructField("col4", IntegerType, nullable = false),
      StructField("col5", IntegerType, nullable = false),
      StructField("col6", FloatType, nullable = false),
      StructField("col7", IntegerType, nullable = false),
      StructField("col8", FloatType, nullable = false),
      StructField("col9", StringType, nullable = false)
    ))
    this.orderLineExtDf = snc.createExternalTable(orderLineExtTable, "csv", schema,
      Map("path" -> orderLineCSV, "inferSchema" -> "false", "header" -> "false"))
  }

  def createOrderTable(snc: SnappyContext, populate: Boolean = true): Unit = {
    val df = snc.createTable(orderLineTable, tableType,
      this.orderLineExtDf.schema, Map.empty[String, String], false)
    if (populate) {
      this.orderLineExtDf.write.format(tableType).
        mode(SaveMode.Append).saveAsTable(orderLineTable)
    }
  }

  def createSampleTable(snc: SnappyContext): Unit = {
    snc.sql(s"create sample table $orderLineSampleTable1 on $orderLineTable " +
      s"options (qcs 'col1', fraction '0.01', strataReservoirSize '10')" +
      s" as (select * from $orderLineTable)")
  }

  def dropSampleTable(snc: SnappyContext): Unit = {
    snc.sql(s"DROP TABLE if exists $orderLineSampleTable1")
  }

  private def assertInsertion(): Unit = {
    var rsBase1 = snc.sql(s"select col1, count(*) countt " +
      s"from $orderLineTable group by col1 order by countt").collect()
    var rAqp1 = snc.sql(s"select col1, count(*) countt, absolute_error(countt) " +
      s"from $orderLineTable group by col1 order by countt with error").collect()
    // AssertAQPAnalysis.closedFormAnalysis(this.snc)
    assertEquals(rsBase1.length, rAqp1.length)
    rsBase1.zip(rAqp1).foreach {
      case (r1, r2) => assertEquals(r1.getLong(1), r2.getLong(1))
    }
  }

  def testJDBCBatchInsert(): Unit = {
    this.createSampleTable(snc)
    this.assertInsertion()
    // Now do a batch insert in the main table using data frame
    val ps = conn.prepareStatement(s"insert into $orderLineTable values" +
      s" (?,?,?, ?,?,?, ?,?,?)")
    orderLineExtDf.collect().foreach(row => {
      ps.setInt(1, row.getInt(0))
      ps.setInt(2, row.getInt(1))
      ps.setInt(3, row.getInt(2))
      ps.setInt(4, row.getInt(3))
      ps.setInt(5, row.getInt(4))
      ps.setFloat(6, row.getFloat(5))
      ps.setInt(7, row.getInt(6))
      ps.setFloat(8, row.getFloat(7))
      ps.setString(9, row.getString(8))
      ps.addBatch()
    })
    ps.executeBatch()
    this.assertInsertion()
    this.dropSampleTable(snc)
  }

  def testInsertIntoValSelectSyntax() {
    this.createSampleTable(snc)
    this.assertInsertion()
    // Now do a batch insert in the main table using data frame
    this.orderLineExtDf.write.format("column").mode(SaveMode.Append).
      saveAsTable("temp")
    snc.sql(s"insert into $orderLineTable select * from temp ")
    // re execute base & aqp query
    this.assertInsertion()
    snc.dropTable("temp", true)
    this.dropSampleTable(snc)
  }

  def testDataframeWriteToBaseTable_1() {
    this.createSampleTable(snc)
    this.assertInsertion()
    this.orderLineExtDf.write.format(tableType).mode(SaveMode.Append).
      saveAsTable(orderLineTable)
    this.assertInsertion()
    this.dropSampleTable(snc)
  }

  def testDataframeWriteToBaseTable_2() {
    snc.dropTable(orderLineTable, true)
    this.createOrderTable(snc, false)
    this.createSampleTable(snc)
    this.assertInsertion()
    this.orderLineExtDf.write.format(tableType).mode(SaveMode.Append).
      saveAsTable(orderLineTable)
    this.assertInsertion()
    assert(snc.sql(s"select count(*) from $orderLineTable").collect()(0).getLong(0) > 0)
    this.dropSampleTable(snc)
  }

}
