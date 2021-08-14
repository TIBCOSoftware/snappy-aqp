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

import java.sql.{Connection, DriverManager, SQLException}

import scala.util.Try

import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.Constant.DEFAULT_SCHEMA
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper
import org.junit.Assert._
import org.scalatest.Assertions

import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.sources.JdbcExtendedUtils
import org.apache.spark.sql.{AnalysisException, SaveMode, SnappyContext}

class AQPCatalogConsistencyDUnitTest(val s: String) extends ClusterManagerTestBase(s)
    with Assertions {

  var connection: Connection = _
  var queryRoutingDisabledConnection: Connection = _

  private val props = Map.empty[String, String]
  private val schema = DEFAULT_SCHEMA
  private val baseTableName = "AIRLINE"
  private val sampleTableName = "AIRLINE_SAMPLED"
  private val expectedRecordCount = 1888622

  var netPort: Int = _
  var snc: SnappyContext = _

  override def setUp(): Unit = {
    super.setUp()
    snc = SnappyContext(sc)
    netPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort)
    connection = getConnection()
    queryRoutingDisabledConnection = getConnection(false)
  }

  override def tearDown2(): Unit = {
    Try(connection.close())
    Try(queryRoutingDisabledConnection.close())
    super.tearDown2()
  }

  def assertTableDoesNotExist(queryRoutingDisabledConnection: Connection,
      table: String): Unit = {
    intercept[AnalysisException] {
      snc.snappySession.externalCatalog.getTable(schema.toLowerCase, table.toLowerCase)
    }
    intercept[AnalysisException] {
      snc.snappySession.sessionCatalog.lookupRelation(
        snc.snappySession.tableIdentifier(table))
    }

    val ex1 = intercept[SQLException] {
      queryRoutingDisabledConnection.createStatement().executeQuery("select * from " + table)
    }
    assertEquals("42X05", ex1.getSQLState)

    // verify that column batch table is dropped
    val ex2 = intercept[SQLException] {
      queryRoutingDisabledConnection.createStatement().executeQuery(
        "select * from " + ColumnFormatRelation.columnBatchTableName(s"$schema.$table"))
    }
    assertEquals("42X05", ex2.getSQLState)
  }

  private def createSampleTable() = {
    snc.createSampleTable(sampleTableName.toUpperCase, Option(baseTableName),
      Map("qcs" -> "uniqueCarrier ,yeari ,monthi", "fraction" -> "0.5", "strataReservoirSize" ->
          "25", "buckets" -> "57"), allowExisting = false)
  }

  def getConnection(routeQuery: Boolean = true): Connection = {
    val url = if (!routeQuery) {
      s"jdbc:snappydata://localhost:$netPort/route-query=false;internal-connection=true"
    } else {
      s"jdbc:snappydata://localhost:$netPort"
    }
    DriverManager.getConnection(url)
  }

  def testHiveCatalogEntryMissing(): Unit = {
    var inputData: String = getClass.getResource("/2015.parquet").getPath
    val dataDF = snc.read.load(inputData)
    snc.createTable(baseTableName, "column", dataDF.schema, props)
    createSampleTable()
    removeTableEntryFromHiveStore()
    checkTableExistenceInDD()

    def verifyThatTheTableDontExistInHiveStore(): Unit = {
      val ex = intercept[AnalysisException] {
        snc.sql(s"SELECT * FROM $sampleTableName")
      }
      assert(ex.message.startsWith("Table or view not found"))
    }

    verifyThatTheTableDontExistInHiveStore()

    // attempt to repair the catalog with removeTableWithData flag off
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'false')")

    // table should be deleted since table doesn't contain any data
    assertTableDoesNotExist(queryRoutingDisabledConnection, sampleTableName)

    createSampleTable()
    // populate tables with some data
    dataDF.write.insertInto(baseTableName)
    dataDF.write.insertInto(sampleTableName)

    removeTableEntryFromHiveStore()

    checkTableExistenceInDD()
    verifyThatTheTableDontExistInHiveStore()

    // attempt to repair the catalog with removeTableWithData flag off
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'false')")

    // table should not be deleted as it is having data
    checkTableExistenceInDD()

    val exception = intercept[SQLException] {
      createSampleTable()
    }
    assertEquals("X0Y32", exception.getSQLState)

    // repair the catalog with removeTableWithData flag on
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")

    assertTableDoesNotExist(queryRoutingDisabledConnection, sampleTableName)

    // verify that the base table is intact
    assertEquals(expectedRecordCount, snc.table(baseTableName).count())

    // try creating and populating sample table again and verify it's working fine
    createSampleTable()
    dataDF.write.insertInto(sampleTableName)
    assert(snc.table(sampleTableName).count() > 0)
    assertEquals(expectedRecordCount, snc.table(baseTableName).count())
  }

  private def checkTableExistenceInDD(): Unit = {
    queryRoutingDisabledConnection.createStatement()
        .executeQuery(s"select * from $sampleTableName").close()
  }

  private def removeTableEntryFromHiveStore(): Unit = {
    snc.snappySession.sessionCatalog.externalCatalog.dropTable(schema, sampleTableName,
      ignoreIfNotExists = false, purge = false)
  }

  def testNoEntryInDataDictionary(): Unit = {
    var inputData: String = getClass.getResource("/2015.parquet").getPath
    val dataDF = snc.read.load(inputData)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(baseTableName)
    createSampleTable()
    dataDF.write.insertInto(sampleTableName)
    assertEquals(expectedRecordCount, snc.table(baseTableName).count())
    assert(snc.table(sampleTableName).count() > 0)

    checkTableExistenceInDD()

    def dropSampleTableFromStore: Boolean = {
      val resolvedSampleTableName = s"$schema.$sampleTableName"
      val reservoirRegionName = Misc.getReservoirRegionNameForSampleTable(schema,
        resolvedSampleTableName)
      connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")
      val query = "CALL SYS.CREATE_OR_DROP_RESERVOIR_REGION(?, ?, ?)"
      val stmt = connection.prepareStatement(query)
      stmt.setString(1, reservoirRegionName)
      stmt.setString(2, resolvedSampleTableName)
      stmt.setBoolean(3, true)
      stmt.executeUpdate()
      stmt.close()
      queryRoutingDisabledConnection.createStatement().execute(s"drop table " +
          ColumnFormatRelation.columnBatchTableName(resolvedSampleTableName))
      queryRoutingDisabledConnection.createStatement().execute(s"drop table $sampleTableName")
    }

    dropSampleTableFromStore

    // make sure that the table is deleted from data dictionary
    val ex: SQLException = intercept[SQLException] {
      checkTableExistenceInDD()
    }
    assertEquals("42X05", ex.getSQLState)

    // make sure that the table exists in Hive metastore
    // should not throw an exception
    val (sch, tab) = JdbcExtendedUtils.getTableWithSchema(sampleTableName, conn = null,
      Some(snc.snappySession))
    snc.snappySession.externalCatalog.getTable(sch, tab)

    // Repair catalog with removeTablesWithData flag false. This should not drop the table.
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('false', 'true')")

    // make sure that the table exists in Hive metastore.
    // should not throw an exception
    snc.snappySession.externalCatalog.getTable(sch, tab)

    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")

    assertTableDoesNotExist(queryRoutingDisabledConnection, sampleTableName)

    // try creating and populating sample table to make sure that it is working as expected
    createSampleTable()
    dataDF.write.insertInto(sampleTableName)
    assertEquals(expectedRecordCount, snc.table(baseTableName).count())
    assert(snc.table(sampleTableName).count() > 0)
  }


  // Hive entry missing but DD entry exists
  def testCatalogRepairedWhenLeadStopped_HiveEntryMissing(): Unit = {
    var inputData: String = getClass.getResource("/2015.parquet").getPath
    val dataDF = snc.read.load(inputData)

    snc.createTable(baseTableName, "column", dataDF.schema, props)
    dataDF.write.insertInto(baseTableName)
    createSampleTable()

    // remove sample table entry from Hive store but not from store DD
    snc.snappySession.sessionCatalog.dropTable(
      snc.snappySession.tableIdentifier(sampleTableName), ignoreIfNotExists = false, purge = false)

    // stop spark
    val sparkContext = SnappyContext.globalSparkContext
    if (sparkContext != null) sparkContext.stop()
    ClusterManagerTestBase.stopAny()

    // repair the catalog
    // does not actually repair, just adds warning to log file
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('false', 'false')")
    // actually repair the catalog
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")

    ClusterManagerTestBase.startSnappyLead(ClusterManagerTestBase.locatorPort, bootProps)
    snc = SnappyContext(sc)
    val queryRoutingDisabledConnection = getConnection(false)
    // sample table should not be found in either catalog after repair
    assertTableDoesNotExist(queryRoutingDisabledConnection, sampleTableName)

    // base table should be intact
    assertEquals(expectedRecordCount, snc.table(baseTableName).count())
  }
}
