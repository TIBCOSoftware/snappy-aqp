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

import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.{Attribute, TestUtil}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}

import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.execution.common.{AQPRules, HAC}
import org.apache.spark.sql.{DataFrame, SaveMode}

class AQPConnectionPropertySetTest
  extends SnappyFunSuite
    with BeforeAndAfterAll {
  var serverHostPort: String = _
  val error = 0.5d
  val confidence = 0.99d
  var behavior = "local_omit"
  val hfile: String = getClass.getResource("/2015.parquet").getPath
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
    serverHostPort = TestUtil.startNetServer()

  }

  override def afterAll(): Unit = {
    val snc = this.snc
    // cleanup metastore
    if (snc != null) {
      snc.clearCache()
    }
    TestUtil.stopNetServer()
    super.afterAll()
    stopAll()


  }

  protected override def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
    new org.apache.spark.SparkConf().setAppName("AQPConnectionPropertySetTest")
      .setMaster("local[6]")
      .set("spark.logConf", "true")
  }

  def setupData(): Unit = {

    val snContext = this.snc
    snContext.sql("set spark.sql.shuffle.partitions=6")
    val airlineDataFrame: DataFrame = snContext.read.load(hfile)
    airlineDataFrame.write.format("column").mode(SaveMode.Append)
      .saveAsTable("airline")

    snc.sql(s"CREATE TABLE airlineSampled $ddlStr" +
      "USING column_sample " +
      "options " +
      "(" +
      "qcs 'UniqueCarrier' ," +
      "fraction '0.03'," +
      "strataReservoirSize '50', " +
      "baseTable 'airline')")

    airlineDataFrame.write.format("column_sample").mode(SaveMode.Append)
      .saveAsTable("airlineSampled")
  }

  test("AQP-285") {
    val conn = getConnection()
    val stmt = conn.createStatement()
    val rs1 = stmt.executeQuery(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier""")
    val results1 = scala.collection.mutable.ArrayBuffer[Double]()
    while (rs1.next()) {
      results1 += rs1.getDouble(1)
    }
    assertTrue(results1.size > 0)


    val rs2 = stmt.executeQuery(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier""")
    val results2 = scala.collection.mutable.ArrayBuffer[Double]()
    while (rs2.next()) {
      results2 += rs2.getDouble(1)
    }

    results1.zip(results2).foreach(tuple => assertTrue(tuple._1 == tuple._2))

    stmt.execute(s"set spark.sql.aqp.error=$error")


    val rs3 = stmt.executeQuery(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier""")
    val results3 = scala.collection.mutable.ArrayBuffer[Double]()
    while (rs3.next()) {
      results3 += rs3.getDouble(1)
    }
    assertTrue(results3.size > 0)
    results1.zip(results3).foreach(tuple => assertFalse(tuple._1 == tuple._2))
    // now disable the error
    stmt.execute(s"set spark.sql.aqp.error=-1d")
    val rs4 = stmt.executeQuery(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier""")
    val results4 = scala.collection.mutable.ArrayBuffer[Double]()
    while (rs4.next()) {
      results4 += rs4.getDouble(1)
    }

    results1.zip(results4).foreach(tuple => assertTrue(tuple._1 == tuple._2))
  }

  test("error clause or error property is mandatory for aqp") {
    val conn = getConnection()
    val stmt = conn.createStatement()
    val rs1 = stmt.executeQuery(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier""")
    val results1 = scala.collection.mutable.ArrayBuffer[Double]()
    while (rs1.next()) {
      results1 += rs1.getDouble(1)
    }
    assertTrue(results1.size > 0)

    val rs2 = stmt.executeQuery(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier""")
    val results2 = scala.collection.mutable.ArrayBuffer[Double]()
    while (rs2.next()) {
      results2 += rs2.getDouble(1)
    }

    results1.zip(results2).foreach(tuple => assertTrue(tuple._1 == tuple._2))

    stmt.execute(s"set spark.sql.aqp.confidence=$confidence")

    val rs3 = stmt.executeQuery(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier""")
    val results3 = scala.collection.mutable.ArrayBuffer[Double]()
    while (rs3.next()) {
      results3 += rs3.getDouble(1)
    }
    assertTrue(results3.size > 0)
    results1.zip(results3).foreach(tuple => assertTrue(tuple._1 == tuple._2))


    stmt.execute(s"set spark.sql.aqp.error=.5d")
    val rs4 = stmt.executeQuery(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier""")
    val results4 = scala.collection.mutable.ArrayBuffer[Double]()
    while (rs4.next()) {
      results4 += rs4.getDouble(1)
    }

    results1.zip(results4).foreach(tuple => assertFalse(tuple._1 == tuple._2))
  }

  test("AQP-285 with HAC changes") {
    val conn = getConnection()
    val stmt = conn.createStatement()
    val rs1 = stmt.executeQuery(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier""")
    val results1 = scala.collection.mutable.ArrayBuffer[Double]()
    var actualNumRows = 0
    while (rs1.next()) {
      actualNumRows += 1
      results1 += rs1.getDouble(1)
    }
    assertTrue(results1.size > 0)
    // set hac property but not error property. AQP should not get used
    stmt.execute(s"set spark.sql.aqp.behavior=PARTIAL_RUN_ON_BASE_TABLE")
    val rs2 = stmt.executeQuery(
      """SELECT avg(ArrDelay), UniqueCarrier FROM airline GROUP BY
            UniqueCarrier order by UniqueCarrier""")
    val results2 = scala.collection.mutable.ArrayBuffer[Double]()
    while (rs2.next()) {
      results2 += rs2.getDouble(1)
    }
    results1.zip(results2).foreach(tuple => assertTrue(tuple._1 == tuple._2))

    // Now set the error
    stmt.execute(s"set spark.sql.aqp.error=0.2d")
    // All the relative errors should be 0 or less than 0.2d, as partial routing is set
    // on connection
    val rs3 = stmt.executeQuery(
      """select uniquecarrier,sum(arrdelay) as x, absolute_error( x ),
        |relative_error( x ) from airline group by uniquecarrier""".stripMargin)

    var numRows3 = 0
    var numNonZeroErrorRows = 0
    while (rs3.next()) {
      val relativeError = rs3.getDouble(4)
      numRows3 += 1
      if (!rs3.wasNull()) {

        assertTrue(relativeError == 0 || relativeError <= .2d)
        if (relativeError != 0) {
          numNonZeroErrorRows += 1
        }
      }
    }
    assertTrue(numNonZeroErrorRows > 0)
    assertEquals(actualNumRows, numRows3)

    // rexecute the same query again & check the behaviour
    val rs5 = stmt.executeQuery(
      """select uniquecarrier,sum(arrdelay) as x, absolute_error( x ),
        |relative_error( x ) from airline group by uniquecarrier""".stripMargin)

    var numRows5 = 0
    var numNonZeroErrorRows5 = 0
    while (rs5.next()) {
      val relativeError = rs5.getDouble(4)
      numRows5 += 1
      if (!rs5.wasNull()) {
        assertTrue(relativeError == 0 || relativeError <= .2d)
        if (relativeError != 0) {
          numNonZeroErrorRows5 += 1
        }
      }
    }
    assertTrue(numNonZeroErrorRows5 > 0)
    assertEquals(actualNumRows, numRows5)

    // Now change the HAC behaviour
    stmt.execute(s"set spark.sql.aqp.behavior=LOCAL_OMIT")
    val rs4 = stmt.executeQuery(
      """select uniquecarrier,sum(arrdelay) as x, absolute_error( x ),
        |relative_error( x ) from airline group by uniquecarrier""".stripMargin)
    var numRows4 = 0
    while (rs4.next()) {
      val relativeError = rs4.getDouble(4)
     if (!rs4.wasNull()) {
       numRows4 += 1
       assertTrue(relativeError <= .2d)
     }
    }
    assertTrue(numRows4 > 0 && numRows4 < actualNumRows )
    assertTrue(numRows4 < numRows3)


    stmt.execute(s"set spark.sql.aqp.behavior=strict")
    stmt.execute(s"set spark.sql.aqp.error=0.01d")
    try {
      stmt.executeQuery(
        """select uniquecarrier,sum(arrdelay) as x, absolute_error( x ),
          |relative_error( x ) from airline group by uniquecarrier""".stripMargin)
      fail("query should have failed as behaviour is strict")
    } catch {
      case e: Exception => assertTrue(e.toString.indexOf("limit") != -1)
      case x: Throwable => throw x
    }

  }


  private def getConnection(user: Option[String] = None): Connection = {
    val props = new Properties()
    if (user.isDefined) {
      props.put(Attribute.USERNAME_ATTR, user.get)
    }
    DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort", props)
  }
}
