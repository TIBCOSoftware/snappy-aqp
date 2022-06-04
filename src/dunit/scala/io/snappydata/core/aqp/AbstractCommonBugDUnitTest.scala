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

import java.sql.{Connection, DriverManager}

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.{AvailablePortHelper, SerializableRunnable}
import io.snappydata.{Constant => Constants}

abstract class AbstractCommonBugDUnitTest (val s: String) extends ClusterManagerTestBase(s) {

  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE
  val testInstance: CommonBugTest = this.createTestInstance


  def createTestInstance: CommonBugTest

  override def tearDown2(): Unit = {
    // this.testInstance.afterAll()
    // reset the chunk size on lead node

    setDMLMaxChunkSize(default_chunk_size)
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "false")
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke( new SerializableRunnable() {
      def run(): Unit = {
        System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "false")
      }
    }))
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

    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke( new SerializableRunnable() {
      def run(): Unit = {
        System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
      }
    }))

    this.testInstance.beforeTest()
  }


  def _testSnap_806_1(): Unit = {
    this.testInstance.testSnap_806_1()
    this.testInstance.afterTest()
  }

  def _testSnap_806_2(): Unit = {
    this.testInstance.testSnap_806_2()
    this.testInstance.afterTest()
  }

  def _testSnap822_1(): Unit = {
    this.testInstance.testSnap822_1()
    this.testInstance.afterTest()
  }

  def _testSnap822_2(): Unit = {
    this.testInstance.testSnap822_2()
    this.testInstance.afterTest()
  }

  def _testQueryWithErrorAndWhereClauseOnSampleTable(): Unit = {
    this.testInstance.testQueryWithErrorAndWhereClauseOnSampleTable()
    this.testInstance.afterTest()
  }

  def _testCountQueryOnColumn(): Unit = {
    this.testInstance.testCountQueryOnColumn()
    this.testInstance.afterTest()
  }

  def _testAggregatesOnNullColumns_1(): Unit = {
    this.testInstance.testAggregatesOnNullColumns_1()
    this.testInstance.afterTest()
  }

  def _testAggregateOnNullColumn_2(): Unit = {
    this.testInstance.testAggregateOnNullColumn_2()
    this.testInstance.afterTest()
  }

  def _testSnap806(): Unit = {
    this.testInstance.testSnap806()
    this.testInstance.afterTest()
  }

  def _testSnap823(): Unit = {
    this.testInstance.testSnap823()
    this.testInstance.afterTest()
  }

  def _testConsistencyOfSumCountAvgs(): Unit = {
    this.testInstance.testConsistencyOfSumCountAvg()
    this.testInstance.afterTest()
  }

  def _testAQP128_AQP96(): Unit = {
    this.testInstance.testAQP128_AQP96_AQP206_AQP77()
    this.testInstance.afterTest()
  }

  def testDummy(): Unit = {

  }
}
