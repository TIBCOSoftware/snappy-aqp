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

class BootstrapAccuracyDUnitSingleTest  (val s: String) extends ClusterManagerTestBase(s) {

  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE

  val bootStrapTrials = 100 // default
  bootProps.put(io.snappydata.Property.NumBootStrapTrials.name, s"$bootStrapTrials")
  bootProps.put(io.snappydata.Property.ClosedFormEstimates.name, "false")
  bootProps.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  bootProps.put("spark.kryo.registrator",
    "org.apache.spark.sql.execution.serializer.SnappyKryoRegistrator")
  // bootProps.put("spark.executor.instances", "8")

  var testInstance: BootStrapAccuracySuite = _

  override def tearDown2(): Unit = {
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
    val driver = "com.pivotal.gemfirexd.jdbc.ClientDriver"
    Class.forName(driver).newInstance //scalastyle:ignore
    val url = s"jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  def setupData(): Unit = {
    this.testInstance = new BootStrapAccuracySuite()
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke( new SerializableRunnable() {
     def run(): Unit = {
       System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
     }
    }))
    this.testInstance.setupData()
  }


  // To enable the test, uncomment the two lines
  def testAccuracy(): Unit = {
    // this.setupData()
    // this.testInstance.testBounds()
  }
}
