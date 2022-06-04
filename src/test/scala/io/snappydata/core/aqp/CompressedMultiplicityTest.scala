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

import io.snappydata.{SnappyFunSuite, Constant => Constants}
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.AssertAQPAnalysis
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.bootstrap.{BootstrapFunctions, BootstrapMultiplicity}
import org.apache.spark.sql.execution.common.AQPRules

/**
 * Created by ashahid on 5/18/16.
 */
class CompressedMultiplicityTest extends SnappyFunSuite with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
  }

  override def afterAll(): Unit = {
    val snc = this.snc
    // cleanup metastore
    if (snc != null) {
      snc.clearCache()
    }
    super.afterAll()
    stopAll()
    System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "false")
  }

  test("bit mask of multiplicty columns") {
    var i = 0
   // val multiplicityExps = BoundReference(0, ArrayType(ByteType), nullable = false)

    // 1, 2, 0, 1,2 0, 1
    i = 1
    val columns = Array.fill[Any](BootstrapMultiplicity.groupSize)( {
      val temp = i
      i = i + 1
      (temp % 3).toByte
    }
    )
    val arrayData = new GenericArrayData(columns)
    val compressed = BootstrapFunctions.compressMultiplicty(arrayData, 0 , 6)

    logInfo("compressed byte =" + compressed)
    for(i <- 1 to BootstrapMultiplicity.groupSize) {
      val isOnAtPos = ((1 << (i -1)) & compressed) > 0
      assert(isOnAtPos === ((i % 3) > 0) )
    }
  }
}
