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

import io.snappydata.{Constant => Constants}


class BootstrapBugDUnitSingleTest (s: String) extends AbstractCommonBugDUnitTest(s) {


  val bootStrapTrials = 100 // default
  bootProps.put(io.snappydata.Property.NumBootStrapTrials.name, s"$bootStrapTrials")
  bootProps.put(io.snappydata.Property.ClosedFormEstimates.name, "false")
  bootProps.put(Constants.defaultBehaviorAsDO_NOTHING, "true")
  System.setProperty(Constants.defaultBehaviorAsDO_NOTHING, "true")
  bootProps.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  bootProps.put("spark.kryo.registrator",
    "org.apache.spark.sql.execution.serializer.SnappyKryoRegistrator")
  bootProps.put("spark.sql.shuffle.partitions", "6")

  def createTestInstance: CommonBugTest = new BootStrapBugTest

  // bootstrap specific tests

  // disabling the test till timing issue is sorted

  def _testSampleTableQueryCountAggWithErrorEstimates(): Unit = {
    this.testInstance.asInstanceOf[BootStrapBugTest]
        .testSampleTableQueryCountAggWithErrorEstimates()
    this.testInstance.afterTest()
  }

  def _testSampleQueryWithHavingClauseAsAggregateFunc(): Unit = {
    this.testInstance.asInstanceOf[BootStrapBugTest]
        .testSampleQueryWithHavingClauseAsAggregateFunc()
    this.testInstance.afterTest()
  }

  def _testMixedAggregatesWithGroupBy(): Unit = {
    this.testInstance.asInstanceOf[BootStrapBugTest].testMixedAggregatesWithGroupBy()
    this.testInstance.afterTest()
  }

  def _testSampleQueryOnAvgWithIntegerColWithErrorEstimates(): Unit = {
    this.testInstance.asInstanceOf[BootStrapBugTest]
        .testSampleQueryOnAvgWithIntegerColWithErrorEstimates()
    this.testInstance.afterTest()
  }

  def _testSampleQueryWithAliasOnSum(): Unit = {
    this.testInstance.asInstanceOf[BootStrapBugTest].testSampleQueryWithAliasOnSum()
    this.testInstance.afterTest()
  }

}
