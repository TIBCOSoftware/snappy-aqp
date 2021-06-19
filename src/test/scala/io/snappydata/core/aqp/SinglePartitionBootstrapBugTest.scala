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

import org.apache.spark.SparkConf



class SinglePartitionBootstrapBugTest extends BootStrapBugTest {

  override protected def addSpecificProps(conf: SparkConf): Unit = {
    super.addSpecificProps(conf)
    conf.set("spark.master", "local[1]")
  }

  // Disabling these tests as they take lot of time
  override def testAQP224_AQP247(): Unit = {}
  override def testAQP231_1(): Unit = {}
  override def testAQP233_AQP249(): Unit = {}
  override def testAQP231_2(): Unit = {}
  override def testAQP225(): Unit = {}
  override def testAQP217(): Unit = {}
  override def testBugAQP_204(): Unit = {}
  override def testBugIndexOutOfBoundsException(): Unit = {}
}
