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

import org.apache.spark.SparkConf
import io.snappydata.{Constant => Constants}
import io.snappydata.Property

/**
 * Created by ashahid on 5/24/16.
 */
class BootstrapAQPDataFrameAPIPart2Test extends AbstractAQPDataFrameAPIPart2Test {

  protected def addSpecificProps(conf: SparkConf): Unit = {
    conf.setAppName("BootstrapAQPDataFrameAPIPart2Test")
        .set(Property.ClosedFormEstimates.name, "false").set(Property.NumBootStrapTrials.name, "10")
  }

}
