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

import com.gemstone.gemfire.internal.AvailablePort
import io.snappydata.SnappyFunSuite
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SnappyContext, DataFrame, SaveMode}

/**
  * Created by vivekb on 19/5/16.
  */
class PartialQueryRoutingOnRowTableTestSuite extends PartialQueryRoutingTestSuite {

  // All the base table has been created as "row table"
  protected override val createTableExtension: String = "  using row "

  protected override def createBaseTable(snc: SnappyContext, testNo: Int): Unit = {
    snc.sql(s"create table order_line$testNo(" +
        "ol_w_id         integer," +
        "ol_d_id         integer," +
        "ol_o_id         integer," +
        "ol_number       integer," +
        "ol_i_id         integer," +
        "ol_amount       float(10)," +
        "ol_supply_w_id  integer," +
        "ol_quantity     float(10)," +
        "ol_dist_info    varchar(24)) " +
        createTableExtension )
  }
}
