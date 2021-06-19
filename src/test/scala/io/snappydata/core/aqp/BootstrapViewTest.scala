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
package io.snappydata.core.aqp;
import io.snappydata.Property
import org.junit.Assert.assertEquals

import org.apache.spark.{AssertAQPAnalysis, SparkConf}

class BootstrapViewTest extends AbstractViewTest {

  protected def addSpecificProps(conf: SparkConf): Unit = {
    conf.setAppName("BootstrapViewTest")
      .set(Property.ClosedFormEstimates.name, "false").set(Property.NumBootStrapTrials.name, "8")
      .set(Property.AqpDebugFixedSeed.name, "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator",
        "org.apache.spark.sql.execution.serializer.SnappyKryoRegistrator")

  }

  override def assertAnalysis(): Unit = {
    AssertAQPAnalysis.bootStrapAnalysis(this.snc)
  }

  test("SNAP-3131 : query on view which is formed on join of tables does not use aqp") {
    snc.setConf(Property.DisableHashJoin.name, "true")
    snc.sql(s"create view airline_view as " +
      s"(select t1.*, t2.description from $airlineMainTable t1, " +
      s"$airlineRefTable t2 where t1.uniquecarrier = t2.code)")

    var rs1 = snc.sql("select count(*) couunt, count(*) sample_count from airline_view with error")
    AssertAQPAnalysis.bypassErrorCalc(snc)

    var rs2 = snc.sql(s"select count(*) couunt, count(*) sample_count " +
      s" from $airlineMainTable t1, $airlineRefTable t2 where  t1.uniquecarrier = t2.code" +
      s" with error")
    AssertAQPAnalysis.bypassErrorCalc(snc)

    var row1 = rs1.collect()(0)
    var row2 = rs2.collect()(0)
    assertEquals(row1.getLong(0), row2.getLong(0))
    assertEquals(row1.getLong(1), row2.getLong(1))

    rs1 = snc.sql("select count(*) couunt, count(*) sample_count, absolute_error(couunt)," +
      "relative_error(couunt) from airline_view with error")
    assertAnalysis()

    rs2 = snc.sql(s"select count(*) couunt, count(*) sample_count, absolute_error(couunt)," +
      s" relative_error(couunt) from $airlineMainTable t1, $airlineRefTable t2 where " +
      s" t1.uniquecarrier = t2.code with error")
    assertAnalysis()

    row1 = rs1.collect()(0)
    row2 = rs2.collect()(0)
    assertEquals(row1.getLong(0), row2.getLong(0))
    assertEquals(row1.getLong(1), row2.getLong(1))
    assertEquals(row1.getDouble(2), row2.getDouble(2), 0d)
    assertEquals(row1.getDouble(3), row2.getDouble(3), 0d)
    snc.sql("drop view airline_view")
  }

}
