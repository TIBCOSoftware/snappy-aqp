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

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

class CFEEFunctionWithDatasourceAPI_1_Test extends ClosedFormErrorEstimateFunctionTest {
  override protected def initTestTables(): Unit = {
    val snc = this.snc

    val mainTableDF = createTable(snc, mainTable)

    this.createTable(snc, sampleTable, isSample = true, mainTableDF, mainTable)
    val rows = snc.sql(s"select * from $sampleTable")
    rows.collect()
    this.initAirlineTables()
  }


  def createTable(sqlContext: SQLContext,
      tableName: String, isSample: Boolean = false,
      mainTableDataframe: DataFrame = null, mainTableName: String = null): DataFrame = {

    if (!isSample) {

      val schema = StructType(Seq(
        StructField("l_orderkey", IntegerType, nullable = false),
        StructField("l_partkey", IntegerType, nullable = false),
        StructField("l_suppkey", IntegerType, nullable = false),
        StructField("l_linenumber", IntegerType, nullable = false),
        StructField("l_quantity", FloatType, nullable = false),
        StructField("l_extendedprice", FloatType, nullable = false),
        StructField("l_discount", FloatType, nullable = false),
        StructField("l_tax", FloatType, nullable = false),
        StructField("l_returnflag", StringType, nullable = false),
        StructField("l_linestatus", StringType, nullable = false),
        StructField("l_shipdate", DateType, nullable = false),
        StructField("l_commitdate", DateType, nullable = false),
        StructField("l_receiptdate", DateType, nullable = false),
        StructField("l_shipinstruct", StringType, nullable = false),
        StructField("l_shipmode", StringType, nullable = false),
        StructField("l_comment", StringType, nullable = false),
        StructField("scale", IntegerType, nullable = false)
      ))

      sqlContext.sql("DROP TABLE IF EXISTS " + tableName)

      val people = sqlContext.sparkContext.textFile(LINEITEM_DATA_FILE)
        .map(_.split('|')).map(p => Row(p(0).trim.toInt, p(1).trim.toInt,
        p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toFloat, p(5).trim.toFloat,
        p(6).trim.toFloat, p(7).trim.toFloat,
        p(8).trim, p(9).trim, java.sql.Date.valueOf(p(10).trim),
        java.sql.Date.valueOf(p(11).trim), java.sql.Date.valueOf(p(12).trim),
        p(13).trim, p(14).trim, p(15).trim, p(16).trim.toInt))


      val dfx = sqlContext.createDataFrame(people, schema)
      // Create a table in snappy store
      val df = snc.createTable(tableName, "column",
        dfx.schema, Map("buckets" -> "5"))

      // Populate the table in snappy store
      dfx.write.mode(SaveMode.Append).saveAsTable(tableName)
      df
    } else {

      snc.sql(s"CREATE TABLE $tableName"  +
        " USING column_sample " +
        "options " +
        "(" +
        "qcs 'l_quantity'," +
        "fraction '0.999'," +
        "strataReservoirSize '50', " +
        s"baseTable '$mainTableName') AS (select * from $mainTableName)")
      logInfo("main table size=" + mainTableDataframe.count())

      null
    }

  }

  test("basic2 sanity test") {
    val result = snc.sql("SELECT avg(l_quantity) as x, lower_bound(x), " +
        "upper_bound(x), l_orderkey, absolute_error(x), " +
        s"relative_error(x) FROM $mainTable group by l_orderkey " +
        "having avg(l_quantity) > 25 order by  l_orderkey " +
        s"with error $DEFAULT_ERROR confidence .95")
    // avg(l_quantity)

    val schema = result.schema
    assert("x" === schema.fieldNames(0))
    assert("Lower_Bound(x)" === schema.fieldNames(1))
    assert("Upper_Bound(x)" === schema.fieldNames(2))
    assert("l_orderkey" === schema.fieldNames(3))
    assert("Absolute_Error(x)" === schema.fieldNames(4))
    assert("Relative_Error(x)" === schema.fieldNames(5))
    result.collect()
    this.assertAnalysis()
  }
}
