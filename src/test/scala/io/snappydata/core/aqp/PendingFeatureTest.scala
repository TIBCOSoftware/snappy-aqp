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

import com.gemstone.gemfire.internal.AvailablePort
import io.snappydata.{Property, SnappyFunSuite, Constant => Constants}
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.{AssertAQPAnalysis, SparkConf}
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class PendingFeatureTest extends SnappyFunSuite
with Matchers
with BeforeAndAfterAll{
  val lineTable: String = getClass.getSimpleName + "_" + "line"
  val mainTable: String = getClass.getSimpleName + "_" + "mainTable"
  val sampleTable: String = getClass.getSimpleName + "_" + "mainTable_sampled"

  // Set up sample & Main table
  val LINEITEM_DATA_FILE = getClass.getResource("/datafile.tbl").getPath

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
   // initTestTables()
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

  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    /**
     * Pls do not change the flag values of Property.TestDisableCodeGenFlag.name
     * and Property.UseOptimizedHashAggregateForSingleKey.name
     * They are meant to suppress CodegenFallback Plan so that optimized
     * byte buffer code path is tested & prevented from false passing.
     * If your test needs CodegenFallback, then override the newConf function
     * & clear the flag from the conf of the test locally.
     */
    val conf = new org.apache.spark.SparkConf()
        .setAppName("ErrorEstimationFunctionTest")
        .setMaster("local[6]")
        .set("spark.sql.hive.metastore.sharedPrefixes",
          "com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc," +
              "com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity," +
              "com.mapr.fs.jni,org.apache.commons")
        .set("master.ui.port", "" + AvailablePort.getRandomAvailablePort(0))
        .set(Property.TestDisableCodeGenFlag.name , "true")
        .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")

    addSpecificProps(conf)
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  protected def addSpecificProps(conf: SparkConf): Unit = {
    conf.setAppName("PendingFeatureTest").set(Property.ClosedFormEstimates.name, "false").
      set(Property.NumBootStrapTrials.name, "4")
  }

  protected def initTestTables(): Unit = {
    val snc = this.snc
    createLineitemTable(snc, lineTable)
    val mainTableDF = createLineitemTable(snc, mainTable)
    snc.createSampleTable(sampleTable, Some(mainTable),
       Map(
        "qcs" -> "l_quantity",
        "fraction" -> "0.01",
        "strataReservoirSize" -> "50"),
    allowExisting = false)

    mainTableDF.write.insertInto(sampleTable)
  }


  ignore("Sample Table Query on avg aggregate with error estimates should be correct") {
    val result = snc.sql(s"SELECT avg(l_quantity)  FROM $mainTable  confidence .95 ")
    result.show()
    val rows2 = result.collect()
    val estimate = rows2(0).getDouble(0)
    msg("estimate=" + estimate)

  }

  def msg(m: String): Unit = DebugUtils.msg(m)

  def createLineitemTable(sqlContext: SQLContext,
      tableName: String, isSample: Boolean = false): DataFrame = {


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
    sqlContext.sql("DROP TABLE IF EXISTS " + tableName + "_sampled")

    val people = sqlContext.sparkContext.textFile(LINEITEM_DATA_FILE)
        .map(_.split('|')).map(p => Row(p(0).trim.toInt, p(1).trim.toInt,
      p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toFloat, p(5).trim.toFloat,
      p(6).trim.toFloat, p(7).trim.toFloat,
      p(8).trim, p(9).trim, java.sql.Date.valueOf(p(10).trim),
      java.sql.Date.valueOf(p(11).trim), java.sql.Date.valueOf(p(12).trim),
      p(13).trim, p(14).trim, p(15).trim, p(16).trim.toInt))

    logInfo("total rows in table =" + people.collect().length)
    val df = if (isSample) {
      sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row],
        schema)

    } else {
      val dfx = sqlContext.createDataFrame(people, schema)
      dfx.registerTempTable(tableName)
      dfx
    }
    df
  }


}
