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

import io.snappydata.{Property, SnappyFunSuite}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.common.AQPRules
import org.apache.spark.sql.types._
import org.apache.spark.{AssertAQPAnalysis, Logging, SparkConf}

class QueryProcessingSuite
    extends SnappyFunSuite
        with Logging {

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
    AQPRules.setTestHookStoreAQPInfo(AssertAQPAnalysis)
  }

  override def afterAll(): Unit = {
    val snc = this.snc
    // cleanup metastore
    if (snc != null) {
      snc.clearCache()
    }
    super.afterAll()
    stopAll()
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
    new SparkConf().setAppName("Simple Application")
        .setMaster("local[6]")
        .set(io.snappydata.Property.ClosedFormEstimates.name, "true")
        .set(Property.TestDisableCodeGenFlag.name , "true")
        .set(io.snappydata.Property.UseOptimizedHashAggregateForSingleKey.name, "true")
  }

  // Create an RDD
  val people = Seq(Seq("John", 30), Seq("Tora", 31),
    Seq("Mark", 40), Seq("Rob", 40), Seq("Tommy", 40), Seq("Tod", 43))
  val sample_people = Seq(Seq("John", 30), Seq("Tommy", 40), Seq("Tod", 43))
  val sample_people_1 = Seq(Seq("Tora", 31), Seq("Tommy", 40), Seq("Tod", 43))
  var expected = ""

  test("Run a sum query on sample table if pre query triage qualifies the query to run on " +
      "sample table") {

    // Generate the schema based on the string of schema
    // val schemaTypes = List(StringType, IntegerType)
    // val schema = StructType(schemaString.split(" ").zipWithIndex.map(
    // { case (fieldName, i) => StructField(fieldName, schemaTypes(i), true) }
    // ))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = sc.parallelize(people, people.length).map(s =>
      new DataX(s(0).asInstanceOf[String], s(1).asInstanceOf[Int]))

    val sample_rowRDD = sc.parallelize(sample_people, sample_people.length).map(s =>
      new DataX(s(0).asInstanceOf[String], s(1).asInstanceOf[Int]))

    val snContext = this.snc
    // Apply the schema to the RDD.
    val tempDataFrame = snContext.createDataFrame(rowRDD).toDF()
    tempDataFrame.write.parquet("people.parquet")

    val peopleDataFrame = snContext.read.parquet("people.parquet")
    val sample_peopleDataFrame = snContext.createDataFrame(sample_rowRDD)

    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people")

    /*
    val peopleSampled = peopleDataFrame.stratifiedSample(Map(
      "qcs" -> "age",
      "fraction" -> 0.1,
      "strataReservoirSize" -> 1))
     peopleSampled.registerTempTable("people_sampled")
     */

    snContext.createSampleTable("people_sampled", Some("people"),
      Map(
        "qcs" -> "age",
        "fraction" -> "0.01",
        "strataReservoirSize" -> "50",
       // "aqp.debug.byPassSampleOperator" -> "true",
        "buckets" -> "1"
      ),
      allowExisting = false)
    // clear all the rows added to sample table as the test relies on
    // data from explicit population of sample table..
    snc.sql("truncate table people_sampled")
    sample_peopleDataFrame.write.insertInto("people_sampled")

    // Run query on actual table
    val result = snContext.sql("SELECT sum(age) FROM people where age > 35")

    expected = "[163]"
    result.collect().foreach(verifyResult)

    // Run query on sample table
    val sampled_result = snContext.sql("SELECT sum(age) FROM people where age > 35 " +
        "WITH ERROR 0.05 behavior 'DO_NOTHING'")


    AssertAQPAnalysis.bypassErrorCalc(snc)
    expected = "[83]"
    val rs = sampled_result.collect()
    snc.sql("select * from people_sampled").show
    sampled_result.collect().foreach(verifyResult)


    snContext.dropTable("people_sampled")
  }

  test("Run a avg query on sample table if pre query triage qualifies the query to run" +
      " on sample table") {

    // Generate the schema based on the string of schema
    // val schemaTypes = List(StringType, IntegerType)
    // val schema = StructType(schemaString.split(" ").zipWithIndex.map(
    // { case (fieldName, i) => StructField(fieldName, schemaTypes(i), true) }
    // ))

    val sample_rowRDD = sc.parallelize(sample_people, sample_people.length).map(s =>
      new DataX(s(0).asInstanceOf[String], s(1).asInstanceOf[Int]))

    val snContext = this.snc
    // Apply the schema to the RDD.
    val sample_peopleDataFrame = snContext.createDataFrame(sample_rowRDD)

    // Register the DataFrames as a table.
    snContext.createSampleTable("people_sampled", Some("people"),
      sample_peopleDataFrame.schema, Map(
        "qcs" -> "age",
        "fraction" -> "0.01",
        "strataReservoirSize" -> "50",
        "buckets" -> "1"
      ))
    // clear all the rows added to sample table as the test relies on
    // data from explicit population of sample table..
    snc.sql("truncate table people_sampled")
    sample_peopleDataFrame.write.insertInto("people_sampled")

    // Run query on actual table
    val result = snContext.sql("SELECT AVG(age) FROM people where age > 35")
    expected = "[40.75]"
    result.collect().foreach(verifyResult)

    // Run query on sample table
    val sampled_result = snContext.sql(
      "SELECT AVG(age) FROM people where age > 35 WITH ERROR 0.085 behavior 'DO_NOTHING'")
    AssertAQPAnalysis.bypassErrorCalc(snc)
    expected = "[41.5]"
    sampled_result.collect.foreach(verifyResult)
  }

  test("Run query on appropriate sample table if multiple sample tables " +
      "for a sample table available") {


    val sample_rowRDD = sc.parallelize(sample_people_1, sample_people_1.length).map(s =>
      new DataX(s(0).asInstanceOf[String], s(1).asInstanceOf[Int]))

    val snContext = this.snc
    val sample_peopleDataFrame1 = snContext.createDataFrame(sample_rowRDD)

    snContext.createSampleTable("people_sampled_1", Some("people"),
      sample_peopleDataFrame1.schema, Map(
        "qcs" -> "age,name",
        "fraction" -> "0.01",
        "strataReservoirSize" -> "50",
        "buckets" -> "1"))

    // clear all the rows added to sample table as the test relies on
    // data from explicit population of sample table..
    snc.sql("truncate table people_sampled")
    sample_peopleDataFrame1.write.insertInto("people_sampled_1")

    // Run query on appropriate sample table ( Expected to execute on people_sampled_1 table)
    val sampled_result = snContext.sql("SELECT AVG(age) " +
        "FROM people where age > 30 AND name like 'To%' WITH ERROR 0.85 CONFIDENCE 0.9")
    AssertAQPAnalysis.bootStrapAnalysis(snc)
    expected = "[38.0]"
    sampled_result.collect().foreach(verifyResult)
  }

  test("aqp query on external table AQP-282") {
    val orderLineTable = "orderline"
    val orderLineSampleTable = "orderlineSample"
    snc.sql(s"drop table if exists $orderLineTable ")


    val orderLineCSV = getClass.getResource("/ORDER_LINE.csv").getPath
    snc.createExternalTable(orderLineTable, "csv",
      Map("path" -> orderLineCSV, "inferSchema" -> "true"))
   // snc.table(orderLineTable).printSchema()
    snc.sql(s"CREATE SAMPLE TABLE $orderLineSampleTable" +
      s" OPTIONS(qcs '_c6, _c1', fraction '0.01', strataReservoirSize '50'," +
      s" baseTable '$orderLineTable')")

    val rs = snc.sql(s"select sum (_c0) as summ, absolute_error(summ)  " +
      s"from $orderLineTable group by _c6 with error")
    rs.collect()
    AssertAQPAnalysis.closedFormAnalysis(this.snc, 0)
    snc.dropTable(orderLineSampleTable, true)
    snc.dropTable(orderLineTable, true)
  }

  def verifyResult(r: Row): Unit = {
    logInfo(r.toString())
    val x = if (r.schema.head.dataType.isInstanceOf[StructType]) {
      val struct = r.getStruct(0)
      struct.get(0).toString
    } else {
      r.toString()
    }
    assert(x == expected)
  }
}

case class DataX(name: String, age: Int) extends Serializable
