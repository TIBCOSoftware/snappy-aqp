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
package io.snappydata.app;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SampleDataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SnappyContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple4;

import static org.apache.spark.SnappyJavaUtils.snappyJavaUtil;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.count;

public class SnappyJavaBasicAPISuite implements Serializable {
  private transient JavaSparkContext jsc;
  private transient SnappyContext snc;

  @Before
  public void setUp() {
    SparkContext currentAcive = SnappyContext.globalSparkContext();
    if (currentAcive != null) {
      currentAcive.stop();
    }
    jsc = new JavaSparkContext("local", "SnappyJavaBasicAPISuite");
    snc = SnappyContext.apply(jsc);
  }

  @After
  public void tearDown() {
    if (jsc != null) {
      jsc.stop();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void createADataFrameAndTestRowTableInsert() {
    List<JavaBean> data = Arrays.asList(new JavaBean(1, 2, 3), new JavaBean(3, 4, 5));
    JavaRDD<JavaBean> s1 = jsc.parallelize(data);
    Dataset<Row> dataDF = snc.createDataFrame(s1, JavaBean.class);

    snc.sql("Create Table my_schema.MY_TABLE (a INT, b INT, c INT)");


    dataDF.write().format("row").mode(SaveMode.Append).saveAsTable("MY_SCHEMA.MY_TABLE");

    Dataset<Row> countDF = snc.sql("select * from MY_SCHEMA.MY_TABLE");
    Assert.assertEquals(countDF.count(), 2);


    snappyJavaUtil(dataDF.write().format("row").mode(SaveMode.Overwrite)).putInto("MY_SCHEMA.MY_TABLE");


    Assert.assertEquals(countDF.count(), 4);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void createADataFrameAndTestColumnTableInsert() {
    List<JavaBean> data = Arrays.asList(new JavaBean(1, 2, 3), new JavaBean(3, 4, 5));
    JavaRDD<JavaBean> s1 = jsc.parallelize(data);
    Dataset<Row> dataDF = snc.createDataFrame(s1, JavaBean.class);

    snc.sql("Create Table my_schema.MY_COLUMN_TABLE (a INT, b INT, c INT) using column options()");


    dataDF.write().format("column").mode(SaveMode.Append).saveAsTable("MY_SCHEMA.MY_COLUMN_TABLE");

    Dataset<Row> countDF = snc.sql("select * from MY_SCHEMA.MY_COLUMN_TABLE");
    Assert.assertEquals(countDF.count(), 2);
  }

  // Test ignored as CountDistinct is not supported on sample table
  @SuppressWarnings("unchecked")
  @Test
  public void createADataFrameAndTestStratifiedSampleForCountDistinct() {
    List<JavaBean> data = new ArrayList();
    for (int i = 1; i <= 100; i++) {
      data.add(new JavaBean(i % 10, (i + 1) % 20, (i + 2) % 15));
    }
    Assert.assertTrue(data.size() == 100);
    JavaRDD<JavaBean> s1 = jsc.parallelize(data);
    Dataset<Row> dataDF = snc.createDataFrame(s1, JavaBean.class);

    Map<String, Object> options = new HashMap<String, Object>();

    options.put("qcs", "col1");
    options.put("fraction", 0.1);
    options.put("strataReservoirSize", 50);

    SampleDataFrame sampleDataFrame = snappyJavaUtil(dataDF).stratifiedSample(options);

    sampleDataFrame.registerTempTable("JavaBean");
    Dataset<Row> sampledDataFrame = snc.table("JavaBean");

    dataDF.show(1000);
    sampleDataFrame.show(1000);
    System.out.println(sampleDataFrame.count());
    Assert.assertTrue(sampledDataFrame.count() > 0);

    java.util.Map<Row, Tuple4<Double, Double, Double, Double>> result =
        snappyJavaUtil(sampledDataFrame).errorEstimateAverage("col1", 0.75, null);
    for (Tuple4 error : result.values()) {
      System.out.println(error._1());
      break;
    }

    Set<String> sgroupedColumn = new HashSet();
    sgroupedColumn.add("col1");
    result = snappyJavaUtil(sampledDataFrame).errorEstimateAverage("col1", 0.75, sgroupedColumn);
    for (Tuple4 error : result.values()) {
      System.out.println(error._1());
      break;
    }

    Dataset<Row> gpDF = sampledDataFrame.groupBy().agg(countDistinct("col1", "col2"));
    Dataset<Row> weDF = snappyJavaUtil(gpDF).withError(0.5, 0.5);
    weDF.show();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void createADataFrameAndTestStratifiedSampleForCount() {
    List<JavaBean> data = new ArrayList();
    for (int i = 1; i <= 100; i++) {
      data.add(new JavaBean(i % 10, (i + 1) % 20, (i + 2) % 15));
    }
    Assert.assertTrue(data.size() == 100);
    JavaRDD<JavaBean> s1 = jsc.parallelize(data);
    Dataset<Row> dataDF = snc.createDataFrame(s1, JavaBean.class);

    Map<String, Object> options = new HashMap<String, Object>();

    options.put("qcs", "col1");
    options.put("fraction", 0.1);
    options.put("strataReservoirSize", 50);

    SampleDataFrame sampleDataFrame = snappyJavaUtil(dataDF).stratifiedSample(options);

    sampleDataFrame.registerTempTable("JavaBean");
    Dataset<Row> sampledDataFrame = snc.table("JavaBean");
    System.out.println(sampleDataFrame.count());
    Assert.assertTrue(sampledDataFrame.count() > 0);

    java.util.Map<Row, Tuple4<Double, Double, Double, Double>> result =
        snappyJavaUtil(sampledDataFrame).errorEstimateAverage("col1", 0.75, null);
    for (Tuple4 error : result.values()) {
      System.out.println(error._1());
      break;
    }

    Set<String> sgroupedColumn = new HashSet();
    sgroupedColumn.add("col1");
    result = snappyJavaUtil(sampledDataFrame).errorEstimateAverage("col1", 0.75, sgroupedColumn);
    for (Tuple4 error : result.values()) {
      System.out.println(error._1());
      break;
    }

    Dataset<Row> gpDF = sampledDataFrame.groupBy().agg(count("col1"));
    Dataset<Row> weDF = snappyJavaUtil(gpDF).withError(0.5, 0.5);
    weDF.show();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void createRDDAndTestRDDImplicits() {
    JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4));

    JavaRDD<String> strings = snappyJavaUtil(rdd).mapPreserve(
        new Function<Integer, String>() {
          @Override
          public String call(Integer i) {
            return i.toString();
          }
        });
    Assert.assertEquals(4, strings.count());

    JavaRDD<String> words = snappyJavaUtil(rdd).mapPartitionsPreserve(
        new FlatMapFunction<Iterator<Integer>, String>() {
      @Override
      public Iterator<String> call(java.util.Iterator<Integer> x) {
        List<String> strList = new ArrayList(4);
        while (x.hasNext()) {
          strList.add(x.next().toString());
        }
        return strList.iterator();
      }
    }, true);

    Assert.assertEquals(4, words.count());

    JavaRDD<String> words1 = snappyJavaUtil(rdd).mapPartitionsPreserveWithIndex(
        new FlatMapFunction<Tuple2<Integer, java.util.Iterator<Integer>>, String>() {
      @Override
      public Iterator<String> call(Tuple2<Integer, Iterator<Integer>> x) {
        List<String> strList = new ArrayList(4);
        while (x._2().hasNext()) {
          strList.add(x._2().next().toString());
        }
        return strList.iterator();
      }
    }, true);

    Assert.assertEquals(4, words1.count());

  }

  public static class JavaBean implements Serializable {
    private int col1 = 0;
    private int col2 = 0;
    private int col3 = 0;

    public JavaBean(int col1, int col2, int col3) {
      this.col1 = col1;
      this.col2 = col2;
      this.col3 = col3;
    }

    public int getCol1() {
      return col1;
    }

    public int getCol2() {
      return col2;
    }

    public int getCol3() {
      return col3;
    }
  }
}
