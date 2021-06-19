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

package io.snappydata.app.streaming
import java.io.PrintWriter
import java.sql.{DriverManager, SQLException, Statement}

import scala.util.{Failure, Random, Success, Try}

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.SnappyFunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import twitter4j.Status

import org.apache.spark.sql.streaming.{SchemaDStream, StreamToRowsConverter}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SnappyContext}
import org.apache.spark.streaming._

class AQPStreamingSuite
    extends SnappyFunSuite
    with Eventually
    with BeforeAndAfterAll with BeforeAndAfter {

  private var snsc: SnappyStreamingContext = _

  def framework: String = this.getClass.getSimpleName

  def batchDuration: Duration = Seconds(1)

  before {
    SnappyStreamingContext.getActive.foreach { _.stop(false, true) }
    snsc = new SnappyStreamingContext(sc, batchDuration)
  }

  after {
    baseCleanup(false)
    snsc.stop(false , true)
  }

  // TODO: Mark true for verbose print
  val doPrint : Boolean = false
  def verbosePrint(str : Any) : Unit = if (doPrint) println(str) //scalastyle:ignore

  ignore("SNAP-467") {
    try {
      snsc.sql("STREAMING INIT 2secs")

      snsc.sql("CREATE STREAM TABLE HASHTAGTABLE (hashtag string) USING twitter_stream " +
          "OPTIONS (consumerKey '**REMOVED**', " +
          "consumerSecret '**REMOVED**', " +
          "accessToken '**REMOVED**', " +
          "accessTokenSecret '**REMOVED**', " +
          "rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow')")

      snsc.sql("CREATE STREAM TABLE RETWEETTABLE " +
          "(retweetId long,retweetCnt int, retweetTxt string) " +
          "USING twitter_stream OPTIONS (consumerKey '**REMOVED**', " +
          "consumerSecret '**REMOVED**', " +
          "accessToken '**REMOVED**', " +
          "accessTokenSecret '**REMOVED**', " +
          "rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow')")

      snsc.sql("CREATE TOPK TABLE TOPKTABLE ON HASHTAGTABLE OPTIONS" +
          "(key 'hashtag', timeInterval '2000ms', size '10')")

      snsc.sql("STREAMING START")
      snsc.sql("STREAMING STOP")
      snsc.sql("DROP TABLE IF EXISTS TOPKTABLE")
      snsc.sql("DROP TABLE IF EXISTS HASHTAGTABLE")
      snsc.sql("DROP TABLE IF EXISTS RETWEETTABLE")

      snsc.sql("STREAMING INIT 2 SECS")

      snsc.sql("CREATE STREAM TABLE HASHTAGTABLE (hashtag string) USING twitter_stream " +
          "OPTIONS (consumerKey '**REMOVED**', " +
          "consumerSecret '**REMOVED**', " +
          "accessToken '**REMOVED**', " +
          "accessTokenSecret '**REMOVED**', " +
          "rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow')")

      snsc.sql("CREATE STREAM TABLE RETWEETTABLE " +
          "(retweetId long,retweetCnt int, retweetTxt string) " +
          "USING twitter_stream OPTIONS (consumerKey '**REMOVED**', " +
          "consumerSecret '**REMOVED**', " +
          "accessToken '**REMOVED**', " +
          "accessTokenSecret '**REMOVED**', " +
          "rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow')")

      snsc.sql("CREATE TOPK TABLE TOPKTABLE ON HASHTAGTABLE OPTIONS" +
          " (key 'hashtag', timeInterval '2000ms', size '10' )")

      snsc.sql("STREAMING START")

      // it should also work fine without dropping any tables
      snsc.sql("STREAMING STOP")

      snsc.sql("STREAMING INIT 2 SECS")
      snsc.sql("STREAMING START")

      snsc.sql("select * from HASHTAGTABLE").collect()
      snsc.sql("select * from RETWEETTABLE").collect()
      snsc.sql("select * from TOPKTABLE").collect()

    } finally {
      snsc.sql("STREAMING STOP")
      snsc.sql("DROP TABLE IF EXISTS TOPKTABLE")
      snsc.sql("DROP TABLE IF EXISTS HASHTAGTABLE")
      snsc.sql("DROP TABLE IF EXISTS RETWEETTABLE")
    }
  }

  ignore("streamingAQPJob") {

    val schema = StructType(List(StructField("hashtag", StringType)))

    snsc.sql("CREATE STREAM TABLE HASHTAGTABLE (hashtag string) using " +
        "twitter_stream options (" +
        "consumerKey '**REMOVED**', " +
        "consumerSecret '**REMOVED**', " +
        "accessToken '**REMOVED**', " +
        "accessTokenSecret '**REMOVED**', " +
        "rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow')")

    snsc.sql("CREATE STREAM TABLE RETWEETTABLE (retweetId long, retweetCnt int," +
        "retweetTxt string) using " +
        "twitter_stream options (" +
        "consumerKey '**REMOVED**', " +
        "consumerSecret '**REMOVED**', " +
        "accessToken '**REMOVED**', " +
        "accessTokenSecret '**REMOVED**', " +
        "rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow')")

    val retweetStream: SchemaDStream = snsc.registerCQ("SELECT * FROM RETWEETTABLE " +
        "window (duration 2 seconds, slide 2 seconds)")
    val hashtagStream: SchemaDStream = snsc.registerCQ("SELECT * FROM HASHTAGTABLE " +
        "window (duration 2 seconds, slide 2 seconds)")


    val topKOption = Map(
      "epoch" -> System.currentTimeMillis().toString,
      "timeInterval" -> "2000ms",
      "size" -> "10"
    )

    snsc.snappyContext.createApproxTSTopK("topktable", None, "hashtag",
      schema, topKOption)
    snsc.snappyContext.saveStream(hashtagStream,
      Seq("topktable"),
      None
    )

    val tableName = "retweetStore"

    snsc.snappyContext.dropTable(tableName, true)

    snsc.snappyContext.sql(s"CREATE TABLE $tableName (retweetId bigint, " +
        s"retweetCnt int, retweetTxt string) USING row options(PARTITION_BY 'retweetId')")

    retweetStream.foreachDataFrame(_.write.insertInto(tableName))

    snsc.start()
    val hashtagStream_dynamic: SchemaDStream = snsc.registerCQ("SELECT * FROM " +
        "HASHTAGTABLE window (duration 2 seconds, slide 2 seconds)")
    hashtagStream_dynamic.foreachDataFrame(df => {
      verbosePrint("---------- Top Tags after start (dynamicCQ) ----------") //scalastyle:ignore
      df.groupBy(df("hashtag")).agg("hashtag" -> "count").
          orderBy("count(hashtag)").collect().foreach(x => verbosePrint("dynamicCQ " + x)) // scalastyle:ignore

    })

    // Iterate over the streaming data for sometime and publish the results to a file.
    try {
      val rand = new Random()
      val end = System.currentTimeMillis + 15000
      while (end > System.currentTimeMillis()) {
        Thread.sleep(1000)
        verbosePrint("\n******** Top 10 hash tags for the last interval (queryTopK loop) *******\n") //scalastyle:ignore
        val ts = System.currentTimeMillis()
        if (rand.nextBoolean()) {
          verbosePrint(snsc.sql("select * from topKTable where " +
              s"startTime = '${ts - 2000}' and endTime = '$ts'")
              .collect().mkString("----------\n", "\n",
            "\n----------\n")) //scalastyle:ignore
        } else {
          verbosePrint(snsc.snappyContext.queryApproxTSTopK("topktable",
            ts - 2000, ts).collect().mkString("==========\n", "\n",
            "\n==========\n")) //scalastyle:ignore
        }
      }
      verbosePrint("\n************ Top 10 hash tags until now (queryTopK) ***************\n") //scalastyle:ignore

      snsc.snappyContext.queryApproxTSTopK("topktable").collect().foreach {
        result => verbosePrint(result.toString()) //scalastyle:ignore
      }
      verbosePrint("\n************ Top 10 hash tags until now (queryTopK) ***************\n") //scalastyle:ignore

      snsc.sql("select * from Topktable").collect().foreach {
        result => verbosePrint(result.toString()) //scalastyle:ignore
      }

      verbosePrint("\n####### Top 10 popular tweets using gemxd query #######\n") //scalastyle:ignore
      snsc.snappyContext.sql(s"select retweetId as RetweetId, retweetCnt as RetweetsCount, " +
          s"retweetTxt as Text from ${tableName} order by RetweetsCount desc limit 10")
          .collect.foreach(row => {
        verbosePrint(row.toString()) //scalastyle:ignore
      })

      verbosePrint("\n#######################################################") //scalastyle:ignore
    } finally {
      snsc.stop(false, true)
    }
  }

  ignore("sql stream sampling") {

    snsc.sql("create stream table tweetstreamtable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '**REMOVED**', " +
        "consumerSecret '**REMOVED**', " +
        "accessToken '**REMOVED**', " +
        "accessTokenSecret '**REMOVED**', " +
        "rowConverter 'io.snappydata.app.streaming.TweetToRowsConverter')")

    val tableStream = snsc.registerCQ("select * from tweetstreamtable" +
        " window (duration 2 seconds, slide 2 seconds)")

    snsc.snappyContext.createSampleTable("tweetstreamtable_sampled", None,
      tableStream.schema, Map(
        "qcs" -> "hashtag",
        "fraction" -> "0.05",
        "strataReservoirSize" -> "300",
        "timeInterval" -> "3m"))

    snsc.snappyContext.saveStream(tableStream, Seq("tweetstreamtable_sampled"), None)

    snsc.sql("create table rawStreamColumnTable(id long, " +
        "text string, " +
        "fullName string, " +
        "country string, " +
        "retweets int, " +
        "hashtag string) " +
        "using column " +
        "options(PARTITION_BY 'id')")

    var numTimes = 0
    tableStream.foreachDataFrame { df =>
      df.write.insertInto("rawStreamColumnTable")

      val top10Tags = snsc.sql("select count(*) as cnt, hashtag from " +
          "rawStreamColumnTable where length(hashtag) > 0 group by hashtag " +
          "order by cnt desc limit 10").collect()
      top10Tags.foreach(verbosePrint) // scalastyle:ignore

      numTimes += 1
      if ((numTimes % 18) == 1) {
        if (doPrint) snsc.sql("SELECT count(*) FROM rawStreamColumnTable").show()
      }

      val stop10Tags = snsc.sql("select count(*) as cnt, " +
          "hashtag from tweetstreamtable_sampled where length(hashtag) > 0 " +
          "group by hashtag order by cnt desc limit 10").collect()
      stop10Tags.foreach(verbosePrint) // scalastyle:ignore
    }

    snsc.sql("STREAMING START")
    // first wait till at least one RDD received
    var maxWaitCount = 180
    while (StreamingHelper.getGeneratedRDDs(tableStream).isEmpty) {
      Thread.sleep(500)
      if (maxWaitCount <= 0) {
        fail("No data received for over 3 minutes")
      }
      maxWaitCount -= 1
    }
    snsc.awaitTerminationOrTimeout(10 * 1000)
  }

  ignore("SNAP-249 aqp/topk tables with streamingSnappy") {

    snsc.sql("create stream table tweetstreamtable1 " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '**REMOVED**', " +
        "consumerSecret '**REMOVED**', " +
        "accessToken '**REMOVED**', " +
        "accessTokenSecret '**REMOVED**', " +
        "rowConverter 'io.snappydata.app.streaming.TweetToRowsConverter')")

    val tableStream = snsc.registerCQ("select * from tweetstreamtable1" +
        " window (duration 2 seconds, slide 2 seconds)")

    snsc.sql("create topK table TopkTable1 on tweetstreamTable1 " +
        "options(key 'hashtag', frequencyCol 'retweets')")

    snsc.start()
    // first wait till at least one RDD received
    var maxWaitCount = 180
    while (StreamingHelper.getGeneratedRDDs(tableStream).isEmpty) {
      Thread.sleep(1000)
      if (maxWaitCount <= 0) {
        fail("No data received for over 3 minutes")
      }
      maxWaitCount -= 1
    }
    eventually(timeout(60 seconds), interval(3 seconds)) {
      val df = snsc.snappyContext.queryApproxTSTopK("topKTable1", -1, -1)
      if (doPrint) df.show()
      val rows = df.collect()
      assert(rows.length > 0)
      assert(rows(0).getLong(1) > 0)
    }

    val df2 = snsc.sql("select * from TopKTable1")
    if (doPrint) df2.show()
    val rows2 = df2.collect()
    assert(rows2.length > 0)
    assert(rows2(0).getLong(1) > 0)

    snsc.awaitTerminationOrTimeout(5 * 1000)

    snsc.sql("drop table topktable1")
  }

  ignore("SNAP-463 topk tables data with SQL. Case 1 with time interval > batch interval") {

    // start network server
    val serverHostPort = TestUtil.startNetServer()
    verbosePrint(" Network server started.")

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val s = conn.createStatement()
    s.execute("streaming init 2 SECS")
    s.execute("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '**REMOVED**', " +
        "consumerSecret '**REMOVED**', " +
        "accessToken '**REMOVED**', " +
        "accessTokenSecret '**REMOVED**', " +
        "rowConverter 'io.snappydata.app.streaming.TweetToRowsConverter')")
    snsc.sql("create table fullTweetsTable (id long, text string, " +
        "fullName string, country string, retweets int, " +
        "hashtag string) using column")
    s.execute("create topk table topkTweets on tweetsTable options(" +
         "timeInterval '3000ms'," +
        "key 'hashtag', size '10')")

    val tweetsStream = SnappyStreamingContext.getInstance().get
        .getSchemaDStream("tweetsTable")
    tweetsStream.foreachDataFrame { df =>
      df.write.insertInto("fullTweetsTable")
    }

    val snc = SnappyContext()

    s.execute("streaming start")

    var loopBreak = false
    for (a <- 1 to 20 if !loopBreak) {

      Thread.sleep(2000)

      s.execute("select hashtag, count(hashtag) as cht from fullTweetsTable " +
          "group by hashtag order by cht desc limit 10")
      verbosePrint("\n\n-----  TOP Tweets  -----\n")
      var hasTags = false
      var rs = s.getResultSet
      val rs1Next = rs.next()
      if (a >= 5 && rs1Next) loopBreak = true
      if (a == 20) assert(rs1Next)
      if (rs1Next) do {
        val count = rs.getInt(2)
        if (count > 1) {
          hasTags = true
        }
        verbosePrint(s"${rs.getString(1)} ; $count")
      } while (rs.next())

      s.execute("select * from topkTweets order by EstimatedValue desc")
      val topKdf = snc.queryApproxTSTopK("topkTweets", null, null)
      val topKRes = topKdf.collect()
      verbosePrint("\n\n-----  TOPK Tweets  -----\n")
      rs = s.getResultSet
      var numResults = 0
      while (rs.next()) {
        val count = rs.getInt(2)
        if (hasTags) {
          assert(count > 1)
          hasTags = false
        } else {
          assert(count > 0)
        }
        verbosePrint(s"${rs.getString(1)} ; ${rs.getLong(2)} ; ${rs.getString(3)}")
        numResults += 1
      }
      verbosePrint(s"Num results=$numResults")
      assert(numResults <= 10)

      verbosePrint("\n\n-----  TOPK Tweets2  -----\n")
      topKRes.foreach(verbosePrint)
      numResults = topKRes.length
      verbosePrint(s"Num results=$numResults")
      assert(numResults <= 10)

      s.execute("select text, fullName from fullTweetsTable " +
          "where text like '%e%'")
      rs = s.getResultSet
      if (rs1Next || a == 20) assert(rs.next)
      while (rs.next()) {
        rs.getString(1)
        rs.getString(2)
      }
    }

    s.execute("streaming stop")

    Thread.sleep(2000)
    val currentTime = System.currentTimeMillis()
    val startTime = new java.sql.Timestamp(currentTime - 2000000L).toString
    val endTime = new java.sql.Timestamp(currentTime + 2000000L).toString
    val df1 = snc.sql("select hashtag, EstimatedValue, ErrorBoundsInfo from " +
        s"topkTweets where startTime='$startTime' and endTime='$endTime'")
    val df2 = snc.queryApproxTSTopK("topkTweets", startTime, endTime)
    val rs1 = df1.sort("hashtag").collect().map(_.toString())
    val rs2 = df2.sort("hashtag").collect().map(_.toString())

    if (!rs1.corresponds(rs2){_ == _}) {
      verbosePrint(s"ERROR Result1:\n  ${rs1.mkString("\n  ")}")
      verbosePrint(s"ERROR Result2:\n  ${rs2.mkString("\n  ")}")
    }

    s.execute("drop table fullTweetsTable")
    s.execute("drop table topktweets")
    s.execute("drop table tweetsTable")

    conn.close()
  }

  ignore("SNAP-463 topk tables data with SQL. Case 2 with time interval < batch interval") {

    // start network server
    val serverHostPort = TestUtil.startNetServer()
    verbosePrint(" Network server started.")

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val s = conn.createStatement()
    s.execute("streaming init 3secs")
    s.execute("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '**REMOVED**', " +
        "consumerSecret '**REMOVED**', " +
        "accessToken '**REMOVED**', " +
        "accessTokenSecret '**REMOVED**', " +
        "rowConverter 'io.snappydata.app.streaming.TweetToRowsConverter')")
    snsc.sql("create table fullTweetsTable (id long, text string, " +
        "fullName string, country string, retweets int, " +
        "hashtag string) using column")
    s.execute("create topk table topkTweets on tweetsTable options(" +
        "timeInterval '2000ms'," +
        "key 'hashtag', size '10')")

    val tweetsStream = SnappyStreamingContext.getInstance().get
        .getSchemaDStream("tweetsTable")
    tweetsStream.foreachDataFrame { df =>
      df.write.insertInto("fullTweetsTable")
    }

    val snc = SnappyContext()

    s.execute("streaming start")

    for (a <- 1 to 5) {

      Thread.sleep(2000)

      s.execute("select hashtag, count(hashtag) as cht from fullTweetsTable " +
          "group by hashtag order by cht desc limit 10")
      verbosePrint("\n\n-----  TOP Tweets  -----\n")
      var hasTags = false
      var rs = s.getResultSet
      if (a == 5) assert(rs.next)
      while (rs.next()) {
        val count = rs.getInt(2)
        if (count > 1) {
          hasTags = true
        }
        verbosePrint(s"${rs.getString(1)} ; $count")
      }

      s.execute("select * from topkTweets order by EstimatedValue desc")
      val topKdf = snc.queryApproxTSTopK("topkTweets", null, null)
      val topKRes = topKdf.collect()
      verbosePrint("\n\n-----  TOPK Tweets  -----\n")
      rs = s.getResultSet
      var numResults = 0
      while (rs.next()) {
        val count = rs.getInt(2)
        if (hasTags) {
          assert(count > 1)
          hasTags = false
        } else {
          assert(count > 0)
        }
        verbosePrint(s"${rs.getString(1)} ; ${rs.getLong(2)} ; ${rs.getString(3)}")
        numResults += 1
      }
      verbosePrint(s"Num results=$numResults")
      assert(numResults <= 10)

      verbosePrint("\n\n-----  TOPK Tweets2  -----\n")
      topKRes.foreach(verbosePrint)
      numResults = topKRes.length
      verbosePrint(s"Num results=$numResults")
      assert(numResults <= 10)

      s.execute("select text, fullName from fullTweetsTable " +
          "where text like '%e%'")
      rs = s.getResultSet
      if (a == 5) assert(rs.next)
      while (rs.next()) {
        rs.getString(1)
        rs.getString(2)
      }
    }
    s.execute("streaming stop")
    s.execute("drop table fullTweetsTable")
    s.execute("drop table topktweets")
    s.execute("drop table tweetsTable")

    conn.close()
  }

  ignore("SNAP-463 topk tables data with SQL. Case 3 with time interval = batch interval") {

    // start network server
    val serverHostPort = TestUtil.startNetServer()
    verbosePrint(" Network server started.")

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val s = conn.createStatement()
    s.execute("streaming init 2secs")
    s.execute("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '**REMOVED**', " +
        "consumerSecret '**REMOVED**', " +
        "accessToken '**REMOVED**', " +
        "accessTokenSecret '**REMOVED**', " +
        "rowConverter 'io.snappydata.app.streaming.TweetToRowsConverter')")
    snsc.sql("create table fullTweetsTable (id long, text string, " +
        "fullName string, country string, retweets int, " +
        "hashtag string) using column")
    s.execute("create topk table topkTweets on tweetsTable options(" +
        "timeInterval '2000ms'," +
        "key 'hashtag', size '10')")

    val tweetsStream = SnappyStreamingContext.getInstance().get
        .getSchemaDStream("tweetsTable")
    tweetsStream.foreachDataFrame { df =>
      df.write.insertInto("fullTweetsTable")
    }

    val snc = SnappyContext()

    s.execute("streaming start")

    for (a <- 1 to 5) {

      Thread.sleep(2000)

      s.execute("select hashtag, count(hashtag) as cht from fullTweetsTable " +
          "group by hashtag order by cht desc limit 10")
      verbosePrint("\n\n-----  TOP Tweets  -----\n")
      var hasTags = false
      var rs = s.getResultSet
      if (a == 5) assert(rs.next)
      while (rs.next()) {
        val count = rs.getInt(2)
        if (count > 1) {
          hasTags = true
        }
        verbosePrint(s"${rs.getString(1)} ; $count")
      }

      s.execute("select * from topkTweets order by EstimatedValue desc")
      val topKdf = snc.queryApproxTSTopK("topkTweets", null, null)
      val topKRes = topKdf.collect()
      verbosePrint("\n\n-----  TOPK Tweets  -----\n")
      rs = s.getResultSet
      var numResults = 0
      while (rs.next()) {
        val count = rs.getInt(2)
        if (hasTags) {
          assert(count > 1)
          hasTags = false
        } else {
          assert(count > 0)
        }
        verbosePrint(s"${rs.getString(1)} ; ${rs.getLong(2)} ; ${rs.getString(3)}")
        numResults += 1
      }
      verbosePrint(s"Num results=$numResults")
      assert(numResults <= 10)

      verbosePrint("\n\n-----  TOPK Tweets2  -----\n")
      topKRes.foreach(verbosePrint)
      numResults = topKRes.length
      verbosePrint(s"Num results=$numResults")
      assert(numResults <= 10)

      s.execute("select text, fullName from fullTweetsTable " +
          "where text like '%e%'")
      rs = s.getResultSet
      if (a == 5) assert(rs.next)
      while (rs.next()) {
        rs.getString(1)
        rs.getString(2)
      }
    }
    s.execute("streaming stop")
    s.execute("drop table fullTweetsTable")
    s.execute("drop table topktweets")
    s.execute("drop table tweetsTable")

    conn.close()
  }

  ignore("SNAP-463 topk tables data with SQL. - without time interval") {

    // start network server
    val serverHostPort = TestUtil.startNetServer()
    verbosePrint(" Network server started.")

    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val s = conn.createStatement()
    s.execute("streaming init 2secs")
    s.execute("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '**REMOVED**', " +
        "consumerSecret '**REMOVED**', " +
        "accessToken '**REMOVED**', " +
        "accessTokenSecret '**REMOVED**', " +
        "rowConverter 'io.snappydata.app.streaming.TweetToRowsConverter')")
    snsc.sql("create table fullTweetsTable (id long, text string, " +
        "fullName string, country string, retweets int, " +
        "hashtag string) using column")
    s.execute("create topk table topkTweets on tweetsTable options(" +
        "key 'hashtag', size '10')")

    val tweetsStream = SnappyStreamingContext.getInstance().get
        .getSchemaDStream("tweetsTable")
    tweetsStream.foreachDataFrame { df =>
      df.write.insertInto("fullTweetsTable")
    }

    val snc = SnappyContext()

    s.execute("streaming start")

    for (a <- 1 to 5) {

      Thread.sleep(2000)

      s.execute("select hashtag, count(hashtag) as cht from fullTweetsTable " +
          "group by hashtag order by cht desc limit 10")
      verbosePrint("\n\n-----  TOP Tweets  -----\n")
      var hasTags = false
      var rs = s.getResultSet
      if (a == 5) assert(rs.next)
      while (rs.next()) {
        val count = rs.getInt(2)
        if (count > 1) {
          hasTags = true
        }
        verbosePrint(s"${rs.getString(1)} ; $count")
      }

      s.execute("select * from topkTweets order by EstimatedValue desc")
      val topKdf = snc.queryApproxTSTopK("topkTweets", null, null)
      val topKRes = topKdf.collect()
      verbosePrint("\n\n-----  TOPK Tweets  -----\n")
      rs = s.getResultSet
      var numResults = 0
      while (rs.next()) {
        val count = rs.getInt(2)
        if (hasTags) {
          assert(count > 1)
          hasTags = false
        } else {
          assert(count > 0)
        }
        verbosePrint(s"${rs.getString(1)} ; ${rs.getLong(2)} ; ${rs.getString(3)}")
        numResults += 1
      }
      verbosePrint(s"Num results=$numResults")
      assert(numResults <= 10)

      verbosePrint("\n\n-----  TOPK Tweets2  -----\n")
      topKRes.foreach(verbosePrint)
      numResults = topKRes.length
      verbosePrint(s"Num results=$numResults")
      assert(numResults <= 10)

      s.execute("select text, fullName from fullTweetsTable " +
          "where text like '%e%'")
      rs = s.getResultSet
      if (a == 5) assert(rs.next)
      while (rs.next()) {
        rs.getString(1)
        rs.getString(2)
      }
    }
    s.execute("streaming stop")
    s.execute("drop table fullTweetsTable")
    // don't allow dropping parent table first
    try {
      s.execute("drop table tweetsTable")
      fail("expected failure in dropping stream table with existing topK")
    } catch {
      case sqle: SQLException if (sqle.getSQLState == "42000") &&
          sqle.getMessage.contains("tweetstable cannot be dropped " +
              "because of dependent objects") => // expected
    }
    s.execute("drop table topktweets")
    s.execute("drop table tweetsTable")

    conn.close()
  }

  def testStreamingAdhocSQL(): Unit = {
    val serverHostPort = TestUtil.startNetServer()
    verbosePrint(" Network server started.")
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val s = conn.createStatement()
    try {
      s.execute("streaming init 2secs")
      s.execute("create stream table tweetsTable " +
          "(id long, text string, fullName string, " +
          "country string, retweets int, hashtag string) " +
          "using twitter_stream options (" +
          "consumerKey '**REMOVED**', " +
          "consumerSecret '**REMOVED**', " +
          "accessToken '**REMOVED**', " +
          "accessTokenSecret '**REMOVED**', " +
          "rowConverter 'io.snappydata.dunit.streaming.TweetToRowsConverter')")
      snsc.sql("create table fullTweetsTable (id long, text string, " +
          "fullName string, country string, retweets int, " +
          "hashtag string) using column")
      s.execute("create topk table topkTweets on tweetsTable options(" +
          s"key 'hashtag', " +
          "timeInterval '6000ms', size '10')")

      val tweetsStream = SnappyStreamingContext.getInstance().get
          .getSchemaDStream("tweetsTable")
      tweetsStream.foreachDataFrame { df =>
        df.write.insertInto("fullTweetsTable")
      }

      val snc = SnappyContext()

      s.execute("streaming start")

      for (a <- 1 to 5) {

        Thread.sleep(2000)

        s.execute("select hashtag, count(hashtag) as cht from fullTweetsTable " +
            "group by hashtag order by cht desc limit 10")
        verbosePrint("\n\n-----  TOP Tweets  -----\n")
        var rs = s.getResultSet
        if (a == 5) assert(rs.next)
        while (rs.next()) {
          verbosePrint(s"${rs.getString(1)} ; ${rs.getInt(2)}")
        }

        s.execute("select * from topkTweets order by EstimatedValue desc")
        verbosePrint("\n\n-----  TOPK Tweets  -----\n")
        rs = s.getResultSet
        var numResults = 0
        while (rs.next()) {
          verbosePrint(s"${rs.getString(1)} ; ${rs.getLong(2)} ; ${rs.getString(3)}")
          numResults += 1
        }
        verbosePrint(s"Num results=$numResults")
        assert(numResults <= 10)

        val topKdf = snc.queryApproxTSTopK("topkTweets", -1, -1)
        verbosePrint("\n\n-----  TOPK Tweets2  -----\n")
        val topKRes = topKdf.collect()
        topKRes.foreach(verbosePrint)
        numResults = topKRes.length
        verbosePrint(s"Num results=$numResults")
        assert(numResults <= 10)

        s.execute("select text, fullName from fullTweetsTable " +
            "where text like '%e%'")
        rs = s.getResultSet
        if (a == 5) assert(rs.next)
        while (rs.next()) {
          rs.getString(1)
          rs.getString(2)
        }
      }
    } finally {
      s.execute("streaming stop")
      s.execute("drop table if exists fullTweetsTable")
      s.execute("drop table if exists topktweets")
      s.execute("drop table if exists tweetsTable")
      conn.close()
    }
  }

  def testStreamingAdhocSQL_NPEBug(): Unit = {
    val serverHostPort = TestUtil.startNetServer()
    verbosePrint(" Network server started.")
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val s = conn.createStatement()
    s.execute("streaming init 2secs")
    s.execute("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '**REMOVED**', " +
        "consumerSecret '**REMOVED**', " +
        "accessToken '**REMOVED**', " +
        "accessTokenSecret '**REMOVED**', " +
        "rowConverter 'io.snappydata.dunit.streaming.TweetToRowsConverter')")
    s.execute("create topk table topkTweets on tweetsTable options(" +
        s"key 'hashtag', epoch '${System.currentTimeMillis()}', " +
        "timeInterval '30000ms', size '10')")

    s.execute("streaming start")

    for (a <- 1 to 5) {

      Thread.sleep(2000)

      s.execute("select hashtag, count(hashtag) as cht from tweetsTable " +
          "group by hashtag order by cht desc limit 10")
      verbosePrint("\n\n-----  TOP Tweets  -----\n")
      var rs = s.getResultSet
      if (a == 5) assert(rs.next)
      while (rs.next()) {
        verbosePrint(s"${rs.getString(1)} ; ${rs.getInt(2)}")
      }

      s.execute("select * from topkTweets order by EstimatedValue desc")
      verbosePrint("\n\n-----  TOPK Tweets  -----\n")
      rs = s.getResultSet
      var numResults = 0
      while (rs.next()) {
        verbosePrint(s"${rs.getString(1)} ; ${rs.getLong(2)} ; ${rs.getString(3)}")
        numResults += 1
      }
      verbosePrint(s"Num results=$numResults")
      assert(numResults <= 10)

      s.execute("select text, fullName from tweetsTable where text like '%e%'")
      rs = s.getResultSet
      if (a == 5) assert(rs.next)
      while (rs.next()) {
        rs.getString(1)
        rs.getString(2)
      }
    }
    s.execute("streaming stop")
    s.execute("drop table tweetsTable")
    s.execute("drop table topktweets")

    conn.close()
  }

  /** To test SNAP-789: Taken from Snappy-Poc
    * TODO: Following steps to enable this test,
    * need to run this in IdeaJ only.
    * 1. Rename ignore as test from its name
    * 2. In IdeaJ module dependency, add full path to
    * ./assembly/build/libs/AdImpressionLogAggr-0.3-assembly.jar
    * Above Jar (or its newer version) is can be build using Snappy-POC
    * 4. From Snappy POc, run this:
    * ./gradlew generateAdImpressions
   */
  val kafkaTopic = "adImpressionsTopic"
  val brokerList = "localhost:9092"
  ignore("SNAP-789") {
    try {
      val serverHostPort = TestUtil.startNetServer()
      verbosePrint(" Network server started.")
      val conn = DriverManager.getConnection(
        "jdbc:snappydata://" + serverHostPort)
      val s = conn.createStatement()
      snsc.sql("set spark.sql.shuffle.partitions=8")

      snsc.sql("drop table if exists adImpressionStream")
      snsc.sql("drop table if exists sampledAdImpressions")
      snsc.sql("drop table if exists sampledAdImpressions2")
      snsc.sql("drop table if exists sampledAdImpressions3")
      snsc.sql("drop table if exists sampledAdImpressions4")
      snsc.sql("drop table if exists aggrAdImpressions")

      snsc.sql("create stream table adImpressionStream (" +
          " time_stamp timestamp," +
          " publisher string," +
          " advertiser string," +
          " website string," +
          " geo string," +
          " bid double," +
          " cookie string) " +
          " using directkafka_stream options(" +
          " rowConverter 'io.snappydata.adanalytics.aggregator.AdImpressionToRowsConverter' ," +
          s" kafkaParams 'metadata.broker.list->$brokerList'," +
          s" topics '$kafkaTopic'," +
          " K 'java.lang.String'," +
          " V 'io.snappydata.adanalytics.aggregator.AdImpressionLog', " +
          " KD 'kafka.serializer.StringDecoder', " +
          " VD 'io.snappydata.adanalytics.aggregator.AdImpressionLogAvroDecoder')")

      // Next, create the Column table where we ingest all our data into.
      snsc.sql("create table aggrAdImpressions(time_stamp timestamp, publisher string," +
          " geo string, avg_bid double, imps long, uniques long) using column")

      snsc.sql("CREATE SAMPLE TABLE sampledAdImpressions" +
          " OPTIONS(qcs 'geo', fraction '0.03'," +
          " strataReservoirSize '50'," +
          " baseTable 'aggrAdImpressions')")

      snsc.sql("CREATE SAMPLE TABLE sampledAdImpressions2" +
          " ON aggrAdImpressions OPTIONS(qcs 'geo', fraction '0.03'," +
          " strataReservoirSize '50')")

      //      snsc.sql("CREATE SAMPLE TABLE sampledAdImpressions3" +
      //          " OPTIONS(qcs 'geo', fraction '0.03'," +
      //          " strataReservoirSize '50'," +
      //          " baseTable 'aggrAdImpressions')")
      //
      //      snsc.sql("CREATE SAMPLE TABLE sampledAdImpressions4" +
      //          " OPTIONS(qcs 'geo', fraction '0.03'," +
      //          " strataReservoirSize '50'," +
      //          " baseTable 'aggrAdImpressions')")

      // Execute this query once every second. Output is a SchemaDStream.
      val resultStream : SchemaDStream = snsc.registerCQ(
        "select time_stamp, publisher, geo, avg(bid) as avg_bid," +
            " count(*) as imps , count(distinct(cookie)) as uniques" +
            " from adImpressionStream window (duration 1 seconds, slide 1 seconds)" +
            " where geo != 'unknown' group by publisher, geo, time_stamp")

      resultStream.foreachDataFrame( df => {
        df.write.insertInto("aggrAdImpressions")
        df.write.insertInto("sampledAdImpressions")
        df.write.insertInto("sampledAdImpressions2")
        //        df.write.insertInto("sampledAdImpressions3")
        //        df.write.insertInto("sampledAdImpressions4")
      })

      val rand = scala.util.Random.nextInt(100)
      snsc.start()
      Thread.sleep(5000)
      tcRunQuery(s, rand)
      //      snsc.sql("STREAMING STOP")
      //      runQuery(this.snc)
      //      snsc.sql("STREAMING INIT 2 secs")
      //      snsc.sql("STREAMING START")
      Thread.sleep(10000)
      snsc.sql("STREAMING STOP")
      runQuery(this.snc, rand)
      tcRunCountQuery(s, rand)

      snsc.awaitTermination()
    } finally {
      snsc.sql("STREAMING STOP")
      snsc.sql("drop table if exists adImpressionStream")
      snsc.sql("drop table if exists sampledAdImpressions")
      snsc.sql("drop table if exists sampledAdImpressions2")
      snsc.sql("drop table if exists sampledAdImpressions3")
      snsc.sql("drop table if exists sampledAdImpressions4")
      snsc.sql("drop table if exists aggrAdImpressions")
    }
  }

  private def tcRunQuery(s : Statement, rand : Int): Any = {
    tcPrintColTable(s, "aggrAdImpressions", rand)
    tcPrintSampledTable(s, "sampledAdImpressions", rand)
  }

  private def tcRunCountQuery(s : Statement, rand : Int): Any = {
    tcPrintCountOfSampledTable(s, "sampledAdImpressions", rand)
    tcPrintCountOfSampledTable(s, "sampledAdImpressions2", rand)
  }

  private def runQuery(snc: SnappyContext, rand : Int): Any = {
    snc.sql("set spark.sql.shuffle.partitions=6")
    printColTable(snc, "aggrAdImpressions", rand)
    printSampledTable(snc, "sampledAdImpressions", rand)
    printSampledTable(snc, "sampledAdImpressions2", rand)
    //    printSampledTable(snc, "sampledAdImpressions3", rand)
    //    printSampledTable(snc, "sampledAdImpressions4", rand)
  }

  private def printSampledTable(snc: SnappyContext, tabName: String, rand : Integer): Unit = {
    def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
    val fileName = s"$tabName.$rand.out"
    val pw = new PrintWriter(fileName)

    Try {
      // TODO: Get error
//      printQueryResult(snc, tabName, pw,
//        s"select sum(imps), count(imps), avg(imps), count(imps)  as sample_cnt " +
//            s" from $tabName where imps < 5000 with error .5")
      printQueryResult(snc, tabName, pw,
        s"select sum(imps), count(imps), avg(imps), count(imps)  as sample_cnt " +
            s" from $tabName where imps < 5000")
      printQueryResult(snc, tabName, pw,
        s"select geo, count(imps) as sample_cnt, count(imps) as count" +
            s" from $tabName group by geo")
      printQueryResult(snc, tabName, pw,
        s"select SNAPPY_SAMPLER_WEIGHTAGE, count(imps) as sample_cnt " +
            s" from $tabName where geo='NY' group by SNAPPY_SAMPLER_WEIGHTAGE")
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/$fileName"
      case Failure(e) => pw.close()
        throw e;
    }
  }

  private def tcPrintSampledTable(s: Statement, tabName: String, rand : Integer): Unit = {
    def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
    val fileName = s"$tabName.$rand.tc.out"
    val pw = new PrintWriter(fileName)

    Try {
      tcPrintQueryResult(s, tabName, pw,
        s"select sum(imps), count(imps), avg(imps), count(imps)  as sample_cnt " +
            s" from $tabName where imps < 5000", true)
      // TODO: Getting error
//      tcPrintQueryResult(s, tabName, pw,
//        s"select sum(imps), count(imps), avg(imps), count(imps)  as sample_cnt " +
//            s" from $tabName where imps < 5000 with error .5", true)
//      tcPrintQueryResult(s, tabName, pw,
//        s"select sum(*), count(*), avg(*), count(*)  as sample_cnt " +
//            s" from $tabName where imps < 5000 with error .5", true)
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/$fileName"
      case Failure(e) => pw.close()
        throw e;
    }
  }

  private def tcPrintCountOfSampledTable(s: Statement, tabName: String, rand : Integer): Unit = {
    def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
    val fileName = s"$tabName.$rand.count.out"
    val pw = new PrintWriter(fileName)

    Try {
      tcPrintCountQueryResult(s, tabName, pw,
        s"select geo, SNAPPY_SAMPLER_WEIGHTAGE, count(imps) as sample_cnt" +
            s" from $tabName group by geo, SNAPPY_SAMPLER_WEIGHTAGE")
      tcPrintCountQueryResult(s, tabName, pw,
        s"select geo, SNAPPY_SAMPLER_WEIGHTAGE, count(imps) as sample_cnt" +
            s" from $tabName where geo='NY' group by geo, SNAPPY_SAMPLER_WEIGHTAGE")
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/$fileName"
      case Failure(e) => pw.close()
        throw e;
    }
  }

  private def printColTable(snc: SnappyContext, tabName: String, rand : Integer): Unit = {
    def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
    val fileName = s"$tabName.$rand.out"
    val pw = new PrintWriter(fileName)

    Try {
      printQueryResult(snc, tabName, pw,
        s"select sum(imps), count(imps), avg(imps) from $tabName where imps < 5000")
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/$fileName"
      case Failure(e) => pw.close()
        throw e
    }
  }

  private def tcPrintColTable(s: Statement, tabName: String, rand : Integer): Unit = {
    def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
    val fileName = s"$tabName.$rand.tc.out"
    val pw = new PrintWriter(fileName)

    Try {
      tcPrintQueryResult(s, tabName, pw,
        s"select sum(imps), count(imps), avg(imps) from $tabName where imps < 5000", false)

    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/$fileName"
      case Failure(e) => pw.close()
        throw e
    }
  }

  private def printQueryResult(snc: SnappyContext, tabName: String,
      pw : PrintWriter, query: String): Unit = {
    val actualResult = snc.sql(query)
    val start = System.currentTimeMillis
    val result = actualResult.collect()
    val totalTime = System.currentTimeMillis - start
    // scalastyle:off println
    pw.println(s"Query Execution on $tabName " +
        s"table took ${totalTime}ms : $query")
    result.foreach(rs => {
      pw.println(rs.toString)
    })
    // scalastyle:on println
  }

  private def tcPrintCountQueryResult(s: Statement, tabName: String,
      pw : PrintWriter, query: String): Unit = {
    val actualResult = snc.sql(query)
    val start = System.currentTimeMillis
    s.execute(query)
    val rs = s.getResultSet
    val totalTime = System.currentTimeMillis - start
    // scalastyle:off println
    pw.println(s"Count of $tabName " +
        s"table took ${totalTime}ms : $query")
    // scalastyle:on println
    var numGroups = 0
    var rowsSeen : Long = 0
    var sumOfRtWt : Long = 0
    var conditionalCount : Long = 0
    while (rs.next()) {
      val weight = rs.getLong(2)
      val rowcount = rs.getLong(3)
      val rWt = (weight >> 8) & 0xffffffffL
      val lWt = (weight >> 40) & 0xffffffffL
      if (lWt > 0) {
        conditionalCount = conditionalCount + (rowcount * rWt * 100 / lWt)
        sumOfRtWt = sumOfRtWt + rWt
        rowsSeen = rowsSeen + rowcount
      }
      else {
        // scalastyle:off println
        pw.println(s"Attention lWt=$rWt")
        // scalastyle:on println
      }
      if (rWt < 1) {
        // scalastyle:off println
        pw.println(s"Attention rWt=$rWt")
        // scalastyle:on println
      }
      numGroups += 1
    }

    // scalastyle:off println
    pw.println(s"conditionalCount=%.2f".format(conditionalCount / 100.0) +
        s"; rowsSeen=$rowsSeen" + s"; numGroups=$numGroups" + s"; sumOfRtWt=$sumOfRtWt")
    // scalastyle:on println
  }

  private def tcPrintQueryResult(s: Statement, tabName: String,
      pw : PrintWriter, query: String, isSample : Boolean): Unit = {
    val actualResult = snc.sql(query)
    val start = System.currentTimeMillis
    s.execute(query)
    val rs = s.getResultSet
    val totalTime = System.currentTimeMillis - start
    // scalastyle:off println
    pw.println(s"TC: Query Execution on $tabName " +
        s"table took ${totalTime}ms : $query")
    var numResults = 0
    while (rs.next()) {
      if (isSample) {
        pw.println(s"${rs.getLong(1)} ; ${rs.getLong(2)} ; ${rs.getDouble(3)}; ${rs.getLong(4)}")
      }
      else {
        pw.println(s"${rs.getLong(1)} ; ${rs.getLong(2)} ; ${rs.getDouble(3)}")
      }
      numResults += 1
    }
    // scalastyle:on println
  }
}

case class Tweet(id: Int, text: String)

class TweetToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    val status: Status = message.asInstanceOf[Status]
    val retweetCount = status.getRetweetCount
    val r = new Random(13)

    val hashTags = new StringBuilder()
    for (tag <- status.getHashtagEntities) {
      if (hashTags.nonEmpty) {
        hashTags.append(',').append(tag.getText)
      } else {
        hashTags.append(tag.getText)
      }
    }
    Seq(Row.fromSeq(Seq(status.getId,
      status.getText,
      status.getUser.getName,
      status.getUser.getLang,
      if (retweetCount == 0) {
        r.nextInt(13)
      } else {
        retweetCount
      }, hashTags.toString())))
  }
}
