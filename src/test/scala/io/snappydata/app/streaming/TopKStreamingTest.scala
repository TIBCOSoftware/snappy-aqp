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

import java.io.{BufferedWriter, IOException, OutputStreamWriter}
import java.net.ServerSocket

import scala.collection.mutable.ArrayBuffer

import io.snappydata.SnappyFunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Duration, SnappyStreamingContext}

class TopKStreamingTest extends SnappyFunSuite
    with Eventually with BeforeAndAfterAll with BeforeAndAfter {

  var snpystr: SnappyStreamingContext = _

  def init(durationStreaming: Duration): Unit = {
    snpystr = new SnappyStreamingContext(sc, durationStreaming)
  }

  after {
    Server.stop()
  }

  before {

    if (snc != null) {
      snc.clearCache()
    }

    if (snpystr != null) {
      snpystr.stop(stopSparkContext = true, stopGracefully = true)
      // snpystr.stop()
    }

    // SnappyContext.stop
  }

  override def afterAll(): Unit = {
    baseCleanup()
    snpystr.stop()
  }

  test("test basic streaming populating topK structure") {
    val port = 13133
    try {
      this.init(Duration(500))
      val dstreamText = snpystr.socketTextStream("localhost", port)
      val charDStream = dstreamText.flatMap[Char](str => str.toCharArray.map(
        ch => if (ch == 'a' || ch == 'A' || ch == 'e' || ch == 'E' ||
            ch == 'i' || ch == 'I' || ch == 'o' || ch == 'O' ||
            ch == 'u' || ch == 'U') {
          ch
        } else {
          'c'
        }))
      val topKOption = Map(

        "epoch" -> System.currentTimeMillis().toString,
        "timeInterval" -> "1000ms",
        "size" -> "4"
      )
      val schema = StructType(List(StructField("CharType", StringType)))
      snc.createApproxTSTopK("charTopK", None, "CharType",
        schema, topKOption)
      snc.saveStream[Char](charDStream,
        Seq("charTopK"),
        Some((rdd: RDD[Char]) => {
          rdd.map(x => Row(x.toString))
        }))
      Server.startServer(port)
      Thread.sleep(2000)

      snpystr.start()

      // Iterate over the streaming data for sometime and publish
      // the results to a file.

      val end = System.currentTimeMillis + 10000
      var foundNonZero = false
      var prevNonZero: Long = 0
      var numUniqueNonZero: Int = 0
      while (end > System.currentTimeMillis()) {
        Thread.sleep(1000)
        logInfo("Top 10 hash tags using sketches for the last interval")

        val df = snc.queryApproxTSTopK("charTopK", -1, System.currentTimeMillis)
        logInfo(df.collect().mkString("\n"))
        val rows = df.collect()
        if (rows.length != 0) {
          val temp = rows(0).getLong(1)
          if (temp > 0) {
            foundNonZero = true
            if (temp > prevNonZero) {
              numUniqueNonZero += 1
              prevNonZero = temp
            }
          }
        }
        logInfo("Top 4 hash tags until now using sketches ")
      }
      assert(foundNonZero)
      assert(numUniqueNonZero > 2)

    } finally {
      snc.dropTable("charTopK", ifExists = true)
    }
  }

  // Find out why this stack over flow is coming. there is no clear cause yet
  test("test  streaming populating topK structure for Stack OverFlow." +
      "Data may or may not be there.") {
    val port = 13134
    try {
      this.init(Duration(100))
      val dstreamText = snpystr.socketTextStream("localhost", port)
      val charDStream = dstreamText.flatMap[Char](str => str.toCharArray.map(
        ch => if (ch == 'a' || ch == 'A' || ch == 'e' || ch == 'E' ||
            ch == 'i' || ch == 'I' || ch == 'o' || ch == 'O' ||
            ch == 'u' || ch == 'U') {
          ch
        } else {
          'c'
        }))

      val topKOption = Map(
        "timeInterval" -> "1000ms",
        "size" -> "4",
        "epoch" -> (System.currentTimeMillis() - 30000).toString
      )
      val schema = StructType(List(StructField("CharType", StringType)))
      snc.createApproxTSTopK("charTopK", None, "CharType",
        schema, topKOption)
      snc.saveStream[Char](charDStream,
        Seq("charTopK"),
        Some((rdd: RDD[Char]) => {
          rdd.map(x => Row(x.toString))
        }))
      Server.startServer(port)
      Thread.sleep(1000)

      snpystr.start()

      // Iterate over the streaming data for sometime and publish
      // the results to a file.

      val end = System.currentTimeMillis + 10000
      var foundNonZero = false
      var prevNonZero: Long = 0
      var numUniqueNonZero: Int = 0
      while (end > System.currentTimeMillis()) {
        Thread.sleep(1000)
        logInfo(
          "Top 10 hash tags using sketches for the last interval")
        val df = snc.queryApproxTSTopK("charTopK",
          System.currentTimeMillis - 12000000, System.currentTimeMillis)
        logInfo(df.collect().mkString("\n"))
        val rows = df.collect()
        if (rows.length != 0) {
          val temp = rows(0).getLong(1)
          if (temp > 0) {
            foundNonZero = true
            if (temp > prevNonZero) {
              numUniqueNonZero += 1
              prevNonZero = temp
            }
          }
        }
        logInfo("Top 4 hash tags until now using sketches ")
      }
      // assert(foundNonZero)
      // assert(numUniqueNonZero > 2)
    } finally {
      snc.dropTable("charTopK", ifExists = true)
    }
  }

  test("test  streaming populating topK structure for Stack OverFlow." +
      "Data may or may not be there. First populate then query  - case") {
    val port = 13134
    try {
      this.init(Duration(100))
      val dstreamText = snpystr.socketTextStream("localhost", port)
      val charDStream = dstreamText.flatMap[Char](str => str.toCharArray.map(
        ch => if (ch == 'a' || ch == 'A' || ch == 'e' || ch == 'E' ||
            ch == 'i' || ch == 'I' || ch == 'o' || ch == 'O' ||
            ch == 'u' || ch == 'U') {
          ch
        } else {
          'c'
        }))

      val topKOption = Map(
        "timeInterval" -> "1000ms",
        "size" -> "4",
        "epoch" -> (System.currentTimeMillis() - 30000).toString
      )
      val schema = StructType(List(StructField("CharType", StringType)))
      snc.createApproxTSTopK("charTopK", None, "CharType",
        schema, topKOption)
      snc.saveStream[Char](charDStream,
        Seq("charTopK"),
        Some((rdd: RDD[Char]) => {
          rdd.map(x => Row(x.toString))
        }))
      Server.startServer(port)
      Thread.sleep(1000)

      snpystr.start()

      // Let it populate the data for a while
      Thread.sleep(10000)
      Server.stop()
      // now query the data for some time.
      val end = System.currentTimeMillis + 10000
      var foundNonZero = false
      var prevNonZero: Long = 0
      var numUniqueNonZero: Int = 0
      while (end > System.currentTimeMillis()) {
        Thread.sleep(1000)
        logInfo("Top 10 hash tags using sketches for the last interval")
        val df = snc.queryApproxTSTopK("charTopK",
          System.currentTimeMillis - 12000000, System.currentTimeMillis)
        logInfo(df.collect().mkString("\n"))
        val rows = df.collect()
        if (rows.length != 0) {
          val temp = rows(0).getLong(1)
          if (temp > 0) {
            foundNonZero = true
            if (temp > prevNonZero) {
              numUniqueNonZero += 1
              prevNonZero = temp
            }
          }
        }
        logInfo("Top 4 hash tags until now using sketches ")
      }
      // assert(foundNonZero)
      // assert(numUniqueNonZero > 2)
    } finally {
      snc.dropTable("charTopK", ifExists = true)
    }
  }
}

object Server {

  var listener: Thread = _
  var keepGoing: Boolean = true
  var ss: ServerSocket = _
  val toPrint = Array(
    "When case classes cannot be defined ahead of time (for example, the structure of ",
    "records is encoded in a string, or a text dataset will be parsed ",
    "and fields will be projected differently ",
    "for different users), a DataFrame can be created programmatically with three steps.",
    "Create an RDD of Rows from the original RDD;\n",
    "Create the schema represented by a StructType matching ",
    "the structure of Rows in the RDD created in Step 1.\n",
    "Apply the schema to the RDD of Rows via createDataFrame method provided by SQLContext"
  )

  def startServer(port: Int) {
    keepGoing = true
    listener = new Thread(
      new Runnable() {
        def run() {
          val threadsStarted: ArrayBuffer[Thread] = ArrayBuffer[Thread]()
          ss = new ServerSocket(port)
          var numConnection = 0
          while (keepGoing) {
            try {
              val socket = ss.accept()
              numConnection += 1
              val th = new Thread() {
                override def run(): Unit = {
                  val num = numConnection
                  val bos = new BufferedWriter(
                    new OutputStreamWriter(socket.getOutputStream))
                  while (keepGoing) {
                    bos.write("This is the output of thread =" + num)
                    bos.newLine()
                    toPrint.foreach(
                      str => {
                        bos.write(str)
                        bos.newLine()
                      }
                    )
                    bos.flush()
                    Thread.sleep(1000)
                  }

                }
              }
              th.start()
              th +=: threadsStarted
            } catch {
              case _: IOException if !keepGoing => // OK
            }
          }
          threadsStarted.foreach(_.join())
          threadsStarted.clear()
        }
      })
    listener.start()
  }

  def stop(): Unit = {
    keepGoing = false
    if (listener != null) {
      listener.interrupt()
      try {
        listener.join(2000)
      } catch {
        case _: InterruptedException => // ignore
      }
    }
    try {
      if (ss != null) {
        ss.close()
      }
    } catch {
      case _: Exception => // ok
    }
  }
}
