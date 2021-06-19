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
package org.apache.spark.sql.execution.streamsummary

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import io.snappydata.util.com.clearspring.analytics.stream.{Counter, StreamSummary}

import org.apache.spark.sql.TimeEpoch
import org.apache.spark.sql.execution.{Approximate, KeyFrequencyWithTimestamp, TopK}

/**
 * TopK using stream summary algorithm.
 *
 */
class StreamSummaryAggregation[T](val capacity: Int, val intervalSize: Long,
  val epoch0: Long, val maxIntervals: Int,
  val streamAggregates: mutable.MutableList[StreamSummary[T]], initInterval: Long) extends TopK {

  def this(capacity: Int, intervalSize: Long,
    epoch0: Long, maxIntervals: Int) = this(capacity, intervalSize,
    epoch0, maxIntervals, new mutable.MutableList[StreamSummary[T]](), 0)

  val timeEpoch = new TimeEpoch(intervalSize, epoch0, initInterval)

  if (streamAggregates.length == 0) {
    for (x <- 1 to maxIntervals) {
      streamAggregates += new StreamSummary[T](capacity)
      timeEpoch.increment()
    }
  }

  def addItems(data: ArrayBuffer[KeyFrequencyWithTimestamp[T]]): Unit = {
    data.foreach(item => {
      val epoch = item.epoch
      if (epoch > 0) {
        // find the appropriate time and item aggregates and update them
        timeEpoch.timestampToInterval(epoch) match {
          case Some(interval) =>
            if (interval <= maxIntervals) {
              streamAggregates(interval - 1).offer(item.key,
                item.frequency.toInt)
            } // else IGNORE
          case None => // IGNORE
        }
      } else {
        throw new UnsupportedOperationException("Support for intervals " +
          "for wall clock time for streams not available yet.")
      }
    })

  }

  def queryIntervals(start: Int, end: Int, k: Int): Map[T, Approximate] = {
    val topkFromAllIntervals = new mutable.MutableList[Counter[T]]

    val lastInterval = if (end > maxIntervals) maxIntervals else end

    for (x <- start to lastInterval) {
      streamAggregates(x).topK(k).asScala.foreach(topkFromAllIntervals += _)
    }
    topkFromAllIntervals groupBy (_.getItem) map { x =>
      (x._1, new Approximate(x._2.foldLeft(0L)(_ + _.getError),
        x._2.foldLeft(0L)(_ + _.getCount), 0, 0))
    }
  }

  def getTopKBetweenTime(epochFrom: Long, epochTo: Long,
    k: Int): Option[Array[(T, Approximate)]] =
    {
      val (end, start) = this.timeEpoch.convertEpochToIntervals(epochFrom,
        epochTo) match {
          case Some(x) => x
          case None => return None
        }
      Some(this.queryIntervals(start - 1, end - 1, k).toArray)
    }

}

object StreamSummaryAggregation {

  def create[T: ClassTag](size: Int,
    timeInterval: Long, epoch0: () => Long, maxInterval: Int): TopK =
    new StreamSummaryAggregation[T](size, timeInterval,
      epoch0(), maxInterval)

  def write(kryo: Kryo, output: Output, obj: StreamSummaryAggregation[_]) {
    output.writeInt(obj.capacity)
    output.writeLong(obj.intervalSize)
    output.writeLong(obj.epoch0)
    output.writeInt(obj.maxIntervals)
    output.writeInt(obj.streamAggregates.length)
    obj.streamAggregates.foreach { x => StreamSummary.write(kryo, output, x) }
    output.writeLong(obj.timeEpoch.t)
  }

  def read(kryo: Kryo, input: Input): StreamSummaryAggregation[_] = {

    val capacity = input.readInt
    val intervalSize = input.readLong
    val epoch0 = input.readLong
    val maxIntervals = input.readInt
    val len = input.readInt
    val streamAggregates = mutable.MutableList.fill[StreamSummary[Any]](len)(
      StreamSummary.read[Any](kryo, input))
    val t = input.readLong
    new StreamSummaryAggregation[Any](capacity, intervalSize, epoch0: Long, maxIntervals,
      streamAggregates, t)

  }
}
