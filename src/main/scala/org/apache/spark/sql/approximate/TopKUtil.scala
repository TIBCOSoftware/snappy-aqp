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
package org.apache.spark.sql.approximate

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation
import org.apache.spark.sql.hive.SnappyAQPSessionCatalog
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SnappyContext, SnappySession}
import org.apache.spark.{Partition, Partitioner, SparkContext, TaskContext}

object TopKUtil {

  val START_TIME_COLUMN = "StartTime"
  val END_TIME_COLUMN = "EndTime"

  def col(context: SnappyContext, name: String): String =
    context.sessionState.catalog.formatTableName(name)

  def getSchema(context: SnappyContext, key: StructField,
      isStreamSummary: Boolean): StructType = {
    val aggColumn = StructField(col(context, "EstimatedValue"), LongType)
    val startTime = StructField(col(context, START_TIME_COLUMN), StringType)
    val endTime = StructField(col(context, END_TIME_COLUMN), StringType)
    if (isStreamSummary) {
      val errorBounds = col(context, "DeltaError")
      StructType(Array(key, aggColumn,
        StructField(errorBounds, LongType),
        startTime, endTime))
    } else {
      val errorBounds = col(context, "ErrorBoundsInfo")
      StructType(Array(key, aggColumn,
        StructField(errorBounds, ApproximateType),
        startTime, endTime))
    }
  }

  def createTopKRDD(name: String, context: SparkContext): RDD[(Int, TopK)] = {
    val partCount = Utils.getAllExecutorsMemoryStatus(context).
        keySet.size * Runtime.getRuntime.availableProcessors() * 2
    Utils.getFixedPartitionRDD[(Int, TopK)](context,
      (_: TaskContext, part: Partition) => {
        scala.collection.Iterator(part.index -> TopKHokusai.createDummy)
      }, new Partitioner() {
        override def numPartitions: Int = partCount

        override def getPartition(key: Any): Int =
          scala.math.abs(key.hashCode()) % partCount
      }, partCount)
  }

  private def getEpoch0AndIterator[T: ClassTag](name: String, topkWrapper: TopKWrapper,
      iterator: Iterator[Any]): (() => Long, Iterator[Any], Int) = {
    if (iterator.hasNext) {
      var tupleIterator = iterator
      val tsCol = if (topkWrapper.timeInterval > 0) {
        topkWrapper.timeSeriesColumn
      }
      else {
        -1
      }
      val epoch = () => {
        if (topkWrapper.epoch != -1L) {
          topkWrapper.epoch
        } else if (tsCol >= 0) {
          var epoch0 = -1L
          val iter = tupleIterator.asInstanceOf[Iterator[(T, Any)]]
          val tupleBuf = new mutable.ArrayBuffer[(T, Any)](4)

          // assume first row will have the least time
          // TODO: this assumption may not be correct and we may need to
          // do something more comprehensive
          do {
            val tuple = iter.next()
            epoch0 = tuple match {
              case (_, (_, epochh: Long)) => epochh
              case (_, epochh: Long) => epochh
            }

            tupleBuf += tuple.copy()
          } while (epoch0 <= 0)
          tupleIterator = tupleBuf.iterator ++ iter
          epoch0
        } else {
          System.currentTimeMillis()
        }

      }
      (epoch, tupleIterator, tsCol)
    } else {
      null
    }
  }

  private def addDataForTopK[T: ClassTag](topKWrapper: TopKWrapper,
      tupleIterator: Iterator[Any], topK: TopK, tsCol: Int, time: Long): Unit = {

    val stsummary = topKWrapper.stsummary
    val streamSummaryAggr: StreamSummaryAggregation[T] = if (stsummary) {
      topK.asInstanceOf[StreamSummaryAggregation[T]]
    } else {
      null
    }
    val topKHokusai = if (!stsummary) {
      topK.asInstanceOf[TopKHokusai[T]]
    } else {
      null
    }
    // val topKKeyIndex = topKWrapper.schema.fieldIndex(topKWrapper.key.name)
    if (tsCol < 0) {
      if (stsummary) {
        throw new IllegalStateException(
          "Timestamp column is required for stream summary")
      }
      topKWrapper.frequencyCol match {
        case None =>
          topKHokusai.addEpochData(tupleIterator.asInstanceOf[Iterator[T]].
              toSeq.foldLeft(
            scala.collection.mutable.Map.empty[T, Long]) {
            (m, x) => m + ((x, m.getOrElse(x, 0L) + 1))
          }, time)
        case Some(_) =>
          val datamap = mutable.Map[T, Long]()
          tupleIterator.asInstanceOf[Iterator[(T, Long)]] foreach {
            case (key, freq) =>
              datamap.get(key) match {
                case Some(prevvalue) => datamap +=
                    (key -> (prevvalue + freq))
                case None => datamap +=
                    (key -> freq)
              }

          }
          topKHokusai.addEpochData(datamap, time)
      }
    } else {
      val dataBuffer = new mutable.ArrayBuffer[KeyFrequencyWithTimestamp[T]]
      val buffer = topKWrapper.frequencyCol match {
        case None =>
          tupleIterator.asInstanceOf[Iterator[(T, Long)]] foreach {
            case (key, timeVal) =>
              dataBuffer += new KeyFrequencyWithTimestamp[T](key, 1L, timeVal)
          }
          dataBuffer
        case Some(_) =>
          tupleIterator.asInstanceOf[Iterator[(T, (Long, Long))]] foreach {
            case (key, (freq, timeVal)) =>
              dataBuffer += new KeyFrequencyWithTimestamp[T](key,
                freq, timeVal)
          }
          dataBuffer
      }
      if (stsummary) {
        streamSummaryAggr.addItems(buffer)
      }
      else {
        topKHokusai.addTimestampedData(buffer)
      }
    }
  }

  def populateTopK[T: ClassTag](rows: RDD[Row], topkWrapper: TopKWrapper,
    session: SnappySession, name: String, topKRDD: RDD[(Int, TopK)],
    time: Long): Unit = {
    val partitioner = topKRDD.partitioner.get
    // val pairRDD = rows.map[(Int, Any)](topkWrapper.rowToTupleConverter(_, partitioner))
    val pairRDD = rows.mapPartitionsPreserve[(Int, mutable.ArrayBuffer[Any])](iter => {
      val batches = mutable.ArrayBuffer.empty[(Int, mutable.ArrayBuffer[Any])]
      val map = iter.foldLeft(mutable.Map.empty[Int, mutable.ArrayBuffer[Any]])((m, x) => {
        val (partitionID, elem) = topkWrapper.rowToTupleConverter(x, partitioner)
        val list = m.getOrElse(partitionID, {
         val newList = mutable.ArrayBuffer[Any]()
         m += partitionID -> newList
         newList
        }) += elem
        if (list.size > 1000) {
          batches += partitionID -> list
          m -= partitionID
        }
        m
      })
      map.toIterator ++ batches.iterator
    }, preservesPartitioning = true)

    val newTopKRDD = topKRDD.cogroup(pairRDD).mapPartitionsPreserve[(Int, TopK)](
      iterator => {

        val (key, (topkIterable, dataIterable)) = iterator.next()
        val tsCol = if (topkWrapper.timeInterval > 0) {
          topkWrapper.timeSeriesColumn
        }
        else {
          -1
        }

        topkIterable.head match {
          case z: TopKStub =>
            val totalDataIterable = dataIterable.foldLeft(
                mutable.ArrayBuffer.empty[Any])((m, x) => {
              if (m.size > x.size) {
                m ++= x
              } else {
                x ++= m
              }
            })
            if (totalDataIterable.isEmpty) {
              scala.collection.Iterator(key -> z)
            } else {
            val (epoch0, itr, tsCol) = getEpoch0AndIterator[T](name,
              topkWrapper, totalDataIterable.toIterator)
            val topK = if (topkWrapper.stsummary) {
              StreamSummaryAggregation.create[T](topkWrapper.size,
                topkWrapper.timeInterval, epoch0, topkWrapper.maxinterval)
            } else {
              TopKHokusai.create[T](topkWrapper.cms, topkWrapper.size,
                tsCol, topkWrapper.timeInterval, epoch0)
            }
            addDataForTopK[T](topkWrapper,
              itr, topK, tsCol, time)
            scala.collection.Iterator(key -> topK)
            }

          case topK: TopK =>
            dataIterable.foreach { x => addDataForTopK[T](
              topkWrapper, x.iterator, topK, tsCol, time)
            }
            scala.collection.Iterator(key -> topK)
        }
      }, preservesPartitioning = true)

    newTopKRDD.persist
    newTopKRDD.localCheckpoint()
    // To allow execution of RDD && checkpointing to occur, below is essential
    newTopKRDD.count()
    val catalog = session.sessionState.catalog.asInstanceOf[SnappyAQPSessionCatalog]
    // Unpersist old rdd in a write lock
    topkWrapper.executeInWriteLock {
      catalog.registerTopK(topkWrapper, newTopKRDD, ifExists = false, overwrite = true)
      topKRDD.unpersist(blocking = false)
    }
  }
}
