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
package org.apache.spark.sql.execution

import io.snappydata.sql.catalog.SnappyExternalCatalog

import org.apache.spark.Partitioner
import org.apache.spark.sql.LockUtils.ReadWriteLock
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.collection.Utils._
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.hive.SnappyAQPSessionCatalog
import org.apache.spark.sql.sources.CastLongTime
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Row}

protected[sql] final class TopKWrapper(val name: String,
    val cms: CMSParams, val size: Int, val timeSeriesColumn: Int,
    val timeInterval: Long, val baseTableSchema: StructType,
    val key: StructField, val frequencyCol: Option[Int], val epoch: Long,
    val maxinterval: Int, val stsummary: Boolean,
    val baseTable: Option[TableIdentifier])
    extends ReadWriteLock with CastLongTime with Serializable {

  val rowToTupleConverter: (Row, Partitioner) => (Int, Any) =
    TopKWrapper.getRowToTupleConverter(this)

  override protected def getNullMillis(getDefaultForNull: Boolean): Long =
    if (getDefaultForNull) System.currentTimeMillis() else -1L

  override def timeColumnType: Option[DataType] = {
    if (timeSeriesColumn >= 0) {
      Some(baseTableSchema(timeSeriesColumn).dataType)
    } else {
      None
    }
  }

  override def module: String = "TopKHokusai"
}

object TopKWrapper {

  def apply(catalog: SnappyAQPSessionCatalog, name: String,
      key: String, opts: Map[String, String], baseTableSchema: StructType): TopKWrapper = {

    val cols = baseTableSchema.fieldNames
    val module = "TopKCMS"

    var epsAndcf = false
    var widthAndDepth = false
    var isStreamParam = false

    // first normalize options into a mutable map to ease lookup and removal
    val options = new CaseInsensitiveMutableHashMap(opts)

    val depth = options.remove("depth").map { d =>
      widthAndDepth = true
      parseInteger(d, module, "depth")
    }.getOrElse(7)
    val width = options.remove("width").map { w =>
      widthAndDepth = true
      parseInteger(w, module, "width")
    }.getOrElse(200)
    val eps = options.remove("eps").map { e =>
      epsAndcf = true
      parseDouble(e, module, "eps", 0.0, 1.0)
    }.getOrElse(0.01)
    val confidence = options.remove("confidence").map { c =>
      epsAndcf = true
      parseDouble(c, module, "confidence", 0.0, 1.0)
    }.getOrElse(0.95)

    val size = options.remove("size").map(
      parseInteger(_, module, "size")).getOrElse(100)

    val stSummary = options.remove("streamSummary").exists {
      ss =>
        try {
          ss.toBoolean
        } catch {
          case _: IllegalArgumentException =>
            throw new AnalysisException(
              s"$module: Cannot parse boolean 'streamSummary'=$ss")
        }
    }
    val maxInterval = options.remove("maxInterval").map { i =>
      isStreamParam = true
      parseInteger(i, module, "maxInterval")
    }.getOrElse(20)

    val tsCol = options.remove("timeSeriesColumn").map(
      parseColumn(_, cols, module, "timeSeriesColumn")).getOrElse(-1)
    val timeInterval = options.remove("timeInterval").map(
      parseTimeInterval(_, module)).getOrElse(
        if (tsCol >= 0 || stSummary) 5000L else Long.MaxValue)

    val frequencyCol = options.remove("frequencyCol").map(
      parseColumn(_, cols, module, "frequencyCol"))

    val epoch = options.remove("epoch").map(
      parseTimestamp(_, module, "epoch")).getOrElse(-1L)

    val baseTable = options.remove(SnappyExternalCatalog.BASETABLE_PROPERTY).map {
      t => catalog.resolveExistingTable(t.toString)
    }

    // check for any remaining unsupported options
    if (options.nonEmpty) {
      val optMsg = if (options.size > 1) "options" else "option"
      throw new AnalysisException(
        s"$module: Unknown $optMsg: $options")
    }

    if (epsAndcf && widthAndDepth) {
      throw new AnalysisException("TopK parameters should specify either " +
        "(eps, confidence) or (width, depth) but not both.")
    }
    if ((epsAndcf || widthAndDepth) && stSummary) {
      throw new AnalysisException("TopK parameters shouldn't specify " +
        "hokusai params for a stream summary.")
    }
    if (isStreamParam) {
      if (!stSummary) {

        throw new AnalysisException("TopK parameters should specify " +
          "stream summary as true with stream summary params.")
      }
      if (tsCol < 0) {
        throw new AnalysisException(
          "Timestamp column is required for stream summary")
      }
    }

    val cms =
      if (epsAndcf) CMSParams(eps, confidence) else CMSParams(width, depth)

    new TopKWrapper(name, cms, size, tsCol, timeInterval, baseTableSchema,
      baseTableSchema(key), frequencyCol, epoch, maxInterval,
      stSummary, baseTable)
  }

  private def getRowToTupleConverter(
      topkWrapper: TopKWrapper): (Row, Partitioner) => (Int, Any) = {

    val tsCol = if (topkWrapper.timeInterval > 0) topkWrapper.timeSeriesColumn else -1

    val topKKeyIndex = topkWrapper.baseTableSchema.fieldIndex(
      topkWrapper.key.name)
    if (tsCol < 0) {
      topkWrapper.frequencyCol match {
        case None =>
          (row: Row, partitioner: Partitioner) =>
            partitioner.getPartition(row(topKKeyIndex)) -> row(topKKeyIndex)
          case Some(freqCol) =>
          (row: Row, partitioner: Partitioner) => {

            val freq = row(freqCol) match {
              case num: Long => num
              case num: Double => num.toLong
              case num: Int => num.toLong
              case num: Float => num.toLong

              case num: java.lang.Number => num.longValue()
              case x => x
            }
            partitioner.getPartition(row(topKKeyIndex)) -> (row(topKKeyIndex), freq)
          }
      }
    } else {
      topkWrapper.frequencyCol match {
        case None =>
          (row: Row, partitioner: Partitioner) => {
            val key = row(topKKeyIndex)
            val timeVal = topkWrapper.parseMillis(row, tsCol)
            partitioner.getPartition(key) -> (key, timeVal)
          }

          case Some(freqCol) =>
          (row: Row, partitioner: Partitioner) => {
            val freq = row(freqCol) match {
              case num: Long => num
              case num: Double => num.toLong
              case num: Int => num.toLong
              case num: Float => num.toLong
              case num: java.lang.Number => num.longValue()
              case x => x
            }
            val key = row(topKKeyIndex)
            val timeVal = topkWrapper.parseMillis(row, tsCol)
            partitioner.getPartition(key) -> (key, (freq, timeVal))
          }
      }
    }
  }
}
