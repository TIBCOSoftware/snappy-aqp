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
package org.apache.spark.sql

import scala.collection.mutable

import io.snappydata.sql.catalog.SnappyExternalCatalog
import org.apache.commons.math3.distribution.{NormalDistribution, TDistribution}

import org.apache.spark.sql.collection.MultiColumnOpenHashMap
import org.apache.spark.sql.collection.Utils._
import org.apache.spark.sql.execution.StratifiedSample
import org.apache.spark.sql.sources.StatCounter
import org.apache.spark.sql.types.{DoubleType, NumericType}

/**
 * Encapsulates a DataFrame created after stratified sampling.
 */
case class SampleDataFrameContractImpl(@transient session: SnappySession,
    frame: SampleDataFrame, @transient logicalPlan: StratifiedSample)
    extends SampleDataFrameContract {

  /** LogicalPlan is deliberately transient, so keep qcs separately */
  final val qcs = logicalPlan.qcs._1

  final type ErrorRow = (Double, Double, Double, Double)

  def registerSampleTable(tableName: String,
      baseTableOpt: Option[String]): Unit = {
    val options = baseTableOpt match {
      case Some(baseTable) => logicalPlan.options.mapValues(_.toString) +
          (SnappyExternalCatalog.BASETABLE_PROPERTY ->
              session.sessionCatalog.resolveExistingTable(baseTable).unquotedString)
      case None => logicalPlan.options.mapValues(_.toString)
    }
    session.createTableInternal(session.tableIdentifier(tableName),
      SnappyContext.SAMPLE_SOURCE, userSpecifiedSchema = None, schemaDDL = None,
      SaveMode.Overwrite, options, isBuiltIn = true, query = Some(logicalPlan.child))
  }

  def errorStats(columnName: String,
      groupBy: Set[String] = Set.empty): MultiColumnOpenHashMap[StatCounter] = {
    val schema = this.frame.schema
    val allColumns = schema.fieldNames
    val colIndex = columnIndex(columnName, allColumns, "errorEstimateStats")
    val requireConversion = schema(colIndex).dataType match {
      case _: DoubleType => false
      case _: NumericType => true // conversion required
      case tp => throw new AnalysisException("errorEstimateStats: Cannot " +
          s"estimate for non-integral column $columnName with type $tp")
    }

    // map group by columns to indices
    val columnIndices = if (groupBy != null && groupBy.nonEmpty) {
      val groupByIndices = groupBy.map(columnIndex(_, allColumns,
        "errorEstimateStats"))
      // check that all columns should be part of qcs
      val qcsCols = intArrayOps(logicalPlan.qcs._1)
      for (col <- groupByIndices) {
        require(qcsCols.indexOf(col) >= 0, "group by columns should be " +
            s"part of QCS: ${qcsCols.map(allColumns(_)).mkString(", ")}")
      }
      if (groupByIndices.size == qcs.length) qcs
      else groupByIndices.toSeq.sorted.toArray
    } else qcs

    frame.rdd.mapPartitions { rows =>
      // group by column map
      val groupedMap = new MultiColumnOpenHashMap[StatCounter](columnIndices,
        columnIndices.map(schema(_).dataType))
      for (row <- rows) {
        if (!row.isNullAt(colIndex)) {
          val stat = groupedMap.get(row).getOrElse {
            val sc = new StatCounter()
            groupedMap(row) = sc
            sc
          }
          // merge the new row into statistics
          if (requireConversion) {
            stat.merge(row(colIndex).asInstanceOf[Number].doubleValue())
          }
          else {
            stat.merge(row.getDouble(colIndex))
          }
        }
      }
      Iterator(groupedMap)
    }.reduce((map1, map2) => {
      // use larger of the two maps
      val (m1, m2) =
        if (map1.size >= map2.size) (map1, map2) else (map2, map1)
      if (m2.nonEmpty) {
        for ((row, stat) <- m2.iterator) {
          // merge the two stats or copy from m2 if m1 does not have the row
          m1.get(row) match {
            case Some(s) => s.merge(stat)
            case None => m1(row) = stat
          }
        }
      }
      m1
    })
  }

  def errorEstimateAverage(columnName: String, confidence: Double,
      groupByColumns: Set[String] = Set.empty): mutable.Map[Row, ErrorRow] = {
    assert(confidence >= 0.0 && confidence <= 1.0,
      "confidence argument expected to be between 0.0 and 1.0")
    val tcache = new StudentTCacher(confidence)
    val stats = errorStats(columnName)
    mutable.Map() ++ stats.mapValues { stat =>
      val nsamples = stat.count
      val mean = stat.mean
      val stdev = math.sqrt(stat.variance / nsamples)
      // 30 is taken to be cut-off limit in most statistics calculations
      // for z vs t distributions (unlike StudentTCacher that uses 100)
      val errorEstimate =
        if (nsamples >= 30) tcache.normalApprox * stdev
        else tcache.get(nsamples) * stdev
      val percentError = (errorEstimate * 100.0) / math.abs(mean)
      (mean, stdev, errorEstimate, percentError)
    }
  }
}

/**
  * A utility class for caching Student's T distribution values for a given confidence level
  * and various sample sizes. This is used by the MeanEvaluator to efficiently calculate
  * confidence intervals for many keys.
  */
private[spark] class StudentTCacher(confidence: Double) {

  val NORMAL_APPROX_SAMPLE_SIZE = 100  // For samples bigger than this, use Gaussian approximation

  val normalApprox: Double =
    new NormalDistribution().inverseCumulativeProbability(1 - (1 - confidence) / 2)
  val cache: Array[Double] = Array.fill[Double](NORMAL_APPROX_SAMPLE_SIZE)(-1.0)

  def get(sampleSize: Long): Double = {
    if (sampleSize >= NORMAL_APPROX_SAMPLE_SIZE) {
      normalApprox
    } else {
      val size = sampleSize.toInt
      if (cache(size) < 0) {
        val tDist = new TDistribution(size - 1)
        cache(size) = tDist.inverseCumulativeProbability(1 - (1 - confidence) / 2)
      }
      cache(size)
    }
  }
}

