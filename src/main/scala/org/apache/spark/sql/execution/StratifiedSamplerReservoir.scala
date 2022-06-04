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

import scala.language.reflectiveCalls

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.collection.{ChangeValue, Utils, WrappedInternalRow}
import org.apache.spark.sql.execution.StratifiedSampler._
import org.apache.spark.sql.types.StructType

/**
 * A simple reservoir based stratified sampler that will use the provided
 * reservoir size for every stratum present in the incoming rows.
 */
final class StratifiedSamplerReservoir(_options: SampleOptions)
    extends StratifiedSampler(_options) {

  val reservoirSize: Int = options.stratumSize
  override def onTruncate(): Unit = {}

  private final class ProcessRows(rowEncoder: ExpressionEncoder[Row])
      extends ChangeValue[Row, StratumReservoir] {

    override def keyCopy(row: Row): Row = row.copy()

    override def defaultValue(row: Row): StratumReservoir = {
      // create new stratum if required
      val reservoir = new Array[UnsafeRow](reservoirSize)
      reservoir(0) = newMutableRow(row, rowEncoder)
      Utils.fillArray(reservoir, EMPTY_ROW, 1, reservoirSize)
      new StratumReservoir(reservoir, 1, 1)
    }

    // This is placeholder. Need to implement if needed.
    override def mergeValueNoNull(row: Row, sr: StratumReservoir): (StratumReservoir, Boolean,
        Boolean) = {
      (mergeValue(row, sr), false, false)
    }

    override def mergeValue(row: Row,
        sr: StratumReservoir): StratumReservoir = {
      // else update meta information in current stratum
      sr.batchTotalSize += 1
      val stratumSize = sr.reservoirSize
      if (stratumSize >= reservoirSize) {
        // copy into the reservoir as per probability (stratumSize/totalSize)
        val rnd = rng.nextInt(sr.batchTotalSize)
        if (rnd < stratumSize) {
          // pick up this row and replace a random one from reservoir
          sr.reservoir(rng.nextInt(stratumSize)) =
              newMutableRow(row, rowEncoder)
        }
      } else {
        // always copy into the reservoir for this case
        sr.reservoir(stratumSize) = newMutableRow(row, rowEncoder)
        sr.reservoirSize += 1
      }
      sr
    }
  }

  override protected def strataReservoirSize: Int = reservoirSize

  override def append[U](rows: Iterator[Row], init: U,
      processFlush: (U, InternalRow) => U, startBatch: (U, Int) => U, endBatch: U => U,
      rowEncoder: ExpressionEncoder[Row], partIndex: Int): Long = {
    if (rows.hasNext) {
      strata.bulkChangeValues(rows, new ProcessRows(rowEncoder), getBucketId(partIndex),
        isBucketLocal(partIndex))
    } else 0
  }

  override def sample(items: Iterator[InternalRow],
      rowEncoder: ExpressionEncoder[Row],
      flush: Boolean): Iterator[InternalRow] = {
    val processRow = new ProcessRows(rowEncoder)
    val wrappedRow = new WrappedInternalRow(StructType(schema.dropRight(1)))
    for (row <- items) {
      wrappedRow.internalRow = row
      // TODO: also allow invoking the optimized methods in
      // MultiColumnOpenHash* classes for WrappedInternalRow
      strata.changeValue(wrappedRow, processRow)
    }

    if (flush) {
      // iterate over all the reservoirs for marked partition
      waitForSamplers(1, 5000)
      setFlushStatus(true)
      // remove sampler used only for DataFrame => DataFrame transformation
      removeSampler(name, markFlushed = true)
      // at this point we don't have a problem with concurrency
      strata.flatMap(_.valuesIterator.flatMap(_.iterator(reservoirSize,
        reservoirSize, schema.length, doReset = true, fullReset = false)))
    } else {
      if (numSamplers.decrementAndGet() == 1) numSamplers.synchronized {
        numSamplers.notifyAll()
      }
      Iterator.empty
    }
  }

  override def clone: StratifiedSamplerReservoir =
    new StratifiedSamplerReservoir(options)

  override  def flushReservoir[U](init: U, process: (U, InternalRow) => U,
    startBatch: (U, Int) => U, endBatch: U => U): U = {
    init

  }
}
