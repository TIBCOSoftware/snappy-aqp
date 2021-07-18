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
package org.apache.spark.sql.execution

import java.io.{DataInput, DataOutput}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.reflectiveCalls

import com.gemstone.gemfire.internal.cache.lru.Sizeable
import com.gemstone.gemfire.internal.shared.SystemProperties
import it.unimi.dsi.fastutil.ints.IntArrayList

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.collection._
import org.apache.spark.sql.execution.StratifiedSampler._
import org.apache.spark.sql.sources.CastLongTime
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A stratified sampling implementation that uses a fraction and initial
 * cache size. Latter is used as the initial reservoir size per stratum
 * for reservoir sampling. It primarily tries to satisfy the fraction of
 * the total data repeatedly filling up the cache as required (and expanding
 * the cache size for bigger reservoir if required in next rounds).
 * The fraction is attempted to be satisfied while ensuring that the selected
 * rows are equally divided among the current stratum (for those that received
 * any rows, that is).
 */
class StratifiedSamplerCached(_options: SampleOptions)
    extends StratifiedSampler(_options) with CastLongTime {

  private val cacheSize: AtomicInteger = new AtomicInteger(options.stratumSize)
  val fraction: Double = options.fraction

  def columnBatchSize: Int = options.memBatchSize

  def timeSeriesColumn: Int = options.timeSeriesColumn

  override def onTruncate(): Unit = {}
  def timeInterval: Long = options.timeInterval

  private val batchSamples, slotSize = new AtomicInteger
  /** Keeps track of the maximum number of samples in a stratum seen so far */
  private val maxSamples = new AtomicLong
  // initialize timeSlotStart to MAX so that it will always be set first
  // time around for the slot (since every valid time will be less)
  private val timeSlotStart = new AtomicLong(Long.MaxValue)
  private val timeSlotEnd = new AtomicLong
  /**
   * Total number of samples collected in a timeInterval. Not atomic since
   * it is always updated and read under global lock.
   */
  private var timeSlotSamples = 0

  protected val flushInProgress = new AtomicBoolean(false)

  override def timeColumnType: Option[DataType] = {
    if (timeSeriesColumn >= 0) {
      Some(schema(timeSeriesColumn).dataType)
    } else {
      None
    }
  }

  private def setTimeSlot(timeSlot: Long) {
    compareOrderAndSet(timeSlotStart, timeSlot, getMax = false)
    compareOrderAndSet(timeSlotEnd, timeSlot, getMax = true)
  }

  private def updateTimeSlot(row: Row,
      useCurrentTimeIfNoColumn: Boolean): Unit = {
    val tsCol = timeSeriesColumn
    if (tsCol >= 0) {
      val timeVal = parseMillis(row, tsCol)
      if (timeVal > 0) setTimeSlot(timeVal)
    }
    // update the timeSlot if a) explicitly requested, or
    // b) in case if it has not been initialized at all
    else if (useCurrentTimeIfNoColumn || timeSlotEnd.get == 0L) {
      setTimeSlot(System.currentTimeMillis)
    }
  }

  protected final class ProcessRows[U](val processFlush: (U, InternalRow) => U,
      val startBatch: (U, Int) => U, val endBatch: U => U,
      rowEncoder: ExpressionEncoder[Row], var result: U)
      extends ChangeValue[Row, StratumReservoir] {

    override def keyCopy(row: Row): Row = row.copy()

    override def defaultValue(row: Row): StratumCache = {
      val capacity = cacheSize.get
      val reservoir = new Array[UnsafeRow](capacity)
      reservoir(0) = newMutableRow(row, rowEncoder)
      Utils.fillArray(reservoir, EMPTY_ROW, 1, capacity)
      // for time-series data don't start with shortfall since this indicates
      // that a new stratum has appeared which can remain under-sampled for
      // a while till it doesn't get enough rows
      val initShortFall = if (timeInterval > 0) {
        // update timeSlot start and end
        updateTimeSlot(row, useCurrentTimeIfNoColumn = true)
        0
      } else math.max(0, maxSamples.get - capacity).toInt
      val sc = new StratumCache(reservoir, 1, 1, 1, initShortFall)
      maxSamples.compareAndSet(0, 1)
      batchSamples.incrementAndGet()
      slotSize.incrementAndGet()
      sc
    }

    override def mergeValue(row: Row, sr: StratumReservoir): StratumCache = {
      val (sc, isNull, _) = mergeValueNoNull(row, sr)
      if (isNull) {
        null
      } else {
        sc
      }
    }

    override def mergeValueNoNull(row: Row, sr: StratumReservoir): (StratumCache, Boolean,
        Boolean) = {
      val sc = sr.asInstanceOf[StratumCache]
      // else update meta information in current stratum
      sc.batchTotalSize += 1
      val reservoirCapacity = cacheSize.get + sc.prevShortFall
      if (sc.reservoirSize >= reservoirCapacity) {
        val rnd = rng.nextInt(sc.batchTotalSize)
        // pick up this row with probability of reservoirCapacity/totalSize
        val updated = if (rnd < reservoirCapacity) {
          // replace a random row in reservoir
          sc.reservoir(rng.nextInt(reservoirCapacity)) = newMutableRow(row, rowEncoder)
          // update timeSlot start and end
          if (timeInterval > 0) {
            updateTimeSlot(row, useCurrentTimeIfNoColumn = false)
          }
          true
        } else false
        if (batchSamples.get >= (fraction * slotSize.incrementAndGet())) (sc, false, updated)
        else (sc, true, updated)
      } else {
        // if reservoir has empty slots then fill them up first
        val reservoirLen = sc.reservoir.length
        if (reservoirLen <= sc.reservoirSize) {
          // new size of reservoir will be > reservoirSize given that it
          // increases only in steps of 1 and the expression
          // reservoirLen + (reservoirLen >>> 1) + 1 will certainly be
          // greater than reservoirLen
          val newReservoir = new Array[UnsafeRow](math.max(math.min(
            reservoirCapacity, reservoirLen + (reservoirLen >>> 1) + 1),
            cacheSize.get))
          Utils.fillArray(newReservoir, EMPTY_ROW, reservoirLen,
            newReservoir.length)
          System.arraycopy(sc.reservoir, 0, newReservoir,
            0, reservoirLen)
          sc.reservoir = newReservoir
        }
        sc.reservoir(sc.reservoirSize) = newMutableRow(row, rowEncoder)
        sc.reservoirSize += 1
        sc.totalSamples += 1

        // update timeSlot start and end
        if (timeInterval > 0) {
          updateTimeSlot(row, useCurrentTimeIfNoColumn = false)
        }

        compareOrderAndSet(maxSamples, sc.totalSamples, getMax = true)
        batchSamples.incrementAndGet()
        slotSize.incrementAndGet()
        (sc, false, true)
      }
    }

    override def segmentAbort(seg: SegmentMap[Row, StratumReservoir]): Boolean = {
      if (!flushInProgress.get && flushInProgress.compareAndSet(false, true)) {
        // increase the priority of this thread
        val currentThread = Thread.currentThread()
        val priority = currentThread.getPriority
        try {
          // reset batch counters
          val numSamples = batchSamples.get

          val slots = slotSize.get
          val fractionSampled = if (slots > 0) numSamples.toDouble / slots else 1.0d

          if ((numSamples >= SystemProperties.SNAPPY_MIN_COLUMN_DELTA_ROWS &&
              fractionSampled < fraction ) || (fraction - fractionSampled)/fraction > .001 ) {
            currentThread.setPriority(Thread.MAX_PRIORITY)
            result = flushCache(result, processFlush, startBatch, endBatch)
            true
          } else {
            false
          }
        } finally {
          flushInProgress.synchronized {
            if (flushInProgress.compareAndSet(true, false)) {
              flushInProgress.notifyAll()
            }
          }
          // restore the thread priority
          if (priority != currentThread.getPriority) {
            currentThread.setPriority(priority)
          }
        }
      } else {
        // wait for the flush thread to finish clearing maps
        flushInProgress.synchronized {
          while (flushInProgress.get) flushInProgress.wait(50)
        }
        true
      }

    }
  }

  /*
  private def columnBuilders = schema.map {
    attribute =>
      val columnType = ColumnType(attribute.dataType)
      val initialBufferSize = columnType.defaultSize * cacheBatchSize
      ColumnBuilder(attribute.dataType, initialBufferSize,
        attribute.name, useCompression = false)
  }.toArray
  protected val dataTypes = schema.map(_.dataType).toArray
  */

  private def flushCache[U](init: U, process: (U, InternalRow) => U, startBatch: (U, Int) => U,
      endBatch: U => U): U = {

    // First acquire all the segment write locks so no concurrent processors
    // are in progress. Keep the locks only for short duration to transfer map
    // to local storage.
    val collectedRows = new mutable.HashMap[Int, mutable.ListBuffer[InternalRow]]()
    try {
      strata.writeLockAllSegments { segments =>

        val numSamples = batchSamples.get
        val numStrata = strata.size

        // if more than 50% of keys are empty, then clear the whole map
        val emptyStrata = strata.foldValuesRead[Int](0, { (bid, cache, empty) =>
          if (cache.reservoir.length == 0) empty + 1 else empty
        })

        val prevCacheSize = this.cacheSize.get
        val timeInterval = this.timeInterval
        var fullReset = false
        if (timeInterval > 0) {
          timeSlotSamples += numSamples
          // update the timeSlot with current time if no timeSeries column
          // has been provided
          if (timeSeriesColumn < 0) {
            updateTimeSlot(null, useCurrentTimeIfNoColumn = true)
          }
          // in case the current timeSlot is over, reset maxSamples
          // (thus causing shortFall to clear up)
          val tsEnd = timeSlotEnd.get
          fullReset = (tsEnd != 0) &&
              ((tsEnd - timeSlotStart.get) >= timeInterval)
          if (fullReset) {
            maxSamples.set(0)
            // reset timeSlot start and end
            timeSlotStart.set(Long.MaxValue)
            timeSlotEnd.set(0)
            // recalculate the reservoir size for next round as per the average
            // of total number of non-empty reservoirs
            val nonEmptyStrata = numStrata.toInt - emptyStrata
            if (nonEmptyStrata > 0) {
              val newCacheSize = timeSlotSamples / nonEmptyStrata
              // no need to change if it is close enough already
              if (math.abs(newCacheSize - prevCacheSize) > (prevCacheSize / 10)) {
                // try to be somewhat gentle in changing to new value
                this.cacheSize.set(math.max(4,
                  (newCacheSize * 2 + prevCacheSize) / 3))
              }
            }
            timeSlotSamples = 0
          }
        } else {
          // increase the cacheSize for future data but not much beyond
          // columnBatchSize
          val newCacheSize = prevCacheSize + (prevCacheSize >>> 1)
          if ((newCacheSize * numStrata) <= math.abs(this.columnBatchSize) * 2) {
            this.cacheSize.set(newCacheSize)
          }
        }
        batchSamples.set(0)
        slotSize.set(0)

        /*
        // fetch all values into the buffer
        segments.foreach(_.foldValues[Unit]((), foldReservoir(prevCacheSize,
          doReset = true, fullReset, { (u, row) =>
           columnBatchHolder.appendRow(u, row); u })))
        */

        val doReset = true
        segments.foreach(_.foldValues[Unit]((), (bid, sr, init) => {
          var currentReservoirRows = collectedRows.getOrElseUpdate(bid,
            new ListBuffer[InternalRow]())
          foldReservoir[Unit](prevCacheSize,
            doReset, fullReset, { (u, row) => currentReservoirRows += row; u })(bid,
            sr, init)
        }, doReset))

        if (numStrata < (emptyStrata << 1)) {
          strata.clear()
        }

        // at this point allow other threads attempting flush to proceed
        flushInProgress.synchronized {
          if (flushInProgress.compareAndSet(true, false)) {
            flushInProgress.notifyAll()
          }
        }
      }

      /*
      columnBatchHolder.forceEndOfBatch()
      // do process() applies outside segment locks
      val allIndices = 0 until schema.length
      // we expect to always find the blocks saved previously
      val columnBatches = blockIds.map(blockManager.getSingle(_).get
          .asInstanceOf[ColumnBatch]).toIterator
      endBatch(ExternalStoreUtils.columnBatchesToRows(columnBatches,
        allIndices, dataTypes, schema).foldLeft(init)(process))
      */

      collectedRows.foreach(bucket =>
        endBatch(bucket._2.foldLeft(startBatch(init, bucket._1))(process)))
      init
    } finally {
      // blockIds.foreach(blockManager.removeBlock(_, tellMaster = false))
    }
  }

  override def flushReservoir[U](init: U, process: (U, InternalRow) => U,
    startBatch: (U, Int) => U, endBatch: U => U): U = {
    flushCache(init, process, startBatch, endBatch);
  }

  override protected def strataReservoirSize: Int = cacheSize.get

  override def append[U](rows: Iterator[Row],
      init: U, processFlush: (U, InternalRow) => U, startBatch: (U, Int) => U, endBatch: U => U,
      rowEncoder: ExpressionEncoder[Row], partIndex: Int): Long = {
    if (rows.hasNext) {
      val processedResult = new ProcessRows(processFlush, startBatch, endBatch,
        rowEncoder, init)
      strata.bulkChangeValues(rows, processedResult, getBucketId(partIndex,
        getPrimaryBucketArray(partIndex)), isBucketLocal(partIndex))
    } else 0
  }

  protected def getPrimaryBucketArray(partIndex: Int): IntArrayList = null

  override def sample(items: Iterator[InternalRow],
      rowEncoder: ExpressionEncoder[Row],
      flush: Boolean): Iterator[InternalRow] = {
    // use "batchSize" to determine the sample buffer size
    val batchSize = BUFSIZE
    val sampleBuffer = new mutable.ArrayBuffer[InternalRow](math.min(batchSize,
      (batchSize * fraction * 10).toInt))
    val wrappedRow = new WrappedInternalRow(StructType(schema.dropRight(1)))
    def dummy[U](a: U, b: Int): U = a

    new GenerateFlatIterator[InternalRow, Boolean](finished => {
      val sbuffer = sampleBuffer
      if (sbuffer.nonEmpty) sbuffer.clear()
      val processRows = new ProcessRows[Unit]((_, sampledRow) => {
        sbuffer += sampledRow
      }, dummy, identity, rowEncoder, ())

      var flushed = false
      while (!flushed && items.hasNext) {
        wrappedRow.internalRow = items.next()
        if (strata.changeValue(wrappedRow, processRows) == null) {
          processRows.segmentAbort(null)
          flushed = sbuffer.nonEmpty
        }
      }

      if (sbuffer.nonEmpty) {
        (sbuffer.iterator, false)
      } else if (finished || !flush) {
        // if required notify any other waiting samplers that iteration is done
        if (numSamplers.decrementAndGet() == 1) numSamplers.synchronized {
          numSamplers.notifyAll()
        }
        (GenerateFlatIterator.TERMINATE, true)
      } else {
        // flush == true
        // wait for all other partitions to flush the cache
        waitForSamplers(1, 5000)
        setFlushStatus(true)
        // remove sampler used only for DataFrame => DataFrame transformation
        removeSampler(name, markFlushed = true)
        flushCache[Unit]((), (_, sampledRow) => {
          sbuffer += sampledRow
        }, dummy, identity)
        (sbuffer.iterator, true)
      }
    }, false)
  }

  override def clone: StratifiedSamplerCached = new StratifiedSamplerCached(options)
}

/**
 * An extension to StratumReservoir to also track total samples seen since
 * last time slot and short fall from previous rounds.
 */
final class StratumCache(_reservoir: Array[UnsafeRow],
    _reservoirSize: Int, _batchTotalSize: Int, var totalSamples: Int,
    var prevShortFall: Int) extends StratumReservoir(_reservoir,
  _reservoirSize, _batchTotalSize) {

  // For deserialization
  def this() = this(Array.empty[UnsafeRow], -5, -5, -5, -5)

  override def reset(prevReservoirSize: Int, newReservoirSize: Int,
      fullReset: Boolean) {

    if (newReservoirSize > 0) {
      val numSamples = reservoirSize

      // reset transient data
      super.reset(prevReservoirSize, newReservoirSize, fullReset)

      // check for the end of current time-slot; if it has ended, then
      // also reset the "shortFall" and other such history
      if (fullReset) {
        totalSamples = 0
        prevShortFall = 0
      } else {
        // First update the shortfall for next round.
        // If it has not seen much data for sometime and has fallen behind
        // a lot, then it is likely gone and there is no point in increasing
        // shortfall indefinitely (else it will over sample if seen in future)
        // [sumedh] Above observation does not actually hold. Two cases:
        // 1) timeInterval is defined: in this case we better keep the shortFall
        //    till the end of timeInterval when it shall be reset in any case
        // 2) timeInterval is not defined: this is a case of non-time series
        //    data where we should keep shortFall till the end of data
        /*
        if (prevReservoirSize <= reservoirSize ||
          prevShortFall <= (prevReservoirSize + numSamples)) {
          prevShortFall += (prevReservoirSize - numSamples)
        }
        */
        prevShortFall += (prevReservoirSize - numSamples)
      }
    }
  }

  private val fixedSizeInBytes = Sizeable.PER_OBJECT_OVERHEAD * 2 // totalSamples + prevShortFall

  override def getSizeInBytes: Int = super.getSizeInBytes() + fixedSizeInBytes

  override def toData(out: DataOutput): Unit = {
    super.toData(out)
    out.writeInt(totalSamples)
    out.writeInt(prevShortFall)
  }

  override def fromData (in: DataInput): Unit = {
    super.fromData(in)
    totalSamples = in.readInt()
    prevShortFall = in.readInt()
  }
}


