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

import java.lang

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

import com.gemstone.gemfire.internal.cache.{BucketRegion, PartitionedRegion, RegionEntry}
import com.pivotal.gemfirexd.internal.engine.store.RegionEntryUtils
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation

import org.apache.spark.sql.Row
import org.apache.spark.sql.collection._
import org.apache.spark.sql.types._

/**
  * Created by vivekb on 21/10/16.
  */
class ReservoirRegionSegmentMap[V: ClassTag](val _reservoirRegion: PartitionedRegion,
                                       val _columns: Array[Int], val _types: Array[DataType],
                                       val _numColumns: Int,
                                       qcsColHandler: Option[MultiColumnOpenHashSet.ColumnHandler],
                                       val _segi: Int,
                                       val _nsegs: Int, _sampleIsPartitioned: Boolean)
  extends SegmentMap[Row, V] with Serializable {

  // Keep last column for bucket id
  private val emptyRow = new ReusableRow(_types.toSeq :+ IntegerType)

  private val redundancyOn = _reservoirRegion.getRedundantCopies > 0

  private val pendingRegionPuts = new mutable.OpenHashMap[Row, V]

  private var hasCreates = false

  private def getBucketsMappedToSegment: mutable.Set[Int] = {
    val buckets = this._reservoirRegion.getDataStore
      .getAllLocalPrimaryBucketRegions.asScala
    for (buck <- buckets if buck.getId % _nsegs == _segi) yield {
      buck.getId
    }
  }

  /*
  def addBuckets(buffer: scala.collection.mutable.Set[Int]): scala.collection.mutable.Set[Int] = {
    buffer ++= mappedBuckets.getOrElse(getBucketsMappedToSegment)
    buffer
  }
  */


  override def foldValues[U](init: U, f: (Int, V, U) => U, reset: Boolean = false): U = {
    var v = init
    val bucketArr = _reservoirRegion.getRegionAdvisor.getProxyBucketArray
    bucketArr.foreach(pbr => {
      val bucketId = pbr.getBucketId
      if (bucketId % _nsegs == _segi && pbr.isPrimary) {
        val owner = pbr.getHostedBucketRegion
        if (owner != null && owner.isInitialized) {
          val itr = owner.getBestLocalIterator(true)
          if (itr ne null) {
            val bid = if (_sampleIsPartitioned) bucketId else -1
            executeInReadLock(owner, {
              while (itr.hasNext) {
                val rl = itr.next().asInstanceOf[RowLocation]
                val c = RegionEntryUtils.getValueWithoutFaultInOrOffHeapEntry(owner,
                  rl).asInstanceOf[V]
                v = f(bid, c, v)
                // TODO why do we want to update region for readFoldValues
                if (redundancyOn && reset) {
                  _reservoirRegion.put(rl.getKey, c)
                }
              }
            }, redundancyOn)
          }
        }
      }
    })
    v
  }

  override def clearBucket(): Unit = {
    val dataStore = _reservoirRegion.getDataStore
    getBucketsMappedToSegment.foreach (dataStore.
      getCreatedBucketForId(null, _).clear())
  }

  override def update(k: Row, hash: Int, v: V): Boolean =
    throw new IllegalStateException(s"RegionSegmentMap: unsupported operation." +
      s"Need to be implemented.")

  override def size: Int =
    throw new IllegalStateException(s"RegionSegmentMap: unsupported operation." +
      s"Need to be implemented.")

  override def apply(k: Row, hash: Int): V =
    throw new IllegalStateException(s"RegionSegmentMap: unsupported operation." +
      s"Need to be implemented.")

  override def contains(k: Row, hash: Int): Boolean =
    throw new IllegalStateException(s"RegionSegmentMap: unsupported operation." +
      s"Need to be implemented.")

  override def iterator: Iterator[(Row, V)] =
    throw new IllegalStateException(s"RegionSegmentMap: unsupported operation." +
      s"Need to be implemented.")

  override def isEmpty: Boolean =
    throw new IllegalStateException(s"RegionSegmentMap: unsupported operation." +
      s"Need to be implemented.")

  def currentRow(newRow: ReusableRow, row: Row, bucketId: Int): ReusableRow = {
    val cols = this._columns
    val ncols = this._numColumns
    val types = this._types

    newRow.setInt(ncols, bucketId)

    var i = 0
    while (i < ncols) {
      val col = cols(i)
      types(i) match {
        case LongType => newRow.setLong(i, row.getLong(col))
        case IntegerType => newRow.setInt(i, row.getInt(col))
        case DoubleType => newRow.setDouble(i, row.getDouble(col))
        case FloatType => newRow.setFloat(i, row.getFloat(col))
        case BooleanType => newRow.setBoolean(i, row.getBoolean(col))
        case ByteType => newRow.setByte(i, row.getByte(col))
        case ShortType => newRow.setShort(i, row.getShort(col))
        case DateType => newRow.setInt(i, row.getInt(col))
        case _ => newRow.update(i, row.get(col))
      }
      i += 1
    }
    newRow
  }

  override def changeValue(r: Row, bucketId: Int, change: ChangeValue[Row, V],
                           isLocal: Boolean): lang.Boolean = {

    val key = qcsColHandler match {
      case Some(x) => x match {
        case q: QCSSQLColumnHandler =>
          q.extractFromRowAndExecuteFunction[ReusableRow](p => {
            currentRow(emptyRow, p, bucketId)
          }, r)
        case _ => currentRow(emptyRow, r, bucketId)
      }
      case None => currentRow(emptyRow, r, bucketId)
    }
    val bucketArr = _reservoirRegion.getRegionAdvisor.getProxyBucketArray
    val br = bucketArr(bucketId).getHostedBucketRegion
    var entry: RegionEntry = null
    // first check in pending map
    var v = pendingRegionPuts.get(key) match {
      case Some(value) => value
      case None => null.asInstanceOf[V]
    }
    val notInPending = v == null
    if (notInPending) {
      entry = br.getRegionEntry(key)
    }
    executeInReadLock(br, {
      if (notInPending && entry == null) {

        val new_v = change.defaultValue(r)
        if (new_v != null) {
          pendingRegionPuts.put(key.copy(), new_v)
          hasCreates = true
          java.lang.Boolean.TRUE
        } else null
      } else {
        val (updated_v, isNull, isUpdated) = if (notInPending) {
          // synchronize on entry to avoid changing during region puts
          entry.synchronized {
            v = entry.getValueOffHeapOrDiskWithoutFaultIn(br).asInstanceOf[V]
            change.mergeValueNoNull(r, v)
          }
        } else {
          change.mergeValueNoNull(r, v)
        }

        if (notInPending && isUpdated &&
            (redundancyOn || (_sampleIsPartitioned && !isLocal))) {
          pendingRegionPuts.put(key.copy(), updated_v)
        }
        if (!isNull) {
          java.lang.Boolean.FALSE
        } else null
      }
    }
      , redundancyOn)
  }

  override def beforeSegmentEnd(): AnyRef = {
    if (pendingRegionPuts.isEmpty) null
    else if (hasCreates) {
      // if there are creates, then do the region ops within segment lock
      _reservoirRegion.putAll(pendingRegionPuts.asJava)
      hasCreates = false
      pendingRegionPuts.clear()
      null
    } else {
      // get a copy of the map of pending puts;
      // the pending map can only be cleared after region puts are done
      pendingRegionPuts.clone().asJava
    }
  }

  override def segmentEnd(puts: AnyRef): Unit = {
    if (puts ne null) {
      // region puts should be outside the segment lock to avoid blocking others
      _reservoirRegion.putAll(puts.asInstanceOf[java.util.Map[_, _]])
      // clear pending puts once region puts are done
      val lock = writeLock()
      lock.lock()
      try {
        pendingRegionPuts.clear()
      } finally {
        lock.unlock()
      }
    }
  }

  def executeInReadLock[T](br: BucketRegion, body: => T, doLock: Boolean): T = {
    if (doLock) {
      br.columnBatchFlushLock.readLock.lock()
      try {
        body
      } finally {
        br.columnBatchFlushLock.readLock.unlock()
      }
    } else body
  }

  def executeInWriteLock[T](br: BucketRegion, body: => T, doLock: Boolean): T = {
    if (doLock) {
      br.columnBatchFlushLock.writeLock.lock()
      try {
        body
      } finally {
        br.columnBatchFlushLock.writeLock.unlock()
      }
    } else body
  }

  override def foldEntries[U](init: U, copyIfRequired: Boolean, f: (Row, V, U) => U): U =
    throw new IllegalStateException(s"RegionSegmentMap: unsupported operation." +
      s"Need to be implemented.")

  override def valuesIterator: Iterator[V] =
    throw new IllegalStateException(s"RegionSegmentMap: unsupported operation." +
      s"Need to be implemented.")

}
