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

import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.gemstone.gnu.trove.TIntArrayList
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store.RegionEntryUtils
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.collection._
import org.apache.spark.sql.types.DataType

/**
  * Created by vivekb on 14/10/16.
  */
protected class StratifiedSamplerCachedInRegion(_reservoirRegion: PartitionedRegion,
  _options: SampleOptions, _sampleIsPartitioned: Boolean)
    extends StratifiedSamplerCached(_options) {
  private val numBuckets = _reservoirRegion.getPartitionAttributes.getTotalNumBuckets

  protected override def getReservoirSegment(newQcs: Array[Int], types: Array[DataType],
  numColumns: Int, initialCapacity: Int, loadFactor: Double,
  qcsColHandler: Option[MultiColumnOpenHashSet.ColumnHandler],
  segi: Int, nsegs: Int): ReservoirSegment =
    new ReservoirRegionSegmentMap[StratumReservoir](_reservoirRegion,
      newQcs, types, numColumns, qcsColHandler, segi, nsegs, _sampleIsPartitioned)

  private var markAsFlushed: Boolean = false

  override def flushReservoir[U](init: U, process: (U, InternalRow) => U,
      startBatch: (U, Int) => U, endBatch: U => U): U = {
    markAsFlushed = true
    super.flushReservoir[U](init: U, process: (U, InternalRow) => U,
      startBatch: (U, Int) => U, endBatch: U => U)
  }

  override def onTruncate(): Unit = {
    this._reservoirRegion.clearLocalPrimaries()
  }

  override def append[U](rows: Iterator[Row],
      init: U, processFlush: (U, InternalRow) => U, startBatch: (U, Int) => U, endBatch: U => U,
      rowEncoder: ExpressionEncoder[Row], partIndex: Int): Long = {
    markAsFlushed = false
    super.append[U](rows: Iterator[Row],
      init: U, processFlush: (U, InternalRow) => U, startBatch: (U, Int) => U, endBatch: U => U,
      rowEncoder: ExpressionEncoder[Row], partIndex: Int)
  }

  override def iteratorOnRegion(buckets: java.util.Set[Integer]): Iterator[InternalRow] = {
    if (strata.isEmpty || markAsFlushed) {
      Iterator.empty
    } else {
      val bitr = Misc.getLocalBucketsIteratorForSampleTable(_reservoirRegion,
        buckets, true)
      if (bitr != null && bitr.hasNext) {
        val first = bitr.next().asInstanceOf[RowLocation]
        val stratumCache = RegionEntryUtils.getValueWithoutFaultInOrOffHeapEntry(bitr.
          getHostedBucketRegion, first).asInstanceOf[StratumCache]
        var iter: Iterator[InternalRow] = stratumCache.iterator(0, stratumCache.reservoirSize,
          schema.length, false, false, true)
        while (bitr.hasNext) {
          val rl = bitr.next().asInstanceOf[RowLocation]
          val owner = bitr.getHostedBucketRegion
          val c = RegionEntryUtils.getValueWithoutFaultInOrOffHeapEntry(owner,
            rl).asInstanceOf[StratumCache]
          iter = iter ++ c.iterator(0, c.reservoirSize, schema.length, false, false, true)
        }
        iter
      } else {
        Iterator.empty
      }
    }
  }

  override protected def isBucketLocal(partIndex: Int): Boolean = {
    if (partIndex > -1) {
      _reservoirRegion.getRegionAdvisor.isBucketLocal(partIndex)
    } else false
  }

  override protected def getPrimaryBucketArray(partIndex: Int): TIntArrayList = if (partIndex ==
      -1) { _reservoirRegion.getDataStore.getAllLocalPrimaryBucketIdArray} else null

  override protected def getBucketId(partIndex: Int,
      primaryBucketIds: TIntArrayList = null)(hashValue: Int): Int = {
    if (partIndex > -1 || primaryBucketIds == null || primaryBucketIds.size() == 0) {
      partIndex
    } else {
      val i: Int = hashValue.abs % primaryBucketIds.size()
      primaryBucketIds.getQuick(i)
    }
  }

  override def reservoirInRegion: Boolean = true
}

