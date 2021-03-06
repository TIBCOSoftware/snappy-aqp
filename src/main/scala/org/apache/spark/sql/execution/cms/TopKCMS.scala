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
package org.apache.spark.sql.execution.cms

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.sql.collection.BoundedSortedSet
import CountMinSketch._
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.sql.execution.Approximate
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import java.io.IOException

final class TopKCMS[T: ClassTag](val topKActual: Int, val topKInternal: Int, depth: Int,
    width: Int, seed: Int, eps: Double, confidence: Double, size: Long, table: Array[Array[Long]],
    hashA: Array[Long], val topkSet: BoundedSortedSet[T, java.lang.Long])
    extends CountMinSketch[T](depth, width, seed, eps, confidence, size, table, hashA) {

  // val topkSet: BoundedSortedSet[T, java.lang.Long] =
  //   new BoundedSortedSet[T, java.lang.Long](topKInternal, true)

  def this(topKActual: Int, topKInternal: Int, depth: Int, width: Int, seed: Int) =
    this(topKActual, topKInternal, depth, width, seed,
      CountMinSketch.initEPS(width), CountMinSketch.initConfidence(depth), 0,
      CountMinSketch.initTable(depth, width), CountMinSketch.initHash(depth, seed),
      new BoundedSortedSet[T, java.lang.Long](topKInternal, true)
    )

  def this(topKActual: Int, topKInternal: Int, depth: Int, width: Int, hashA: Array[Long],
      confidence: Double, eps: Double) = this(topKActual, topKInternal, depth, width, 0,
    eps, confidence, 0, CountMinSketch.initTable(depth, width), hashA,
    new BoundedSortedSet[T, java.lang.Long](topKInternal, true))

  def this(topKActual: Int, topKInternal: Int, epsOfTotalCount: Double, confidence: Double,
      seed: Int) = {
    this(topKActual, topKInternal, CountMinSketch.initDepth(confidence),
      CountMinSketch.initWidth(epsOfTotalCount), seed, epsOfTotalCount, confidence, 0,
      CountMinSketch.initTable(CountMinSketch.initDepth(confidence),
        CountMinSketch.initWidth(epsOfTotalCount)),
      CountMinSketch.initHash(CountMinSketch.initDepth(confidence), seed),
      new BoundedSortedSet[T, java.lang.Long](topKInternal, true))
  }

  def this(topKActual: Int, topKInternal: Int, depth: Int, width: Int, size: Long,
      hashA: Array[Long], table: Array[Array[Long]], confidence: Double, eps: Double) =
    this(topKActual, topKInternal, depth, width, 0, eps, confidence,
      size, table, hashA, new BoundedSortedSet[T, java.lang.Long](topKInternal, true))

  override def add(item: T, count: Long): Long = {
    val totalCount = super.add(item, count)
    val prevValInTopK = this.topkSet.get(item)
    val valueToPut = if (prevValInTopK != null) {
      count + prevValInTopK
    } else {
      totalCount
    }
    this.topkSet.add(item -> valueToPut)
    totalCount
  }

  private def populateTopK(element: (T, java.lang.Long)) {
    this.topkSet.add(element)
  }

  def getFromTopKMap(key: T): Option[Approximate] = {
    val count = this.topkSet.get(key)
    val approx = if (count != null) {
      this.wrapAsApproximate(count)
    } else {
      null
    }
    Option(approx)
  }

  def getTopK: Array[(T, Approximate)] = {
    val size = if (this.topkSet.size() > this.topKActual) {
      this.topKActual
    } else {
      this.topkSet.size
    }
    val iter = this.topkSet.iterator()
    Array.fill[(T, Approximate)](size)({
      val (key, value) = iter.next
      (key, this.wrapAsApproximate(value))

    })
  }

  def getTopKKeys: OpenHashSet[T] = {
    val size = if (this.topkSet.size() > this.topKActual) {
      this.topKActual
    } else {
      this.topkSet.size
    }
    val iter = this.topkSet.iterator()
    val result = new OpenHashSet[T](size)
    while (iter.hasNext) {
      result.add(iter.next._1)
    }
    result
  }

  def getTopKeys: mutable.Set[T] = {
    val size = if (this.topkSet.size() > this.topKActual) {
      this.topKActual
    } else {
      this.topkSet.size
    }
    val set = new mutable.LinkedHashSet[T]()
    val iter = this.topkSet.iterator()
    0 until size foreach { _ =>
      set += iter.next()._1
    }
    set
  }
}

object TopKCMS {
  /**
   * Merges count min sketches to produce a count min sketch for their combined streams
   *
   * @param estimators
   * @return merged estimator or null if no estimators were provided
   * @throws CMSMergeException if estimators are not mergeable (same depth, width and seed)
   */
  @throws(classOf[CountMinSketch.CMSMergeException])
  def merge[T: ClassTag](bound: Int, estimators: Array[CountMinSketch[T]]): CountMinSketch[T] = {
    val (depth, width, hashA, table, size) = CountMinSketch.basicMerge[T](estimators: _*)
    // add all the top K keys of all the estimators
    val seqOfEstimators = estimators.toSeq
    val topkKeys = getCombinedTopKFromEstimators(seqOfEstimators,
      getUnionedTopKKeysFromEstimators(seqOfEstimators))
    val mergedTopK = new TopKCMS[T](estimators(0).asInstanceOf[TopKCMS[T]].topKActual,
        bound, depth, width, size, hashA, table, estimators(0).confidence, estimators(0).eps)
    topkKeys.foreach(x => mergedTopK.populateTopK(x._1 -> x._2.estimate))
    mergedTopK
  }

  def getUnionedTopKKeysFromEstimators[T](estimators: Seq[CountMinSketch[T]]): mutable.Set[T] = {
    val topkKeys = scala.collection.mutable.HashSet[T]()
    estimators.foreach { x =>
      val topkCMS = x.asInstanceOf[TopKCMS[T]]
      val iter = topkCMS.topkSet.iterator()
      while (iter.hasNext) {
        topkKeys += iter.next()._1
      }
    }
    topkKeys
  }

  def getCombinedTopKFromEstimators[T](estimators: Seq[CountMinSketch[T]],
    topKKeys: Iterable[T], topKKeyValMap: mutable.Map[T, Approximate] = null):
    mutable.Map[T, Approximate] = {

    val topkKeysVals = if (topKKeyValMap == null) {
      scala.collection.mutable.HashMap[T, Approximate]()
    } else {
      topKKeyValMap
    }
    estimators.foreach { x =>
      val topkCMS = x.asInstanceOf[TopKCMS[T]]
      topKKeys.foreach { key =>
        val temp = topkCMS.topkSet.get(key)
        val count = if (temp != null) {
          topkCMS.wrapAsApproximate(temp.asInstanceOf[Long])
        } else {
          x.estimateCountAsApproximate(key)
        }
        val prevCount = topkKeysVals.getOrElse[Approximate](key,
            Approximate.zeroApproximate(topkCMS.confidence))
        topkKeysVals += (key -> (prevCount + count))
      }
    }
    topkKeysVals
  }

   def deserialize[T: ClassTag](data: Array[Byte]): TopKCMS[T] = {
     val bis: ByteArrayInputStream = new ByteArrayInputStream(data);
     val s: DataInputStream = new DataInputStream(bis);
     val topKActual = s.readInt
     val topKInternal = s.readInt
     val topKSet = BoundedSortedSet.deserialize(s)
     val(size, depth, width, eps, confidence, hashA, table) = CountMinSketch.read(s)
     new TopKCMS(topKActual, topKInternal, depth, width, 0, eps,
       confidence, size, table, hashA, topKSet.asInstanceOf[BoundedSortedSet[T, java.lang.Long]])
   }

   def serialize(sketch: TopKCMS[_]): Array[Byte] = {
    val bos = new ByteArrayOutputStream();
    val s = new DataOutputStream(bos);
    try {
      s.writeInt(sketch.topKActual)
      s.writeInt(sketch.topKInternal)
      sketch.topkSet.serialize(s);
      CountMinSketch.write(sketch, s)
      return bos.toByteArray();
    } catch {
      // Shouldn't happen
      case ex: IOException => throw new RuntimeException(ex)
    }
  }
}
