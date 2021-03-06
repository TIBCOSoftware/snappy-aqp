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

import scala.collection.mutable.{ArrayBuffer, ListBuffer, MutableList, Stack}
import scala.reflect.ClassTag
import scala.util.Random

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import io.snappydata.util.NumberUtils

import org.apache.spark.sql.TimeEpoch
import org.apache.spark.sql.execution.cms.CountMinSketch

/**
 * Implements the algorithms and data structures from "Hokusai -- Sketching
 * Streams in Real Time", by Sergiy Matusevych, Alexander Smola, Amr Ahmed.
 * http://www.auai.org/uai2012/papers/231.pdf
 *
 * Aggregates state, so this is a mutable class.
 *
 * Since we are all still learning scala, I thought I'd explain the use of
 * implicits in this file.   TimeAggregation takes an implicit constructor
 * parameter:
 *    TimeAggregation[T]()(implicit val cmsMonoid: CMSMonoid[T])
 * The purpose for that is:
 * + In Algebird, a CMSMonoid[T] is a factory for creating instances of CMS[T]
 * + TimeAggregation needs to occasionally make new CMS instances, so it will
 *   use the factory
 * + By making it an implicit (and in the curried param), the outer context of
 *   the TimeAggregation can create/ensure that the factory is there.
 * + Hokusai[T] will be the "outer context" so it can handle that for
 *   TimeAggregation
 *
 *
 * TODO
 * 1. Decide if the underlying CMS should be mutable (save memory) or
 *    functional (algebird) I'm afraid that with the functional approach,
 *    and having so many, every time we merge two CMS, we create a third
 *    and that is wasteful of memory or may take too much memory. If we go
 *    with a mutable CMS, we have to either make stream-lib's serializable,
 *    or make our own.
 *
 * 2. Clean up intrusion of algebird shenanigans in the code (implicit
 *    factories etc)
 *
 * 3. Decide on API for managing data and time.  Do we increment time in a
 *    separate operation or add a time parameter to addData()?
 *
 * 4. Decide if we want to be mutable or functional in this datastruct.
 *    Current version is mutable.
 */
class Hokusai[T: ClassTag](val cmsParams: CMSParams, val windowSize: Long, val epoch0: Long,
  taList: MutableList[CountMinSketch[T]],
  itemList: MutableList[CountMinSketch[T]],
  intervalTracker: IntervalTracker,
  intitalInterval: Long,
  mBarInitializer : Option[CountMinSketch[T]]
                            ) {
  val mergeCreator: Array[CountMinSketch[T]] => CountMinSketch[T] =
    estimators => CountMinSketch.merge[T](estimators: _*)

  val timeEpoch = new TimeEpoch(windowSize, epoch0, intitalInterval)

  val taPlusIa = new TimeAndItemAggregation(taList, itemList, intervalTracker)
  // Current data accummulates into mBar until the next time tick

  var mBar: CountMinSketch[T] = mBarInitializer.getOrElse(createZeroCMS(0))

  private val queryTillLastN_Case2: (T, Int) => Option[Approximate] =
    (item: T, sumUpTo: Int) => Some(this.taPlusIa.queryBySummingTimeAggregates(item, sumUpTo))

  private val queryTillLastN_Case1: (T) => () => Option[Approximate] =
    (item: T) => () => Some(this.taPlusIa.ta.aggregates(0).estimateCountAsApproximate(item))

  private val queryTillLastN_Case3: (T, Int, Int, Int, Int) => Option[Approximate] = (item: T,
      lastNIntervals: Int, totalIntervals: Int, n: Int, nQueried: Int) =>
    if (lastNIntervals > totalIntervals) {
      Some(this.taPlusIa.queryBySummingTimeAggregates(item, n))
    } else {
      val nearestPowerOf2Num = NumberUtils.nearestPowerOf2LE(lastNIntervals)
      var count = this.taPlusIa.queryBySummingTimeAggregates(item,
        NumberUtils.isPowerOf2(nearestPowerOf2Num))
      // the remaining interval will lie in the time interval range
      val lengthOfLastInterval = nearestPowerOf2Num
      val residualIntervals = lastNIntervals - nearestPowerOf2Num
      if (false /* residualIntervals > (lengthOfLastInterval * 3) / 4 */) {
        // it would be better to find the time aggregation of last interval - the other intervals)
        count += this.taPlusIa.queryTimeAggregateForInterval(item, lengthOfLastInterval)
        count -= this.taPlusIa.basicQuery(lastNIntervals + 1 to (2 * nearestPowerOf2Num)
            .asInstanceOf[Int], item, nearestPowerOf2Num.asInstanceOf[Int],
          nearestPowerOf2Num.asInstanceOf[Int] * 2)
      } else {
        count += this.taPlusIa.basicQuery(nearestPowerOf2Num.asInstanceOf[Int] + 1 to
            lastNIntervals, item,
          nearestPowerOf2Num.asInstanceOf[Int],
          nearestPowerOf2Num.asInstanceOf[Int] * 2)
      }

      Some(count)
    }

  private val queryTillLastN_Case4: (T, Int, Int, Int, Int) => Option[Approximate] = (item: T,
      lastNIntervals: Int, totalIntervals: Int, n: Int, nQueried: Int) => {
    val lastNIntervalsToQuery = if (lastNIntervals > totalIntervals) {
      totalIntervals
    } else {
      lastNIntervals
    }

    val (bestPath, computedIntervalLength) = this.taPlusIa.intervalTracker.identifyBestPath(
      lastNIntervalsToQuery, encompassLastInterval = true)

    val skipLastInterval = if (computedIntervalLength > lastNIntervalsToQuery) {
      // this means that last one or many intervals lie within a time interval range
      // drop  the last interval from the count of  best path as it needs to be handled separately
      bestPath.last
    } else {
      -1
    }
    val zeroApprox = Approximate.zeroApproximate(cmsParams.confidence)
    var total = bestPath.aggregate[Approximate](zeroApprox)((partTotal, interval) => {
      partTotal +
        (if (interval != skipLastInterval) {
          this.taPlusIa.queryTimeAggregateForInterval(item, interval)
        } else {
          Approximate.zeroApproximate(cmsParams.confidence)
        })
    }, _ + _)

    // Add the first item representing interval 1
    total += this.taPlusIa.ta.aggregates(0).estimateCountAsApproximate(item)

    if (computedIntervalLength > lastNIntervalsToQuery) {
      // start individual query from interval
      // The accuracy becomes very poor if we query the first interval 1 using entity
      // aggregates . So subtraction approach needs to be looked at more carefully
      val residualLength = lastNIntervalsToQuery - (computedIntervalLength - skipLastInterval)
      if (false /* residualLength > (skipLastInterval * 3) / 4 */) {
        // it will be better to add the whole time aggregate & substract the other intervals
        total += this.taPlusIa.queryTimeAggregateForInterval(item, skipLastInterval)
        val begin = (lastNIntervals + 1).asInstanceOf[Int]
        val end = computedIntervalLength.asInstanceOf[Int]
        total -= this.taPlusIa.basicQuery(begin to end,
          item, skipLastInterval.asInstanceOf[Int], computedIntervalLength.asInstanceOf[Int])

      } else {
        val begin = (computedIntervalLength - skipLastInterval + 1).asInstanceOf[Int]

        total += this.taPlusIa.basicQuery(begin to lastNIntervalsToQuery,
          item, skipLastInterval.asInstanceOf[Int], computedIntervalLength.asInstanceOf[Int])
      }
    }
    Some(total)
  }

  def increment(): Unit = {
    timeEpoch.increment()
    this.taPlusIa.increment(mBar, timeEpoch.t)
    mBar = createZeroCMS(0)
  }

  // For testing.  This follows spark one update per batch model?  But
  // we may need to control for time a bit more carefully?
  def addEpochData(data: Seq[T]): Unit = {
    accummulate(data)
  }

  def addEpochData(data: scala.collection.Map[T, Long], time: Long): Unit = {
    accummulate[(T, Long)](data.toIterable, e => (e._1, e._2, time))
  }

  def addEpochData(data: scala.collection.Map[T, Long]): Unit = {
    accummulate(data)
  }

  def addTimestampedData(data: ArrayBuffer[KeyFrequencyWithTimestamp[T]]): Unit = {
    accummulate[KeyFrequencyWithTimestamp[T]](data, kft => (kft.key, kft.frequency, kft.epoch))
  }

  // Get the frequency estimate for key in epoch from the CMS
  // If there is no data for epoch, returns None
  /* def query(epoch: Long, key: T): Option[Long] =
    // TODO I don't like passing numIntervals in .....
    timeEpoch.jForTimestamp(epoch, numIntervals).flatMap(ta.query(_, key)) */

  def queryTillTime(epoch: Long, key: T): Option[Approximate] = {

    this.timeEpoch.timestampToInterval(epoch).flatMap(x =>

      if (x > timeEpoch.t) {
        None
      } else {
        this.queryTillLastNIntervals(
          this.taPlusIa.convertIntervalBySwappingEnds(x.asInstanceOf[Int]).asInstanceOf[Int],
          key)
      })

  }

  def queryAtTime(epoch: Long, key: T): Option[Approximate] = {

    this.timeEpoch.timestampToInterval(epoch).flatMap(x =>
      if (x > timeEpoch.t) {
        // cannot query the current mBar
        None
      } else {
        this.taPlusIa.queryAtInterval(this.taPlusIa.convertIntervalBySwappingEnds(x)
            .asInstanceOf[Int], key)
      })

  }

  def queryBetweenTime(epochFrom: Long, epochTo: Long, key: T): Option[Approximate] = {

    val (later, earlier) = this.timeEpoch.convertEpochToIntervals(epochFrom, epochTo) match {
      case Some(x) => x
      case None => return None
    }
    this.taPlusIa.queryBetweenIntervals(later, earlier, key)

  }

  // def accummulate(data: Seq[Long]): Unit = mBar = mBar ++ cmsMonoid.create(data)
  def accummulate(data: Seq[T]): Unit =
    data.foreach(i => mBar.add(i, 1L))

  def accummulate(data: scala.collection.Map[T, Long]): Unit =

    data.foreach { case (item, count) => mBar.add(item, count) }

  def accummulate[K](data: Iterable[K], extractor: K => (T, Long, Long)): Unit =
    {

      val iaAggs = this.taPlusIa.ia.aggregates
      val taAggs = this.taPlusIa.ta.aggregates
      data.foreach { v =>
        val (key, frequency, epoch) = extractor(v)
        if (epoch > 0 && windowSize != Long.MaxValue) {
          // find the appropriate time and item aggregates and update them
          timeEpoch.timestampToInterval(epoch) match {
            case Some(interval) =>
              // first check if it lies in current mBar

              if (interval > timeEpoch.t) {
                // check if new slot has to be created
                if (interval > (timeEpoch.t + 1)) {
                  /* if (interval > (timeEpoch.t + 2)) {
                    println(s"WARNING: got time stamp = $epoch " +
                        s"more than twice beyond current")
                  } */
                  val incrementAmount = interval - timeEpoch.t.asInstanceOf[Int] - 1
                  0 until incrementAmount foreach { _ =>
                    increment()
                  }
                }
                mBar.add(key, frequency)
              } else {
                // we have to update CMS in past
                // identify all the intervals which store data for this interval
                if (interval == timeEpoch.t) {
                  taAggs(0).add(key, frequency)
                  if (!taAggs(0).eq(iaAggs(interval - 1))) {
                    iaAggs(interval - 1).add(key, frequency)
                  }
                } else {
                  val timeIntervalsToModify = this.taPlusIa.intervalTracker
                      .identifyIntervalsContaining(this.taPlusIa.convertIntervalBySwappingEnds(
                        interval))
                  timeIntervalsToModify.foreach { x =>
                    taAggs(NumberUtils.isPowerOf2(x) + 1)
                      .add(key, frequency)
                  }
                  if (!(timeIntervalsToModify(0) == 1 && taAggs(1).eq(iaAggs(interval - 1)))) {
                    iaAggs(interval - 1).add(key, frequency)
                  }
                }

              }
            case None =>
              // println(s"WARNING: got epoch=$epoch with " +  s"epoch0 as ${timeEpoch.epoch0}")
          }
        } else {
          mBar.add(key, frequency)
        }
      }
    }

  def queryTillLastNIntervals(lastNIntervals: Int, item: T): Option[Approximate] =
    this.taPlusIa.queryLastNIntervals[Option[Approximate]](lastNIntervals,
      queryTillLastN_Case1(item),
      queryTillLastN_Case2(item, _),
      queryTillLastN_Case3(item, _, _, _, _),
      queryTillLastN_Case4(item, _, _, _, _))

  def createZeroCMS(intervalFromLast: Int): CountMinSketch[T] =
    Hokusai.newZeroCMS[T](cmsParams.depth, cmsParams.width, cmsParams.hashA, cmsParams.confidence,
      cmsParams.eps)

  override def toString: String = s"Hokusai[ta=${taPlusIa}, mBar=${mBar}]"

  class ItemAggregation(val aggregates: MutableList[CountMinSketch[T]]) {
    // val aggregates = new MutableList[CountMinSketch[T]]()

    def increment(cms: CountMinSketch[T], t: Long): Unit = {
      // TODO :Asif : Take care of long being casted to int
      // Since the indexes in aggregates are 0 based, A^1 is obtained by A(0)
      aggregates += cms
      for (k <- 1 to math.floor(Hokusai.log2X(t)).asInstanceOf[Int]) {
        val compressIndex = t.asInstanceOf[Int] - math.pow(2, k).asInstanceOf[Int] - 1
        val compressCMS = aggregates(compressIndex)
        if (compressCMS.width > 512) {
          this.aggregates.update(compressIndex, compressCMS.compress)
        }
      }
    }

  }

  /**
   * Data Structures and Algorithms to maintain Time Aggregation from the paper.
   * Time is modeled as a non-decreasing integer starting at 0.
   *
   * The type parameter, T, is the key type.  This needs to be numeric.  Typical
   * value is Long, but Short can cut down on size of resulting data structure,
   * but increased chance of collision.  BigInt is another possibility.
   *
   *
   * From Theorem 4 in the Paper:
   *
   * At time t, the sketch M^j contains statistics for the period
   * [t-delta, t-delta-2^j] where delta = t mod 2^j
   *
   * The following shows an example of how data ages through m() as t
   * starts at 0 and increases:
   *
   * {{{
   *    === t = 0
   *      t=0  j=0 m is EMPTY
   *    === t = 1
   *      t=1  j=0 m(0)=[0, 1) # secs in m(0): 1
   *    === t = 2
   *      t=2  j=0 m(0)=[1, 2) # secs in m(0): 1
   *      t=2  j=1 m(1)=[0, 2) # secs in m(1): 2
   *    === t = 3
   *      t=3  j=0 m(0)=[2, 3) # secs in m(0): 1
   *      t=3  j=1 m(1)=[0, 2) # secs in m(1): 2
   *    === t = 4
   *      t=4  j=0 m(0)=[3, 4) # secs in m(0): 1
   *      t=4  j=1 m(1)=[2, 4) # secs in m(1): 2
   *      t=4  j=2 m(2)=[0, 4) # secs in m(2): 4
   *    === t = 5
   *      t=5  j=0 m(0)=[4, 5) # secs in m(0): 1
   *      t=5  j=1 m(1)=[2, 4) # secs in m(1): 2
   *      t=5  j=2 m(2)=[0, 4) # secs in m(2): 4
   *    === t = 6
   *      t=6  j=0 m(0)=[5, 6) # secs in m(0): 1
   *      t=6  j=1 m(1)=[4, 6) # secs in m(1): 2
   *      t=6  j=2 m(2)=[0, 4) # secs in m(2): 4
   *    === t = 7
   *      t=7  j=0 m(0)=[6, 7) # secs in m(0): 1
   *      t=7  j=1 m(1)=[4, 6) # secs in m(1): 2
   *      t=7  j=2 m(2)=[0, 4) # secs in m(2): 4
   *    === t = 8
   *      t=8  j=0 m(0)=[7, 8) # secs in m(0): 1
   *      t=8  j=1 m(1)=[6, 8) # secs in m(1): 2
   *      t=8  j=2 m(2)=[4, 8) # secs in m(2): 4
   *      t=8  j=3 m(3)=[0, 8) # secs in m(3): 8
   *    === t = 9
   *      t=9  j=0 m(0)=[8, 9) # secs in m(0): 1
   *      t=9  j=1 m(1)=[6, 8) # secs in m(1): 2
   *      t=9  j=2 m(2)=[4, 8) # secs in m(2): 4
   *      t=9  j=3 m(3)=[0, 8) # secs in m(3): 8
   * }}}
   *
   *  numIntervals The number of sketches to keep in the exponential backoff.
   *        the last one will have a sketch of all data.  Default value is 16.
   */
  class TimeAggregation(val aggregates: MutableList[CountMinSketch[T]]) {

    def query(index: Int, key: T): Option[Long] =
      this.aggregates.get(index).map(_.estimateCount(key))

    def increment(cms: CountMinSketch[T], t: Long, rangeToAggregate: Int): Unit = {
      if (t == 1 || t == 2) {
        cms +=: this.aggregates
      } else {
        val powerOf2 = NumberUtils.isPowerOf2(t.asInstanceOf[Int] - 1)
        if (powerOf2 != -1) {
          // Make a dummy  entry at the last  position
          this.aggregates += createZeroCMS(powerOf2)
        }
        var mBar = cms

        (0 to rangeToAggregate) foreach ({ j =>
          val temp = mBar
          val mj = this.aggregates.get(j) match {
            case Some(map) => map
            case None => {
              throw new IllegalStateException("The index should have had a CMS")

            }
          }
          if (j != 0) {

            mBar = mergeCreator(Array[CountMinSketch[T]](mBar, mj))
            // CountMinSketch.merge[T](mBar, mj)
          } else {
            mBar = mj
          }
          this.aggregates.update(j, temp)
        })
      }

    }

    override def toString: String =
      s"TimeAggregation[${this.aggregates.mkString("M=[", ", ", "]")}]"
  }

  class TimeAndItemAggregation(taList: MutableList[CountMinSketch[T]],
    itemList: MutableList[CountMinSketch[T]], val intervalTracker: IntervalTracker) {
    // val aggregates = new MutableList[CountMinSketch[T]]()
    val ta = new TimeAggregation(taList)
    val ia = new ItemAggregation(itemList)
    // val intervalTracker: IntervalTracker = new IntervalTracker()

    def increment(mBar: CountMinSketch[T], t: Long): Unit = {
      val rangeToAggregate = if (NumberUtils.isPowerOfTwo(t.asInstanceOf[Int])) {
        1
      } else if (NumberUtils.isPowerOfTwo(t.asInstanceOf[Int] - 1)) {
        NumberUtils.isPowerOf2(t - 1) + 1
      } else {
        intervalTracker.numSaturatedSize
      }
      ta.increment(mBar, t, rangeToAggregate)
      ia.increment(mBar, t)
      // this.basicIncrement(mBar, t, rangeToAggregate)
      this.intervalTracker.updateIntervalLinks(t)
    }

    def queryAtInterval(lastNthInterval: Int, key: T): Option[Approximate] = {
      if (lastNthInterval == 1) {
        Some(this.ta.aggregates(0).estimateCountAsApproximate(key))
      } else {
        // Identify the best path which contains the last interval
        val (bestPath, computedIntervalLength) = this.intervalTracker.identifyBestPath(
          lastNthInterval, true)
        val lastIntervalRange = bestPath.last.asInstanceOf[Int]
        if (lastIntervalRange == 1) {
          Some(ta.aggregates(1).estimateCountAsApproximate(key))
        } else {
          Some(this.basicQuery(lastNthInterval to lastNthInterval,
            key, lastIntervalRange, computedIntervalLength.asInstanceOf[Int]))
        }

      }

    }

    def queryBetweenIntervals(later: Int, earlier: Int, key: T): Option[Approximate] = {
      val fromLastNInterval = this.convertIntervalBySwappingEnds(later)
      val tillLastNInterval = this.convertIntervalBySwappingEnds(earlier)
      if (fromLastNInterval == 1 && tillLastNInterval == 1) {
        Some(this.ta.aggregates(0).estimateCountAsApproximate(key))
      } else {
        // Identify the best path
        val (bestPath, computedIntervalLength) = this.intervalTracker.identifyBestPath(
          tillLastNInterval.asInstanceOf[Int], true, 1, fromLastNInterval.asInstanceOf[Int])

        var truncatedSeq = bestPath
        var start = if (fromLastNInterval == 1) {
          fromLastNInterval + 1
        } else {
          fromLastNInterval
        }
        val zeroApprox = Approximate.zeroApproximate(cmsParams.confidence)
        var taIntervalStartsAt = computedIntervalLength -
            bestPath.aggregate[Long](0)(_ + _, _ + _) + 1
        var finalTotal = bestPath.aggregate[Approximate](zeroApprox)((total, interval) => {
          val lengthToDrop = truncatedSeq.head
          truncatedSeq = truncatedSeq.drop(1)
          val lengthTillInterval = computedIntervalLength -
              truncatedSeq.aggregate[Long](0)(_ + _, _ + _)
          val end = if (lengthTillInterval > tillLastNInterval) {
            tillLastNInterval
          } else {
            lengthTillInterval
          }

          val total1 = if (start == taIntervalStartsAt && end == lengthTillInterval) {
            // can add the time series aggregation as whole interval is needed
            ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).estimateCountAsApproximate(key)
          } else {
            basicQuery(start.asInstanceOf[Int] to end.asInstanceOf[Int],
              key, interval.asInstanceOf[Int], lengthTillInterval.asInstanceOf[Int])
          }
          start = lengthTillInterval + 1
          taIntervalStartsAt += lengthToDrop
          total + total1
        }, _ + _)

        if (fromLastNInterval == 1) {
          finalTotal += this.ta.aggregates(0).estimateCountAsApproximate(key)
        }
        Some(finalTotal)
      }

    }

    def queryLastNIntervals[B](lastNIntervals: Int, queryTillLastN_Case1: () => B,
      queryTillLastN_Case2: (Int) => B,
      queryTillLastN_Case3: (Int, Int, Int, Int) => B,
      queryTillLastN_Case4: (Int, Int, Int, Int) => B): B = {
      if (lastNIntervals == 1) {
        return queryTillLastN_Case1()
      }

      // check the total number of intervals excluding the current ( in progress).
      // The current interval counts only at the end of the current interval
      val totalIntervals = this.ia.aggregates.size
      // If total number of intervals is some power of 2, then all intervals are segregated &
      // there is no overlap
      val n = NumberUtils.isPowerOf2(totalIntervals)
      val nQueried = NumberUtils.isPowerOf2(lastNIntervals)
      if (n != -1 && nQueried != -1) {
        val sumUpTo = math.min(n, nQueried)
        // Some(queryBySummingTimeAggregates(item, sumUpTo))
        queryTillLastN_Case2(sumUpTo)
      } else if (n != -1) {
        // the total intervals are power of 2 , but the queried up to interval is not
        // In which case we find the nearest interval, which is power of 2 and sum up
        // those time aggregates & approximate the remaining using interpolation
        queryTillLastN_Case3(lastNIntervals, totalIntervals, n, nQueried)

      } else {
        queryTillLastN_Case4(lastNIntervals, totalIntervals, n, nQueried)
      }

    }

    /**
     * @param tIntervalsToQuery the tIntervals range to query  as per the time aggregates. That is
     * the interval range is the intervals in the past relative to most recent interval
     *  Note that
     * time aggregates are from right to left, that is most recent interval is at the 0th position
     *  , while item aggregates are from left to right, that is most recent interval is at the end
     */
    def basicQuery(tIntervalsToQuery: Range, key: T, taIntervalWithInstant: Int,
      totalComputedIntervalLength: Int): Approximate = {
      // handle level =1
      val totalIntervals = this.ia.aggregates.size
      val jStar = NumberUtils.isPowerOf2(taIntervalWithInstant) + 1

      val beginingOfRangeAsPerIA =
        this.convertIntervalBySwappingEnds(totalComputedIntervalLength)

      val endRangeAsPerIA = beginingOfRangeAsPerIA + taIntervalWithInstant - 1
      val mJStar = this.ta.aggregates.get(jStar).get
      var total = Approximate.zeroApproximate(cmsParams.confidence)
      var n: Array[Long] = null
      val bStart = beginingOfRangeAsPerIA.asInstanceOf[Int]
      val bEnd = endRangeAsPerIA.asInstanceOf[Int]
      val unappliedWidthHashes = this.ia.aggregates(0).getIHashesFor(key, false)
      tIntervalsToQuery foreach {
        j =>
          val intervalNumRelativeToIA = convertIntervalBySwappingEnds(j).asInstanceOf[Int]
          val cmsAtT = this.ia.aggregates.get(intervalNumRelativeToIA - 1).get
          val hashes = unappliedWidthHashes.map(_ % cmsAtT.width)
          val nTilda = calcNTilda(cmsAtT, hashes)
          val width = if (j <= 2) {
            cmsParams.width
          } else {
            cmsParams.width - Hokusai.ilog2(j - 1) + 1
          }
          total += (if (nTilda.estimate > math.E * intervalNumRelativeToIA / (1 << width)
            || nTilda.estimate == 0) {
            nTilda
          } else {
            if (n == null) {
              n = this.queryBySummingEntityAggregates(key, bStart - 1, bEnd - 1)
            }
            calcNCarat(key, jStar, cmsAtT, hashes, n, mJStar)
          })

      }
      total

    }

    /**
     * Converts the last n th interval to interval from begining
     * For example, the most recent interval is say 8th.
     * 8, 7, 6 , 5 , 4 , 3 , 2 ,1
     * last 3rd interval will be converted to 6th interval from the begining
     * The starting interval is 1 based.
     *
     * or
     * Converts the  n th interval from 1st interal onwards to interval number from the end
     * i.e lastNth interval from the most recent interval;
     * For example, the most recent interval is say 8th.
     * 8, 7, 6 , 5 , 4 , 3 , 2 ,1
     * 3rd interval will be converted to 6th interval from the end that is 6th most recent interval
     * The starting interval is 1 based.
     */
    def convertIntervalBySwappingEnds(intervalNumber: Long): Long =
      this.ia.aggregates.size - intervalNumber + 1

    def queryBySummingTimeAggregates(item: T, sumUpTo: Int): Approximate = {
      var count: Approximate = Approximate.zeroApproximate(cmsParams.confidence)
      (0 to sumUpTo) foreach {
        j => count = count + this.ta.aggregates.get(j).get.estimateCountAsApproximate(item)
      }

      count
    }

    def queryTimeAggregateForInterval(item: T, interval: Long): Approximate = {
      assert(NumberUtils.isPowerOf2(interval) != -1)
      ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).estimateCountAsApproximate(item)
    }

    private def queryBySummingEntityAggregates(item: T, startIndex: Int,
        sumUpTo: Int): Array[Long] = {
      val n = Array.ofDim[Long](cmsParams.depth)
      val unappledWidthHashes = this.ia.aggregates(0).getIHashesFor(item, false)
      (startIndex to sumUpTo) foreach {
        j =>
          {
            val cms = this.ia.aggregates.get(j).get
            val hashes = unappledWidthHashes.map(_ % cms.width)
            0 until n.length foreach { i =>
              n(i) = n(i) + cms.table(i)(hashes(i))
            }
          }
      }

      n
    }

    private def calcNTilda(cms: CountMinSketch[T],
      hashes: Array[Int]): Approximate = {
      var res = scala.Long.MaxValue;
      for (i <- 0 until cms.depth) {
        res = Math.min(res, cms.table(i)(hashes(i)));
      }

      return cms.wrapAsApproximate(res)
    }

    private def calcNCarat(key: T, jStar: Int, cmsAtT: CountMinSketch[T],
        hashesForTime: Array[Int], sumOverEntities: Array[Long],
        mJStar: CountMinSketch[T]): Approximate = {

      val mjStarHashes = mJStar.getIHashesFor(key, true)
      var res = scala.Long.MaxValue;
      var m: Long = scala.Long.MaxValue;
      var c: Long = scala.Long.MaxValue;
      var b: Long = scala.Long.MaxValue;
      // since the indexes are zero based

      for (i <- 0 until cmsAtT.depth) {
        if (sumOverEntities(i) != 0) {
          res = Math.min(res, (mJStar.table(i)(mjStarHashes(i)) *
            cmsAtT.table(i)(hashesForTime(i))) / sumOverEntities(i))
        } else {
          return Approximate.zeroApproximate(cmsParams.confidence)
        }
      }

      return if (res == scala.Long.MaxValue) {
        Approximate.zeroApproximate(cmsParams.confidence)
      } else {
        cmsAtT.wrapAsApproximate(res)
      }

    }

  }
}

// TODO Better handling of params and construction (both delta/eps and d/w support)
class CMSParams private (val width: Int, val depth: Int, val eps: Double,
  val confidence: Double, val seed: Int, val hashA: Array[Long]) extends Serializable {

  def this(width: Int, depth: Int, eps: Double,
    confidence: Double, seed: Int = 123) = this(width, depth, eps,
    confidence, seed, CMSParams.createHashA(seed, depth))

}

object CMSParams {
  def apply(eps: Double, confidence: Double): CMSParams = {
    val (width, modifiedEPS) = CountMinSketch.initWidthOfPowerOf2(eps)
    new CMSParams(width,
      CountMinSketch.initDepth(confidence), modifiedEPS, confidence)
  }

  def apply(width: Int, depth: Int): CMSParams = {
    val newWidth = NumberUtils.nearestPowerOf2GE(width)
    new CMSParams(newWidth,
      depth, CountMinSketch.initEPS(newWidth), CountMinSketch.initConfidence(depth))
  }

  def apply(eps: Double, confidence: Double, seed: Int): CMSParams = {
    val (width, modifiedEPS) = CountMinSketch.initWidthOfPowerOf2(eps)
    new CMSParams(width,
      CountMinSketch.initDepth(confidence), modifiedEPS, confidence, seed)
  }

  def apply(width: Int, depth: Int, seed: Int): CMSParams = {
    val newWidth = NumberUtils.nearestPowerOf2GE(width)
    new CMSParams(newWidth,
      depth, CountMinSketch.initEPS(width), CountMinSketch.initConfidence(depth), seed)
  }

  private def createHashA(seed: Int, depth: Int): Array[Long] = {
    val r = new Random(seed)
    Array.fill[Long](depth)(r.nextInt(Int.MaxValue))
  }

  def write(kryo: Kryo, output: Output, obj: CMSParams) {
    output.writeInt(obj.width)
    output.writeInt(obj.depth)
    output.writeDouble(obj.eps)
    output.writeDouble(obj.confidence)
    output.writeInt(obj.seed)
    output.writeInt(obj.hashA.length)
    obj.hashA.foreach(output.writeLong(_))

  }

  def read(kryo: Kryo, input: Input): CMSParams = {
    val width = input.readInt
    val depth = input.readInt
    val eps = input.readDouble
    val confidence = input.readDouble
    val seed = input.readInt
    val hashA = Array.fill[Long](input.readInt)(input.readLong)
    new CMSParams(width, depth, eps, confidence, seed, hashA)
  }

}

final class KeyFrequencyWithTimestamp[T](val key: T, val frequency: Long,
  val epoch: Long)

class IntervalTracker {
  private val unsaturatedIntervals: Stack[IntervalLink] = new Stack[IntervalLink]()
  private var head: IntervalLink = _
  // private val unsaturatedIntervals: Stack[IntervalLink] = new Stack[IntervalLink]()

  def identifyBestPath(lastNIntervals: Int,
    encompassLastInterval: Boolean = false,
    prevTotal: Long = 1,
    startingFromInterval: Int = 1): (Seq[Long], Long) = {
    // The first 1 interval is outside the LinkedInterval but will always be there separate
    // But the computed interval length includes the 1st interval
    this.head.identifyBestPath(lastNIntervals, prevTotal, encompassLastInterval,
      startingFromInterval)
  }

  def identifyIntervalsContaining(interval: Long): Seq[Long] = {
    // The first 1 interval is outside the LinkedInterval but will always be there separate
    // But the computed interval length includes the 1st interval
    this.head.identifyIntervalsContaining(interval)
  }

  def numSaturatedSize: Int = {
    var len = 0

    var start = this.unsaturatedIntervals.top
    while (start != null) {
      len += 1
      start = start.prevLink
    }
    len
  }

  def updateIntervalLinks(t: Long): Unit = {
    if (t > 2) {
      val lastIntervalPowerOf2 = NumberUtils.isPowerOf2(t - 1)
      if (lastIntervalPowerOf2 != -1) {
        // create new Interval Link , with values
        // 2^n+1
        this.unsaturatedIntervals.clear()
        val seq = new ListBuffer[Long]()
        var a = 1
        seq += 1
        for (i <- 1 to lastIntervalPowerOf2) {
          a *= 2
          seq += a
        }
        this.head = new IntervalLink(seq)
        this.unsaturatedIntervals.push(this.head)
      } else {
        val topUnsaturated = this.unsaturatedIntervals.top
        this.head = topUnsaturated.buildBackward
        if (topUnsaturated.isSaturated) {
          this.unsaturatedIntervals.pop()
        }
        if (!this.head.isSaturated) {
          this.unsaturatedIntervals.push(this.head)
        }
      }
    } else if (t == 2) {
      val seq = new ListBuffer[Long]()
      seq += 1
      this.head = new IntervalLink(seq)
    }

  }

  /**
   * Gets all the possible paths which can be calculated from the
   * given state of intervals.
   */
  def getAllPossiblePaths: Seq[Seq[Int]] = {
    var all: Seq[Seq[Int]] = Seq[Seq[Int]]()
    var start = this.head
    var last = Seq[Int](1)
    all = all.+:(last)
    while (start != null) {
      (0 to start.intervals.length - 2) foreach {
        i =>
          val elem = start.intervals(i)
          val path = last :+ elem.asInstanceOf[Int]
          all = all.+:(path)
      }
      val path = last :+ start.intervals.last.asInstanceOf[Int]
      all = all.+:(path)

      last = last.+:(start.intervals.last.asInstanceOf[Int])
      start = start.nextLink
    }

    all
  }

  class IntervalLink(val intervals: ListBuffer[Long]) {

    def this(interval: Long) = this(ListBuffer(interval))
    // intervals having same start
    // The max interval will form the link to the next

    var nextLink: IntervalLink = null
    var prevLink: IntervalLink = null

    def buildBackward(): IntervalLink = {
      val topInterval = this.intervals.remove(0)
      val newHead = new IntervalLink(topInterval)
      // subsume the previous intervals
      if (prevLink != null) {
        prevLink.subsume(newHead.intervals)
      }
      this.prevLink = newHead
      newHead.nextLink = this
      newHead
    }

    def subsume(subsumer: ListBuffer[Long]) {
      this.intervals ++=: subsumer
      if (prevLink != null) {
        prevLink.subsume(subsumer)
      }
    }

    def isSaturated: Boolean = this.intervals.size == 1

    def identifyBestPath(lastNIntervals: Int, prevTotal: Long,
      encompassLastInterval: Boolean, startingFromInterval: Int): (Seq[Long], Long) = {
      val last = this.intervals.last
      if (last + prevTotal == lastNIntervals) {
        (Seq[Long](last), last + prevTotal)
      } else if (last + prevTotal < lastNIntervals) {
        if (this.nextLink != null) {
          val (seq, total) = this.nextLink.identifyBestPath(lastNIntervals, last + prevTotal,
            encompassLastInterval, startingFromInterval)
          if (prevTotal + last >= startingFromInterval) {
            (last +: seq, total)
          } else {
            (seq, total)
          }
        } else {
          (Seq[Long](last), last + prevTotal)
        }
      } else {
        if (encompassLastInterval) {
          this.intervals.find { x => x + prevTotal >= lastNIntervals } match {
            case Some(x) => if (x + prevTotal >= startingFromInterval) {
              (Seq[Long](x), x + prevTotal)
            } else {
              (Nil, x + prevTotal)
            }
            case None => (Nil, prevTotal)
          }
        } else {
          this.intervals.reverse.find { x => x + prevTotal <= lastNIntervals } match {
            case Some(x) => {
              if (x + prevTotal >= startingFromInterval) {
                (Seq[Long](x), x + prevTotal)
              } else {
                (Nil, x + prevTotal)
              }
            }
            case None => (Nil, prevTotal)
          }
        }
      }

    }

    def identifyIntervalsContaining(interval: Long, prevTotal: Long = 1): Seq[Long] = {
      val last = this.intervals.last
      if (last + prevTotal == interval) {
        Seq[Long](last)
      } else if (last + prevTotal < interval) {
        this.nextLink.identifyIntervalsContaining(interval, last + prevTotal)
      } else {
        this.intervals.filter { x =>
          if ((x + prevTotal) >= interval) {
            true
          } else {
            false
          }
        }

      }

    }
  }

}

object IntervalTracker {

  def write(kryo: Kryo, output: Output, obj: IntervalTracker) {
    // write head
    if (obj.head != null) {
      output.writeInt(obj.head.intervals.length)
      obj.head.intervals.foreach { x => output.writeLong(x) }
    } else {
      output.writeInt(-1)
    }
    // write unsaturated levels
    output.writeInt(obj.unsaturatedIntervals.size)
    obj.unsaturatedIntervals.reverseIterator.foreach(x => {
      output.writeInt(x.intervals.length)
      x.intervals.foreach { y => output.writeLong(y) }
    })

  }

  def read(kryo: Kryo, input: Input): IntervalTracker = {
    val tracker = new IntervalTracker()
    val headLength = input.readInt
    tracker.head = if (headLength == -1) {
      null
    } else {
      val intervals: ListBuffer[Long] = ListBuffer.fill(headLength)(input.readLong)
      new tracker.IntervalLink(intervals)
    }
    val len = input.readInt
    for (i <- 1 to len) {
      val lenn = input.readInt
      val intervals: ListBuffer[Long] = ListBuffer.fill(lenn)(input.readLong)
      tracker.unsaturatedIntervals.push(new tracker.IntervalLink(intervals))
    }
    tracker

  }
}

object Hokusai {

  def log2X(X: Long): Double = math.log10(X) / math.log10(2)

  def newZeroCMS[T: ClassTag](depth: Int, width: Int, hashA: Array[Long],
      confidence: Double, eps: Double): CountMinSketch[T] =
    new CountMinSketch[T](depth, width, hashA, confidence, eps)

  // @return the max i such that t % 2^i is zero
  // from the paper (I think the paper has a typo, and "i" should be "t"):
  //    argmax {l where i mod 2^l == 0}
  def maxJ(t: Long): Int = {
    if (t <= 1) return 0
    var l = 0
    while (t % (1 << l) == 0) l = l + 1
    l - 1
  }

  def ilog2(value: Int): Int = {
    var r = 0
    var v = value
    while (v != 0) {
      v = v >> 1
      r = r + 1
    }
    r
  }

}
