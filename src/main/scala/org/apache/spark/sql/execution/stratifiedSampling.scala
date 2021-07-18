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
import java.util.Random
import java.util.concurrent.atomic._
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.language.reflectiveCalls

import com.gemstone.gemfire.DataSerializable
import com.gemstone.gemfire.internal.cache.lru.Sizeable
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.Constant
import it.unimi.dsi.fastutil.ints.IntArrayList

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodeAndComment
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.collection.Utils._
import org.apache.spark.sql.collection._
import org.apache.spark.sql.execution.StratifiedSampler._
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.types.{DataType, LongType, StructType}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Logging, Partition, SparkEnv, TaskContext}

case class StratifiedSample(var options: Map[String, Any],
    @transient override val child: logical.LogicalPlan,
    baseTable: Option[TableIdentifier] = None)
    // pre-compute QCS because it is required by
    // other API invoked from driver
    (val qcs: (Array[Int], Array[String]) = resolveQCS(options, child.schema
        .fieldNames, "StratifiedSample"), weightedColumn: AttributeReference =
    AttributeReference(WEIGHTAGE_COLUMN_NAME, LongType, nullable = false)())
    extends logical.UnaryNode {

  /**
   * StratifiedSample will add one additional column for the ratio of total
   * rows seen for a stratum to the number of samples picked.
   */
  override val output = child.output :+ weightedColumn

  // TODO: This needs to be optimized
  // Temporary fix to avoid column pruning rule to screw the qcs mapping

  override def references: AttributeSet = AttributeSet(child.output)

  override protected final def otherCopyArgs: Seq[AnyRef] =
    Seq(qcs, weightedColumn)

  def getExecution(plan: SparkPlan): SparkPlan = StratifiedSampleExecute(plan,
    output, options, qcs._1)
}

/**
 * Perform stratified sampling given a Query-Column-Set (QCS). This variant
 * can also use a fixed fraction to be sampled instead of fixed number of
 * total samples since it is also designed to be used with streaming data.
 */
case class StratifiedSampleExecute(override val child: SparkPlan,
    override val output: Seq[Attribute], options: Map[String, Any],
    qcs: Array[Int]) extends UnaryExecNode {

  protected override def doExecute(): RDD[InternalRow] =
    new StratifiedSampledRDD(child.execute(), qcs,
      sqlContext.conf.columnBatchSize, options, schema)
}

private final class ExecutorPartitionInfo(val blockId: BlockManagerId,
    val remainingMem: Long, var remainingPartitions: Double)
    extends java.lang.Comparable[ExecutorPartitionInfo] {

  /** update "remainingPartitions" and enqueue again */
  def usePartition(queue: java.util.PriorityQueue[ExecutorPartitionInfo],
      removeFromQueue: Boolean): ExecutorPartitionInfo = {
    if (removeFromQueue) queue.remove(this)
    remainingPartitions -= 1.0
    queue.offer(this)
    this
  }

  override def compareTo(other: ExecutorPartitionInfo): Int = {
    // reverse the order so that max partitions is at head of queue
    java.lang.Double.compare(other.remainingPartitions, remainingPartitions)
  }
}

final class SamplePartition(val parent: Partition, override val index: Int,
    _partInfo: ExecutorPartitionInfo, var isLastHostPartition: Boolean)
    extends Partition with Serializable with Logging {

  val blockId = _partInfo.blockId

  def hostExecutorId: String = getHostExecutorId(blockId)

  override def toString: String =
    s"SamplePartition($index, $blockId, isLast=$isLastHostPartition)"
}

final class StratifiedSampledRDD(@transient val parent: RDD[InternalRow],
    qcs: Array[Int],
    cacheBatchSize: Int,
    options: Map[String, Any],
    schema: StructType)
    extends RDD[InternalRow](parent) with Serializable {

  var executorPartitions: Map[BlockManagerId, IndexedSeq[Int]] = Map.empty

  /**
   * This method does detailed scheduling itself which is required given that
   * the sample cache is not managed by Spark's scheduler implementations.
   * Depending on the amount of memory reported as remaining, we will assign
   * appropriate weight to that executor.
   */
  override def getPartitions: Array[Partition] = {
    val peerExecutorMap = new mutable.HashMap[String,
        mutable.ArrayBuffer[ExecutorPartitionInfo]]()
    var totalMemory = 1L
    for ((blockId, (max, remaining)) <- getAllExecutorsMemoryStatus(
      sparkContext)) {
      peerExecutorMap.getOrElseUpdate(blockId.host,
        new mutable.ArrayBuffer[ExecutorPartitionInfo](4)) +=
          new ExecutorPartitionInfo(blockId, remaining, 0)
      totalMemory += remaining
    }
    if (peerExecutorMap.nonEmpty) {
      // Split partitions executor-wise:
      //  1) assign number of partitions to each executor in proportion
      //     to amount of remaining memory on the executor
      //  2) use a priority queue to order the hosts
      //  3) first prefer the parent's hosts and select the executor with
      //     maximum remaining partitions to be assigned
      //  4) if no parent preference then select head of priority queue
      // So number of partitions assigned to each executor are known and wait
      // on each executor accordingly to drain the remaining cache.
      val parentPartitions = parent.partitions
      // calculate the approx number of partitions for each executor in
      // proportion to its total remaining memory and place in priority queue
      val queue = new java.util.PriorityQueue[ExecutorPartitionInfo]
      val numPartitions = parentPartitions.length
      peerExecutorMap.values.foreach(_.foreach { partInfo =>
        partInfo.remainingPartitions =
            (partInfo.remainingMem.toDouble * numPartitions) / totalMemory
        queue.offer(partInfo)
      })
      val partitions = (0 until numPartitions).map { index =>
        val ppart = parentPartitions(index)
        val plocs = firstParent[InternalRow].preferredLocations(ppart)
        // get the "best" one as per the maximum number of remaining partitions
        // to be assigned from among the parent partition's preferred locations
        // (that can be hosts or executors), else if none found then use the
        // head of priority queue among available peers to get the
        // "best" available executor

        // first find all executors for preferred hosts of parent partition
        plocs.flatMap { loc =>
          val underscoreIndex = loc.lastIndexOf('_')
          if (underscoreIndex >= 0) {
            // if the preferred location is already an executorId then search
            // in available executors for its host
            val host = loc.substring(TaskLocation.executorLocationTag.length,
              underscoreIndex)
            val executorId = loc.substring(underscoreIndex + 1)
            val executors = peerExecutorMap.getOrElse(host, Iterator.empty)
            executors.find(_.blockId.executorId == executorId) match {
              // executorId found so return the single value
              case Some(executor) => Iterator(executor)
              // executorId not found so return all executors for its host
              case None => executors
            }
          } else {
            peerExecutorMap.getOrElse(loc, Iterator.empty)
          }
          // reduceLeft below will find the executor with max number of
          // remaining partitions assigned to it
        }.reduceLeftOption((pm, p) => if (p.compareTo(pm) >= 0) pm else p).map {
          // get the SamplePartition for the "best" executor
          partInfo => new SamplePartition(ppart, index, partInfo.usePartition(
            queue, removeFromQueue = true), isLastHostPartition = false)
          // queue.poll below will pick "best" one from the head of queue when
          // no valid executor found from parent preferred locations
        }.getOrElse(new SamplePartition(ppart, index, queue.poll().usePartition(
          queue, removeFromQueue = false), isLastHostPartition = false))
      }
      val partitionOrdering = Ordering[Int].on[SamplePartition](_.index)
      executorPartitions = partitions.groupBy(_.blockId).map { case (k, v) =>
        val sortedPartitions = v.sorted(partitionOrdering)
        // mark the last partition in each host as the last one that should
        // also be necessarily scheduled on that host
        sortedPartitions.last.isLastHostPartition = true
        (k, sortedPartitions.map(_.index))
      }
      partitions.toArray[Partition]
    } else {
      executorPartitions = Map.empty
      Array.empty[Partition]
    }
  }

  override def compute(split: Partition,
      context: TaskContext): Iterator[InternalRow] = {
    val part = split.asInstanceOf[SamplePartition]
    val thisBlockId = SparkEnv.get.blockManager.blockManagerId
    // use -ve cacheBatchSize to indicate that no additional batching is to be
    // done but still pass the value through for size increases
    val sampleOptions = StratifiedSampler.parseOptions(options, qcs,
      "_rdd_" + id, -cacheBatchSize, schema, None)
    val sampler = StratifiedSampler(sampleOptions, cached = true)
    val numSamplers = executorPartitions(part.blockId).length

    if (part.blockId != thisBlockId) {
      // if this is the last partition then it has to be necessarily scheduled
      // on specified host
      if (part.isLastHostPartition) {
        throw new IllegalStateException(
          s"Unexpected execution of $part on $thisBlockId")
      } else {
        // this has been scheduled from some other target node so increment
        // the number of expected samplers but don't fail
        logWarning(s"Unexpected execution of $part on $thisBlockId")
        if (!sampler.numSamplers.compareAndSet(0, numSamplers + 1)) {
          sampler.numSamplers.incrementAndGet()
        }
      }
    } else {
      sampler.numSamplers.compareAndSet(0, numSamplers)
    }
    sampler.numThreads.incrementAndGet()
    try {
      val rowEncoder = RowEncoder(sampler.schema)
      sampler.sample(firstParent[InternalRow].iterator(part.parent, context),
        // If we are the last partition on this host, then wait for all
        // others to finish and then drain the remaining cache. The flag
        // is set persistently by this last thread so that any other stray
        // additional partitions will also do the flush.
        rowEncoder, part.isLastHostPartition || sampler.flushStatus.get)
    } finally {
      sampler.numThreads.decrementAndGet()
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    Seq(split.asInstanceOf[SamplePartition].hostExecutorId)
}

final case class SampleOptions(qcs: Array[Int], name: String, fraction: Double,
    stratumSize: Int, errorLimitColumn: Int, errorLimitPercent: Double,
    memBatchSize: Int, timeSeriesColumn: Int, timeInterval: Long,
    concurrency: Int, schema: StructType, bypassSampling: Boolean,
    qcsPlan: Option[(CodeAndComment, mutable.ArrayBuffer[Any], Int, Array[DataType])])
    extends Serializable

object StratifiedSampler {

  private final val globalMap = new mutable.HashMap[String, StratifiedSampler]
  private final val mapLock = new ReentrantReadWriteLock

  private final val MAX_FLUSHED_MAP_AGE_SECS = 300
  private final val MAX_FLUSHED_MAP_SIZE = 1024
  private final val flushedMaps = new mutable.LinkedHashMap[String, Long]

  final val BUFSIZE: Int = 1000
  final val EMPTY_RESERVOIR: Array[UnsafeRow] = Array.empty
  final val EMPTY_ROW: UnsafeRow = new UnsafeRow()
  final val LONG_ONE: AnyRef = Long.box(1).asInstanceOf[AnyRef]

  // using a default stratum size of 50 since 30 is taken as the normal
  // limit for assuming a normal Gaussian distribution
  final val DEFAULT_STRATUM_SIZE = 50
  final val DEFAULT_CONCURRENCY = 16
  final val MIN_AVG_NUM_CORES = 4

  def parseOptions(opts: Map[String, Any], qcsi: Array[Int],
      nameSuffix: String, columnBatchSize: Int,
      schema: StructType, qcsSparkPlan: Option[SparkPlan]): SampleOptions = {

    val cols = schema.fieldNames
    val module = "StratifiedSampler"

    // first normalize options into a mutable map to ease lookup and removal
    val options = new CaseInsensitiveMutableHashMap[Any](opts)

    val qcsV = options.remove("qcs")
    val bypassSamplingOp = options.remove(Constant.keyBypassSampleOperator)
        .exists(_.toString.toBoolean)
    // passed in qcs, if any, overrides the one in options


    val qcsSPOption = qcsSparkPlan.map (plan => {
      val numCols = plan.output.length
      val types = plan.output.map(_.dataType).toArray
      // (plan.asInstanceOf[WholeStageCodegenExec].doExecute().iterator(QCSRDD.
      // constantPartition(0), null),
      val (ctx, codeComm) = plan match {
        case ws: WholeStageCodegenExec => ws.doCodeGen()
        case CodegenSparkFallback(ws: WholeStageCodegenExec, _) => ws.doCodeGen()
      }

      (codeComm, ctx.references,
          numCols,
          types)
    }
    )

    val qcs = if (qcsi != null && qcsi.isEmpty) resolveQCS(qcsV, cols, module)._1 else qcsi

    val name = options.remove("name").map(_.toString + nameSuffix)
        .getOrElse(nameSuffix)

    val fraction = options.remove("fraction").map(
      parseDouble(_, module, "fraction", 0.0, 1.0)).getOrElse(0.0)
    val stratumSize = options.remove("strataReservoirSize").map(parseInteger(_,
      module, "strataReservoirSize")).getOrElse(DEFAULT_STRATUM_SIZE)

    val errorLimitColumn = options.remove("errorLimitColumn").map(
      parseColumn(_, cols, module, "errorLimitColumn")).getOrElse(-1)
    val errorLimitPercent = options.remove("errorLimitPercent").map(
      parseDouble(_, module, "errorLimitPercent", 0.0, 100.0)).getOrElse(0.0)

    val tsCol = options.remove("timeSeriesColumn").map(
      parseColumn(_, cols, module, "timeSeriesColumn")).getOrElse(-1)
    val timeInterval = options.remove("timeInterval").map(
      parseTimeInterval(_, module)).getOrElse(0L)

    val concurrency = options.remove("concurrency").map(parseColumn(_, cols,
      module, "concurrency")).getOrElse(DEFAULT_CONCURRENCY)

    // check for any remaining unsupported options
    if (options.nonEmpty) {
      val optMsg = if (options.size > 1) "options" else "option"
      throw new AnalysisException(
        s"$module: Unknown $optMsg: $options")
    }
    SampleOptions(qcs, name, fraction, stratumSize, errorLimitColumn,
      errorLimitPercent, columnBatchSize, tsCol, timeInterval,
      concurrency, schema, bypassSamplingOp, qcsSPOption)
  }

  def apply(options: SampleOptions, cached: Boolean,
    reservoirRegionName: String = null, sampleIsPartitioned: Boolean = false): StratifiedSampler = {
    if (cached && options.name.nonEmpty) {
      lookupOrAdd(options, reservoirRegionName, sampleIsPartitioned)
    } else {
      newSampler(options, reservoirRegionName, sampleIsPartitioned)
    }
  }

  def apply(name: String): Option[StratifiedSampler] = {
    SegmentMap.lock(mapLock.readLock) {
      globalMap.get(name)
    }
  }

  private[sql] def lookupOrAdd(options: SampleOptions,
    reservoirRegionName: String, sampleIsPartitioned: Boolean): StratifiedSampler = {
    val name = options.name
    // not using getOrElse in one shot to allow taking only read lock
    // for the common case, then release it and take write lock if new
    // sampler has to be added
    SegmentMap.lock(mapLock.readLock) {
      globalMap.get(name)
    } match {
      case Some(sampler) => sampler
      case None =>
        // insert into global map but double-check after write lock
        SegmentMap.lock(mapLock.writeLock) {
          globalMap.getOrElse(name, {
            val sampler = newSampler(options, reservoirRegionName, sampleIsPartitioned)
            // if the map has been removed previously, then mark as flushed
            sampler.setFlushStatus(flushedMaps.contains(name))
            globalMap(name) = sampler
            sampler
          })
        }
    }
  }

  def removeSampler(name: String, markFlushed: Boolean): Unit =
    SegmentMap.lock(mapLock.writeLock) {
      globalMap.remove(name).foreach(_.onTruncate())
      if (markFlushed) {
        // make an entry in the flushedMaps list with the current time
        // for expiration later if the map becomes large
        flushedMaps.put(name, System.nanoTime)
        // clear old values if map is too large and expiration
        // has been hit for one or more older entries
        if (flushedMaps.size > MAX_FLUSHED_MAP_SIZE) {
          val expireTime = System.nanoTime -
              (MAX_FLUSHED_MAP_AGE_SECS * 1000000000L)
          flushedMaps.takeWhile(_._2 <= expireTime).keysIterator.
              foreach(flushedMaps.remove)
        }
      }
    }

  private def newSampler(options: SampleOptions,
    reservoirRegionName: String, sampleIsPartitioned: Boolean): StratifiedSampler = {
    if (options.qcsPlan.isEmpty && options.qcs.isEmpty) {
      throw new AnalysisException(ERROR_NO_QCS("StratifiedSampler"))
    } else if (options.timeSeriesColumn >= 0 && options.timeInterval <= 0) {
      throw new AnalysisException("StratifiedSampler: no timeInterval for " +
          s"timeSeriesColumn=${options.schema(options.timeSeriesColumn).name}")
    } else if (options.errorLimitColumn >= 0) {
      new StratifiedSamplerErrorLimit(options)
    } else if (options.fraction > 0.0) {
      val reservoirRegion = Misc.getReservoirRegionForSampleTable(reservoirRegionName)
      if (reservoirRegion != null) {
        new StratifiedSamplerCachedInRegion(reservoirRegion, options, sampleIsPartitioned)
      } else {
        new StratifiedSamplerCached(options)
      }
    } else if (options.stratumSize > 0) {
      new StratifiedSamplerReservoir(options)
    } else {
      throw new AnalysisException("StratifiedSampler: 'fraction'=" +
          s"${options.fraction} 'strataReservoirSize'=${options.stratumSize}")
    }
  }

  def compareOrderAndSet(atomicVal: AtomicLong, compareTo: Long,
      getMax: Boolean): Boolean = {
    while (true) {
      val v = atomicVal.get
      val cmp = if (getMax) compareTo > v else compareTo < v
      if (cmp) {
        if (atomicVal.compareAndSet(v, compareTo)) {
          return true
        }
      } else return false
    }
    false
  }

  def fillWeightage(reservoir: Array[UnsafeRow], pos: Int,
      ratio: Long, lastIndex: Int): UnsafeRow = {
    // fill in the weight ratio column, always
    val row = reservoir(pos)
    // if (row.getLong(lastIndex) == 1L) {
    row.setLong(lastIndex, ratio)
    // }
    row
  }
}

abstract class StratifiedSampler(final val options: SampleOptions)
    extends Serializable with Cloneable with Logging {

  type ReservoirSegment = SegmentMap[Row, StratumReservoir]

  final def qcs: Array[Int] = options.qcs
  final def qcsSparkPlan: Option[(CodeAndComment, mutable.ArrayBuffer[Any],
      Int, Array[DataType])] = options.qcsPlan

  final def name: String = options.name

  final def concurrency: Int = options.concurrency

  final def schema: StructType = options.schema

  def module: String = "StratifiedSampler"

  def onTruncate(): Unit

  /**
   * Map of each stratum key (i.e. a unique combination of values of columns
   * in qcs) to related metadata and reservoir
   */
  protected final val strata = {
    val qcsSPOption = qcsSparkPlan

    val(newQcs, types, numColumns ) = qcsSPOption.map {
      case(codeAndComm, refs, numColls, typees) =>
        (Array.tabulate[Int](numColls)(i => i), typees, numColls)
    }.getOrElse((qcs, qcs.map(schema(_).dataType), qcs.length))

    val baseTypes = schema.map(_.dataType).toArray
    val columnHandler = qcsSPOption.map{ case(codeAndComm, refs, _, tps) =>
      QCSSQLColumnHandler.newSqlHandler((codeAndComm, refs, tps, baseTypes),
        MultiColumnOpenHashSet.newColumnHandler(newQcs, tps, numColumns)
      )
    }.getOrElse( MultiColumnOpenHashSet.newColumnHandler(newQcs, types, numColumns) )

    val qcsColHandler = qcsSPOption.map(_ => columnHandler)

    val hasher: Row => Int = columnHandler.hash
    new ConcurrentSegmentedHashMap[Row, StratumReservoir, ReservoirSegment](
      concurrency, (initialCapacity, loadFactor, segi, nsegs) => getReservoirSegment(newQcs,
        types, numColumns, initialCapacity, loadFactor, qcsColHandler, segi, nsegs), hasher)
  }

  protected def getReservoirSegment(newQcs: Array[Int], types: Array[DataType],
    numColumns: Int, initialCapacity: Int, loadFactor: Double,
    qcsColHandler: Option[MultiColumnOpenHashSet.ColumnHandler],
    segi: Int, nsegs: Int): ReservoirSegment =
    new MultiColumnOpenHashMap[StratumReservoir](newQcs, types, numColumns,
      initialCapacity, loadFactor, qcsColHandler)

  /** Random number generator for sampling. */
  protected final val rng =
  new Random(org.apache.spark.util.Utils.random.nextLong)

  private[sql] final val numSamplers = new AtomicInteger
  private[sql] final val numThreads = new AtomicInteger

  /**
   * Store pending values to be flushed in a separate buffer so that we
   * do not end up creating too small ColumnBatches.
   *
   * Note that this mini-cache is copy-on-write (to avoid copy-on-read for
   * readers) so the buffer inside should never be changed rather the whole
   * buffer replaced if required. This should happen only inside flushCache.
   */
  protected final val pendingBatch = new AtomicReference[
      mutable.ArrayBuffer[InternalRow]](new mutable.ArrayBuffer[InternalRow])

  protected def strataReservoirSize: Int

  def flushReservoir[U](init: U, process: (U, InternalRow) => U,
      startBatch: (U, Int) => U, endBatch: U => U): U

  protected final class RowWithWeight(baseRow: Row) extends Row {

    override def length: Int = baseRow.length + 1

    override def get(i: Int): Any =
      if (i < baseRow.length) baseRow.get(i) else LONG_ONE

    override def isNullAt(i: Int): Boolean =
      if (i < baseRow.length) baseRow.isNullAt(i) else false

    override def getBoolean(i: Int): Boolean = baseRow.getBoolean(i)

    override def getByte(i: Int): Byte = baseRow.getByte(i)

    override def getShort(i: Int): Short = baseRow.getShort(i)

    override def getInt(i: Int): Int = baseRow.getInt(i)

    override def getLong(i: Int): Long =
      if (i < baseRow.length) baseRow.getLong(i) else 1L

    override def getFloat(i: Int): Float = baseRow.getFloat(i)

    override def getDouble(i: Int): Double = baseRow.getDouble(i)

    override def getString(i: Int): String = baseRow.getString(i)

    override def getDecimal(i: Int): java.math.BigDecimal =
      baseRow.getDecimal(i)

    override def getDate(i: Int): java.sql.Date = baseRow.getDate(i)

    override def getTimestamp(i: Int): java.sql.Timestamp =
      baseRow.getTimestamp(i)

    override def getSeq[T](i: Int): Seq[T] = baseRow.getSeq[T](i)

    override def getMap[K, V](i: Int): scala.collection.Map[K, V] =
      baseRow.getMap[K, V](i)

    override def getStruct(i: Int): Row = baseRow.getStruct(i)

    override def fieldIndex(name: String): Int = {
      if (name.equalsIgnoreCase(WEIGHTAGE_COLUMN_NAME)) baseRow.length
      else baseRow.fieldIndex(name)
    }

    override def copy(): Row = new RowWithWeight(baseRow.copy())
  }

  protected final def newMutableRow(row: Row,
      rowEncoder: ExpressionEncoder[Row]): UnsafeRow =
    rowEncoder.toRow(new RowWithWeight(row)).asInstanceOf[UnsafeRow].copy()

  def append[U](rows: Iterator[Row], init: U,
      processFlush: (U, InternalRow) => U, startBatch: (U, Int) => U, endBatch: U => U,
      rowEncoder: ExpressionEncoder[Row], partIndex: Int): Long

  def sample(items: Iterator[InternalRow],
      rowEncoder: ExpressionEncoder[Row],
      flush: Boolean): Iterator[InternalRow]

  private[sql] final val flushStatus = new AtomicBoolean

  def setFlushStatus(doFlush: Boolean): Unit = flushStatus.set(doFlush)

  def iterator(segmentStart: Int, segmentEnd: Int): Iterator[InternalRow] = {
    val sampleBuffer = new mutable.ArrayBuffer[InternalRow](BUFSIZE)
    val itr = strata.foldSegments(segmentStart, segmentEnd,
      Iterator[InternalRow]()) { (it, seg) =>
      it ++ {
        if (sampleBuffer.nonEmpty) sampleBuffer.clear()
        SegmentMap.lock(seg.readLock()) {
          seg.foldValues((), foldReservoir[Unit](0, doReset = false,
            fullReset = false, (_, row) => sampleBuffer += row))
        }
        sampleBuffer.iterator
      }
    }
    if (segmentEnd == strata.concurrency) {
      val pending = this.pendingBatch.get()
      if (pending.nonEmpty) itr ++ pending.iterator else itr
    } else itr
  }

  def iteratorOnRegion(buckets: java.util.Set[Integer]): Iterator[InternalRow] = {
    Iterator.empty
  }

  protected final def foldDrainSegment[U](prevReservoirSize: Int,
      fullReset: Boolean,
      process: (U, InternalRow) => U)
      (init: U, seg: ReservoirSegment): U = {
    seg.foldValues(init, foldReservoir(prevReservoirSize, doReset = true,
      fullReset, process))
  }

  protected final def foldReservoir[U](prevReservoirSize: Int,
      doReset: Boolean, fullReset: Boolean, process: (U, InternalRow) => U)
      (bid: Int, sr: StratumReservoir, init: U): U = {
    // imperative code segment below for best efficiency
    var v = init
    val reservoir = sr.reservoir
    val reservoirSize = sr.reservoirSize
    val ratio = sr.calculateWeightageColumn()
    val lastColumnIndex = schema.length - 1
    var index = 0
    while (index < reservoirSize) {
      v = process(v,
        fillWeightage(reservoir, index, ratio, lastColumnIndex))
      index += 1
    }
    // reset transient data
    if (doReset) {
      sr.reset(prevReservoirSize, strataReservoirSize, fullReset)
    }
    v
  }

  protected def waitForSamplers(waitUntil: Int, maxMillis: Long) {
    val startTime = System.currentTimeMillis
    numThreads.decrementAndGet()
    try {
      numSamplers.synchronized {
        while (numSamplers.get > waitUntil &&
            (numThreads.get > 0 || maxMillis <= 0 ||
                (System.currentTimeMillis - startTime) <= maxMillis))
          numSamplers.wait(100)
      }
    } finally {
      numThreads.incrementAndGet()
    }
  }

  protected def isBucketLocal(partIndex: Int): Boolean = true

  protected def getBucketId(partIndex: Int,
      primaryBucketIds: IntArrayList = null)(hashValue: Int): Int = partIndex

  def reservoirInRegion: Boolean = false
}

// TODO: optimize by having metadata as multiple columns like key;
// TODO: add a good sparse array implementation

/**
 * For each stratum (i.e. a unique set of values for QCS), keep a set of
 * meta-data including number of samples collected, total number of rows
 * in the stratum seen so far, the QCS key, reservoir of samples etc.
 */
class StratumReservoir(final var reservoir: Array[UnsafeRow],
    final var reservoirSize: Int,
    final var batchTotalSize: Int) extends DataSerializable with Sizeable with Serializable {

  self =>

  // For deserialization
  def this() = this(Array.empty[UnsafeRow], -1, -1)

  def numSamples: Int = reservoirSize

  final def iterator(prevReservoirSize: Int, newReservoirSize: Int,
      columns: Int, doReset: Boolean,
      fullReset: Boolean, useWrapper: Boolean = false): Iterator[InternalRow] = {
    class BaseIterator extends Iterator[InternalRow] {
      final val reservoir = self.reservoir
      final val reservoirSize = self.reservoirSize
      final val ratio = calculateWeightageColumn()
      final val lastIndex = columns - 1
      var pos = 0

      override def hasNext: Boolean = {
        if (pos < reservoirSize) {
          true
        } else if (doReset) {
          self.reset(prevReservoirSize, newReservoirSize, fullReset)
          false
        } else {
          false
        }
      }

      override def next(): InternalRow = {
        val v = fillWeightage(reservoir, pos, ratio, lastIndex)
        pos += 1
        v
      }
    }
      if (useWrapper) {
        new BaseIterator () {
          val wrapper = new StratumInternalRow(ratio)
          override def next(): InternalRow = {
            val row = reservoir(pos)
            wrapper.actualRow = row
            pos += 1
            wrapper
          }
        }
      } else {
        new BaseIterator()
      }


  }


  final def calculateWeightageColumn(): Long = {
    val numSamples = self.numSamples
    // calculate the weight ratio column
    if (numSamples > 0) {
      // combine the two integers into a long
      // higher order is number of samples (which is expected to remain mostly
      //   constant will result in less change)
      ((numSamples & 0xffffff).asInstanceOf[Long] << 40L) |
          (batchTotalSize.asInstanceOf[Long] << 8L) |
          (self.hashCode() & 0xff).asInstanceOf[Long]
    } else 0
  }

  def reset(prevReservoirSize: Int, newReservoirSize: Int,
      fullReset: Boolean): Unit = {

    if (newReservoirSize > 0) {
      // shrink reservoir back to strataReservoirSize if required to avoid
      // growing possibly without bound (in case some stratum consistently
      //   gets small number of total rows less than sample size)
      if (reservoir.length == newReservoirSize) {
        fillArray(reservoir, EMPTY_ROW, 0, reservoirSize)
      } else if (reservoirSize <= 2 && newReservoirSize > 3) {
        // empty the reservoir since it did not receive much data in last round
        reservoir = EMPTY_RESERVOIR
      } else {
        reservoir = new Array[UnsafeRow](newReservoirSize)
      }
      reservoirSize = 0
      batchTotalSize = 0
    }
  }

  // 1. Object 2. reservoir 3. reservoirSize 4. batchTotalSize
  private val fixedSizeInBytes = Sizeable.PER_OBJECT_OVERHEAD * 4

  override def getSizeInBytes: Int = fixedSizeInBytes + reservoirSize * Sizeable.PER_OBJECT_OVERHEAD

  override def toData(out: DataOutput): Unit = {
    out.writeInt(reservoir.length)
    for (elem <- reservoir) {
      Option(elem).getOrElse(StratifiedSampler.EMPTY_ROW).toData(out)
    }
    out.writeInt(reservoirSize)
    out.writeInt(batchTotalSize)
  }

  override def fromData (in: DataInput): Unit = {
    val inSize = in.readInt()
    reservoir = new Array[UnsafeRow](inSize)
    for (i <- reservoir.indices) {
      reservoir(i) = new UnsafeRow()
      reservoir(i).fromData(in)
    }
    reservoirSize = in.readInt()
    batchTotalSize = in.readInt()
  }
}
