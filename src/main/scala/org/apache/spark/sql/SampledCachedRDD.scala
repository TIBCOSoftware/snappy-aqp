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

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.{MultiBucketExecutorPartition, Utils}
import org.apache.spark.sql.execution.StratifiedSampler
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}

/**
 * Encapsulates an RDD over all the cached samples for a sampled table.
 * Parallelizes execution using the hashmap segment configuration on the nodes
 * with each partition handling one or more segments of the hashmap on a node.
 */
final class SampledCachedRDD(
    _sc: SparkContext,
    @transient val getExecutors: SparkContext => Iterable[BlockManagerId],
    samplerName: String, concurrencyPerHost: Int, totalCores: Int,
    colocatedPartitions: Array[Partition])
    extends RDD[InternalRow](_sc, Nil) {

  override def getPartitions: Array[Partition] = {
    // use incoming partitions if provided (e.g. for collocated tables)
    if (colocatedPartitions.length > 0) {
      colocatedPartitions
    } else {
      getSamplePartitions
    }
  }

  private def getSamplePartitions: Array[Partition] = {
    val peers = getExecutors(sparkContext)
    val numPeers = peers.size
    if (numPeers > 0) {
      // determine a decent number for parallelism on each host
      val avgNumCores = math.max(totalCores / numPeers,
        StratifiedSampler.MIN_AVG_NUM_CORES)
      // cannot exceed total concurrency of map (concurrencyPerHost*numPeers)
      // but also don't want concurrency to exceed number of cores
      val partitionsPerHost = math.min(avgNumCores, concurrencyPerHost)
      // divide total of "concurrencyPerHost" segments among "avgNumCores"
      // per host; segmentsPerPartition is base number for each partition while
      // "additionalSegments" partitions will have one additional segment
      val segmentsPerPartition = concurrencyPerHost / avgNumCores
      val additionalSegments = concurrencyPerHost % avgNumCores
      var partitionId = 0
      val ps = new ArrayBuffer[Partition]()
      peers.foreach { peer =>
        var startSegmentId = 0
        val parts = for (hostPartitionId <- 0 until partitionsPerHost) yield {
          // for "additionalSegments" partitions, assign one extra segment
          val numSegments =
            if (hostPartitionId < additionalSegments) segmentsPerPartition + 1
            else segmentsPerPartition
          val endSegmentId = startSegmentId + numSegments
          val partition = new SamplePartition(partitionId, startSegmentId,
            endSegmentId, peer)
          ps += partition
          startSegmentId = endSegmentId
          partitionId += 1
          partition
        }
        parts
      }
      ps.toArray[Partition]
    } else {
      Array.empty[Partition]
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split match {
      case m: MultiBucketExecutorPartition => m.hostExecutorIds
      case _ => Seq (split.asInstanceOf[SamplePartition].hostExecutorId)
    }
  }

  override def compute(split: Partition,
      context: TaskContext): Iterator[InternalRow] = {
    split match {
      case m: MultiBucketExecutorPartition =>
        StratifiedSampler(samplerName) match {
          case Some(ss) => ss.iteratorOnRegion(m.buckets)
          case None => Iterator.empty
        }
      case _ => computeSample(split, context)
    }
  }

  private def computeSample(split: Partition,
      context: TaskContext): Iterator[InternalRow] = {
    val part = split.asInstanceOf[SamplePartition]
    val thisBlockId = SparkEnv.get.blockManager.blockManagerId
    if (part.blockId.host != thisBlockId.host ||
        part.blockId.executorId != thisBlockId.executorId) {
      throw new IllegalStateException(
        s"Unexpected execution of $part on $thisBlockId")
    }

    StratifiedSampler(samplerName) match {
      case Some(ss) => ss.iterator(part.segmentStart, part.segmentEnd)
      case None => Iterator.empty
    }
  }
}

final class SamplePartition(override val index: Int, val segmentStart: Int,
    val segmentEnd: Int, val blockId: BlockManagerId) extends Partition {

  def hostExecutorId: String = Utils.getHostExecutorId(blockId)

  def segments: Seq[Int] = segmentStart until segmentEnd

  override def toString: String =
    s"SamplePartition($index, [$segmentStart-$segmentEnd), $blockId)"
}
