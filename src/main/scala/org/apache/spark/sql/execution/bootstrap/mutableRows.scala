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
package org.apache.spark.sql.execution.bootstrap

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util.concurrent.{AbstractExecutorService, Callable, TimeUnit}

import scala.concurrent.ExecutionContextExecutorService

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BaseGenericInternalRow

class DoubleMutableRow(var values: Array[Double], var numTrials: Int)
  extends InternalRow with BaseGenericInternalRow with Externalizable with KryoSerializable {

  def this(size: Int) = this(Array.fill[Double](size)(0d), size)

  def this() = this(null, 0)

  private[bootstrap] var counter = 0
  private[bootstrap] var sum0 = 0d
  private[bootstrap] var stateArrayOption: Option[Array[Byte]] = None
  @transient private[bootstrap] val tempStore = Array.ofDim[Double](DoubleMutableRow.cacheSize)
  @transient private[bootstrap] var seeds: Array[Int] = _
  @transient private[bootstrap] var seedInBuffer: Option[Int] = None
  @transient private[bootstrap] var nullCount = 0

  def setStateArray(stateArray: Array[Byte]): Unit = {
    var temp = stateArray(0)
    temp = (temp | 0x01).asInstanceOf[Byte]
    stateArray(0) = temp
    this.stateArrayOption = Some(stateArray)
  }

  def setSeedArray(seeds: Array[Int]): Unit = {
    this.seeds = seeds
  }

  def add(value: Double, count: Int, seed: Int): Unit = {
    if (count == 1) {
      seedInBuffer = Some(seed)
    } else {
      seeds(count - 1) = seed
    }
    this.add(value, count)
  }

  def add(value: Double, count: Int): Unit = {
    this.counter = count
    sum0 += value
    tempStore(counter - 1) = value
  }

  def addNull(count: Int, seed: Int): Unit = {
    this.add(0, count, seed)
    this.nullCount += 1
  }

  def addNull(count: Int): Unit = {
    this.add(0, count)
    this.nullCount += 1
  }


  def store(): Unit = {
    if (this.nullCount < this.counter) {
      if (this.values == null) {
        this.values = Array.ofDim[Double](numTrials)
      }
      if (counter > 0) {
        this.values(0) += sum0
        sum0 = 0
        seedInBuffer match {
          case Some(seedInBuff) => {
            this.seeds(0) = seedInBuff
            this.seedInBuffer = None
          }
          case None =>
        }
        val totalGroups = BootstrapMultiplicity.calculateMultiplicityResultSize(numTrials)
        val numThreadgroups = totalGroups / DoubleMutableRow.numThreads +
          (if (totalGroups % DoubleMutableRow.numThreads == 0) {
            0
          } else {
            1
          })
        var trialStart = 1
        val callables = for (j <- 1 to numThreadgroups) yield {
          val startingTrial = trialStart
          val endTrial = if (j == numThreadgroups) {
            numTrials
          } else {
            DoubleMutableRow.numThreads * j * BootstrapMultiplicity.groupSize
          }
          trialStart = endTrial
          new Callable[Int] {
            override def call(): Int = {
              var multiplicityIndex = startingTrial / BootstrapMultiplicity.groupSize
              var currentStateByte: Byte = stateArrayOption.map(_ (multiplicityIndex))
                .getOrElse(0)
              startingTrial until endTrial foreach (trialNum => {

                val compressorIndex = trialNum % BootstrapMultiplicity.groupSize
                if (stateArrayOption.isDefined) {
                  if (compressorIndex == 0) {
                    multiplicityIndex = trialNum / BootstrapMultiplicity.groupSize
                    currentStateByte = stateArrayOption.map(_ (multiplicityIndex)).get
                  }
                }
                var sum: Double = 0d
                var skipStateCheck = stateArrayOption.isEmpty ||
                  (currentStateByte & (1 << compressorIndex)) > 0
                val stateArr = if (skipStateCheck) null else stateArrayOption.get
                0 until counter foreach (rowNum => {
                  val actualValue = tempStore(rowNum)
                  val current = seeds(rowNum) * DoubleMutableRow.powers(trialNum - 1)

                  val multiplicity: Byte = if (current <= -567453481) {
                    0
                  } else if (current <= 1012576688) {
                    1
                  } else if (current <= 1802591772) {
                    2
                  } else if (current <= 2065930134) {
                    3
                  } else if (current <= 2131764724) {
                    4
                  } else if (current <= 2144931642) {
                    5
                  } else if (current <= 2147126128) {
                    6
                  } else if (current <= 2147439627) {
                    7
                  } else if (current <= 2147478814) {
                    8
                  } else if (current <= 2147483168) {
                    9
                  } else if (current <= 2147483603) {
                    10
                  } else if (current <= 2147483643) {
                    11
                  } else if (current == 2147483647) {
                    15
                  } else {
                    12
                  }

                  sum += multiplicity * actualValue
                  if (!skipStateCheck && multiplicity > 0) {
                    currentStateByte = (currentStateByte | (1 << compressorIndex)).
                      asInstanceOf[Byte]
                    skipStateCheck = true
                    stateArr(multiplicityIndex) = currentStateByte
                  }

                }
                  )

                values(trialNum) += sum

              })
              0
            }

          }
        }
        val futures = callables.map(DoubleMutableRow.executorService.submit(_))
        futures.foreach(_.get)

      }
    }
    this.counter = 0
    this.nullCount = 0

  }


  def store(otherRow: DoubleMutableRow): Unit = {
    if (counter > 0) {
      this.values(0) += sum0
      sum0 = 0
      otherRow.values(0) += otherRow.sum0
      otherRow.sum0 = 0
      seedInBuffer match {
        case Some(seedInBuff) => {
          this.seeds(0) = seedInBuff
          this.seedInBuffer = None
        }
        case None =>
      }
      val totalGroups = BootstrapMultiplicity.calculateMultiplicityResultSize(numTrials)
      val numThreadgroups = totalGroups / DoubleMutableRow.numThreads +
        (if (totalGroups % DoubleMutableRow.numThreads == 0) {
          0
        } else {
          1
        })
      var trialStart = 1
      val callables = for (j <- 1 to numThreadgroups) yield {
        val startingTrial = trialStart
        val endTrial = if (j == numThreadgroups) {
          numTrials
        } else {
          DoubleMutableRow.numThreads * j * BootstrapMultiplicity.groupSize
        }
        trialStart = endTrial
        new Callable[Int] {
          override def call(): Int = {
            var multiplicityIndex = startingTrial / BootstrapMultiplicity.groupSize
            var currentStateByte: Byte = stateArrayOption.map(_ (multiplicityIndex)).getOrElse(0x00)
            startingTrial until endTrial foreach (trialNum => {
              val compressorIndex = trialNum % BootstrapMultiplicity.groupSize
              if (stateArrayOption.isDefined) {
                if (compressorIndex == 0) {
                  multiplicityIndex = trialNum / BootstrapMultiplicity.groupSize
                  currentStateByte = stateArrayOption.map(_ (multiplicityIndex)).get
                }
              }
              var sum1: Double = 0d
              var sum2: Double = 0d
              var skipStateCheck = stateArrayOption.isEmpty || (currentStateByte & (1 <<
                compressorIndex)) > 0
              val stateArr = if (skipStateCheck) null else stateArrayOption.get
              0 until counter foreach (rowNum => {
                val actualValue1 = tempStore(rowNum)
                val actualValue2 = otherRow.tempStore(rowNum)
                val current = seeds(rowNum) * DoubleMutableRow.powers(trialNum - 1)
                val multiplicity: Byte = if (current <= -567453481) {
                  0
                } else if (current <= 1012576688) {
                  1
                } else if (current <= 1802591772) {
                  2
                } else if (current <= 2065930134) {
                  3
                } else if (current <= 2131764724) {
                  4
                } else if (current <= 2144931642) {
                  5
                } else if (current <= 2147126128) {
                  6
                } else if (current <= 2147439627) {
                  7
                } else if (current <= 2147478814) {
                  8
                } else if (current <= 2147483168) {
                  9
                } else if (current <= 2147483603) {
                  10
                } else if (current <= 2147483643) {
                  11
                } else if (current == 2147483647) {
                  15
                } else {
                  12
                }
                sum1 += multiplicity * actualValue1
                sum2 += multiplicity * actualValue2
                if (!skipStateCheck && multiplicity > 0) {
                  currentStateByte = (currentStateByte | (1 << compressorIndex)).asInstanceOf[Byte]
                  skipStateCheck = true
                  stateArr(multiplicityIndex) = currentStateByte
                }

              }
                )

              values(trialNum) += sum1
              otherRow.values(trialNum) += sum2

            })
            0
          }

        }
      }
      val futures = callables.map(DoubleMutableRow.executorService.submit(_))
      futures.foreach(_.get)

      this.counter = 0
      otherRow.counter = 0
    }

  }

  override def numFields: Int = numTrials

  override def setNullAt(i: Int): Unit = {
    values(i) = Double.NaN
  }

  override def isNullAt(i: Int): Boolean = false // values == null || values(i).isNaN

  // bootstrap relies on object reference remaining the same
  override def copy(): DoubleMutableRow = this

  override protected def genericGet(i: Int): Any = {
    if (this.counter != 0) {
      this.store()
    }
    if (this.values != null) {
      values(i)
    } else {
      Double.NaN
    }
  }

  override def update(ordinal: Int, value: Any) {
    if (value == null) {
      setNullAt(ordinal)
    } else {
      values(ordinal) = value.asInstanceOf[Double]
    }
  }

  override def setInt(ordinal: Int, value: Int): Unit = {
    values(ordinal) = value

  }

  override def getInt(i: Int): Int = {
    if (this.counter != 0) {
      this.store()
    }
    values(i).asInstanceOf[Int]
  }

  override def setFloat(ordinal: Int, value: Float): Unit = {

    values(ordinal) = value
  }

  override def getFloat(i: Int): Float = {
    if (this.counter != 0) {
      this.store()
    }
    values(i).toFloat
  }

  override def setBoolean(ordinal: Int, value: Boolean): Unit = {

  }

  override def getBoolean(i: Int): Boolean = {
    false
  }

  override def setDouble(ordinal: Int, value: Double): Unit = {
    values(ordinal) = value
  }

  override def getDouble(i: Int): Double = {
    if (this.counter != 0) {
      this.store()
    }
    if (values != null) {
      values(i)
    } else {
      Double.NaN
    }
  }

  override def setShort(ordinal: Int, value: Short): Unit = {
    values(ordinal) = value
  }

  override def getShort(i: Int): Short = {
    if (this.counter != 0) {
      this.store()
    }
    values(i).toShort
  }

  override def setLong(ordinal: Int, value: Long): Unit = {
    values(ordinal) = value
  }

  override def getLong(i: Int): Long = {
    if (this.counter != 0) {
      this.store()
    }
    values(i).toLong
  }

  override def setByte(ordinal: Int, value: Byte): Unit = {
    values(ordinal) = value
  }

  override def getByte(i: Int): Byte = {
    if (this.counter != 0) {
      this.store()
    }
    values(i).toByte
  }

  def increment(ordinal: Int, value: Double): Unit = values(ordinal) += value

  def divide(ordinal: Int, value: Double): Unit = values(ordinal) /= value

  def writeExternal(out: ObjectOutput) {
    if (this.counter != 0) {
      this.store()
    }
    out.writeInt(this.numFields)
    out.writeBoolean(this.values == null)
    if (this.values != null) {
      this.values.foreach(out.writeDouble(_))
    }
  }

  def readExternal(in: ObjectInput) {
    val numFields = in.readInt()
    val isNull = in.readBoolean()
    if (!isNull) {
      values = Array.fill[Double](numFields)(in.readDouble())
    }
    this.numTrials = numFields
  }

  def write(kryo: Kryo, out: Output) {
    if (this.counter != 0) {
      this.store()
    }
    out.writeInt(this.numFields)
    out.writeBoolean(this.values == null)
    if (this.values != null) {
      this.values.foreach(out.writeDouble(_))
    }
  }

  def read(kryo: Kryo, in: Input) {
    val numFields = in.readInt()
    val isNull = in.readBoolean()
    if (!isNull) {
      values = Array.fill[Double](numFields)(in.readDouble())
    }
    this.numTrials = numFields
  }

}

class DoubleMutableRowDebug(_values: Array[Double], _numTrials: Int) extends
  DoubleMutableRow(_values, _numTrials) {


  def this(size: Int) = this(Array.fill[Double](size)(0d), size)

  def this() = this(null, 0)

  override def store(): Unit = {
    if (this.nullCount < this.counter) {
      if (this.values == null) {
        this.values = Array.ofDim[Double](numTrials)
      }
      if (counter > 0) {
        this.values(0) += sum0
        sum0 = 0
        seedInBuffer match {
          case Some(seedInBuff) => {
            this.seeds(0) = seedInBuff
            this.seedInBuffer = None
          }
          case None =>
        }
        val totalGroups = BootstrapMultiplicity.calculateMultiplicityResultSize(numTrials)
        val numThreadgroups = totalGroups / DoubleMutableRow.numThreads +
          (if (totalGroups % DoubleMutableRow.numThreads == 0) {
            0
          } else {
            1
          })
        var trialStart = 1
        val callables = for (j <- 1 to numThreadgroups) yield {
          val startingTrial = trialStart
          val endTrial = if (j == numThreadgroups) {
            numTrials
          } else {
            DoubleMutableRow.numThreads * j * BootstrapMultiplicity.groupSize
          }
          trialStart = endTrial
          new Callable[Int] {
            override def call(): Int = {
              var multiplicityIndex = startingTrial / BootstrapMultiplicity.groupSize
              var currentStateByte: Byte = stateArrayOption.map(_ (multiplicityIndex)).
                getOrElse(0x00)
              startingTrial until endTrial foreach (trialNum => {

                val compressorIndex = trialNum % BootstrapMultiplicity.groupSize
                if (stateArrayOption.isDefined) {
                  if (compressorIndex == 0) {
                    multiplicityIndex = trialNum / BootstrapMultiplicity.groupSize
                    currentStateByte = stateArrayOption.map(_ (multiplicityIndex)).get
                  }
                }
                var sum: Double = 0d
                var skipStateCheck = stateArrayOption.isEmpty || (currentStateByte & (1 <<
                  compressorIndex)) > 0
                val stateArr = if (skipStateCheck) null else stateArrayOption.get
                0 until counter foreach (rowNum => {
                  val actualValue = tempStore(rowNum)
                  val current = seeds(rowNum) + trialNum
                  val multiplicity: Byte = current.asInstanceOf[Byte]
                  sum += multiplicity * actualValue
                  if (!skipStateCheck && multiplicity > 0) {
                    currentStateByte = (currentStateByte | (1 << compressorIndex)).
                      asInstanceOf[Byte]
                    skipStateCheck = true
                    stateArr(multiplicityIndex) = currentStateByte
                  }

                }
                  )

                values(trialNum) += sum

              })
              0
            }

          }
        }
        val futures = callables.map(DoubleMutableRow.executorService.submit(_))
        futures.foreach(_.get)

      }
    }
    this.counter = 0
    this.nullCount = 0

  }


  override def store(otherRow: DoubleMutableRow): Unit = {
    if (counter > 0) {
      this.values(0) += sum0
      sum0 = 0
      otherRow.values(0) += otherRow.sum0
      otherRow.sum0 = 0
      seedInBuffer match {
        case Some(seedInBuff) => {
          this.seeds(0) = seedInBuff
          this.seedInBuffer = None
        }
        case None =>
      }
      val totalGroups = BootstrapMultiplicity.calculateMultiplicityResultSize(numTrials)
      val numThreadgroups = totalGroups / DoubleMutableRow.numThreads +
        (if (totalGroups % DoubleMutableRow.numThreads == 0) {
          0
        } else {
          1
        })
      var trialStart = 1
      val callables = for (j <- 1 to numThreadgroups) yield {
        val startingTrial = trialStart
        val endTrial = if (j == numThreadgroups) {
          numTrials
        } else {
          DoubleMutableRow.numThreads * j * BootstrapMultiplicity.groupSize
        }
        trialStart = endTrial
        new Callable[Int] {
          override def call(): Int = {
            var multiplicityIndex = startingTrial / BootstrapMultiplicity.groupSize
            var currentStateByte: Byte = stateArrayOption.map(_ (multiplicityIndex)).getOrElse(0x00)
            startingTrial until endTrial foreach (trialNum => {
              val compressorIndex = trialNum % BootstrapMultiplicity.groupSize
              if (stateArrayOption.isDefined) {
                if (compressorIndex == 0) {
                  multiplicityIndex = trialNum / BootstrapMultiplicity.groupSize
                  currentStateByte = stateArrayOption.map(_ (multiplicityIndex)).get
                }
              }
              var sum1: Double = 0d
              var sum2: Double = 0d
              var skipStateCheck = stateArrayOption.isEmpty || (currentStateByte & (1 <<
                compressorIndex)) > 0
              val stateArr = if (skipStateCheck) null else stateArrayOption.get
              0 until counter foreach (rowNum => {
                val actualValue1 = tempStore(rowNum)
                val actualValue2 = otherRow.tempStore(rowNum)
                val current = seeds(rowNum) + trialNum
                val multiplicity: Byte = current.asInstanceOf[Byte]
                sum1 += multiplicity * actualValue1
                sum2 += multiplicity * actualValue2
                if (!skipStateCheck && multiplicity > 0) {
                  currentStateByte = (currentStateByte | (1 << compressorIndex)).asInstanceOf[Byte]
                  skipStateCheck = true
                  stateArr(multiplicityIndex) = currentStateByte
                }

              }
                )

              values(trialNum) += sum1
              otherRow.values(trialNum) += sum2

            })
            0
          }

        }
      }
      val futures = callables.map(DoubleMutableRow.executorService.submit(_))
      futures.foreach(_.get)

      this.counter = 0
      otherRow.counter = 0
    }
  }
}


final class ByteMutableRow(var values: Array[Byte])
  extends InternalRow with BaseGenericInternalRow with Externalizable with KryoSerializable {

  var firstAggAttributeRow: DoubleMutableRow = _

  def setFirstAggregateMutableRow(doubleMutableRow: DoubleMutableRow): Unit = {
    this.firstAggAttributeRow = doubleMutableRow
  }

  def this(size: Int) = this(Array.fill[Byte](size)(0))

  def this() = this(null)

  override def numFields: Int = values.length

  override def setNullAt(i: Int): Unit = {}

  override def isNullAt(i: Int): Boolean = false

  // bootstrap relies on object reference remaining the same
  override def copy(): ByteMutableRow = this

  override protected def genericGet(i: Int): Any = {
    if (firstAggAttributeRow != null) {
      this.firstAggAttributeRow.store()
      this.firstAggAttributeRow = null
    }
    values(i)
  }

  override def update(ordinal: Int, value: Any) {
    if (value == null) {
      throw new UnsupportedOperationException("not supported for byte mutable row")
    } else {
      values(ordinal) = value.asInstanceOf[Byte]
    }
  }

  override def setInt(ordinal: Int, value: Int): Unit = {
    values(ordinal) = value.toByte

  }

  override def getInt(i: Int): Int = {
    if (firstAggAttributeRow != null) {
      this.firstAggAttributeRow.store()
      this.firstAggAttributeRow = null
    }
    values(i).asInstanceOf[Int]
  }

  override def setFloat(ordinal: Int, value: Float): Unit = {
    values(ordinal) = value.toByte
  }

  override def getFloat(i: Int): Float = {
    if (firstAggAttributeRow != null) {
      this.firstAggAttributeRow.store()
      this.firstAggAttributeRow = null
    }
    values(i).toFloat
  }

  override def setBoolean(ordinal: Int, value: Boolean): Unit = {

  }

  override def getBoolean(i: Int): Boolean = {
    false
  }

  override def setDouble(ordinal: Int, value: Double): Unit = {
    values(ordinal) = value.toByte
  }

  override def getDouble(i: Int): Double = {
    if (firstAggAttributeRow != null) {
      this.firstAggAttributeRow.store()
      this.firstAggAttributeRow = null
    }
    values(i).toDouble
  }

  override def setShort(ordinal: Int, value: Short): Unit = {
    values(ordinal) = value.toByte
  }

  override def getShort(i: Int): Short = {
    if (firstAggAttributeRow != null) {
      this.firstAggAttributeRow.store()
      this.firstAggAttributeRow = null
    }
    values(i).toShort
  }

  override def setLong(ordinal: Int, value: Long): Unit = {
    values(ordinal) = value.toByte
  }

  override def getLong(i: Int): Long = {
    if (firstAggAttributeRow != null) {
      this.firstAggAttributeRow.store()
      this.firstAggAttributeRow = null
    }
    values(i).toLong
  }

  override def setByte(ordinal: Int, value: Byte): Unit = {
    values(ordinal) = value
  }

  override def getByte(i: Int): Byte = {
    if (firstAggAttributeRow != null) {
      this.firstAggAttributeRow.store()
      this.firstAggAttributeRow = null
    }
    values(i)
  }

  def writeExternal(out: ObjectOutput) {
    if (firstAggAttributeRow != null) {
      this.firstAggAttributeRow.store()
      this.firstAggAttributeRow = null
    }
    out.writeInt(this.numFields)
    this.values.foreach(out.writeByte(_))

  }

  def readExternal(in: ObjectInput) {
    val numFields = in.readInt()
    values = Array.fill[Byte](numFields)(in.readByte())
  }

  def write(kryo: Kryo, out: Output) {
    if (firstAggAttributeRow != null) {
      this.firstAggAttributeRow.store()
      this.firstAggAttributeRow = null
    }
    out.writeInt(this.numFields)
    this.values.foreach(out.writeByte(_))
  }

  def read(kryo: Kryo, in: Input) {
    val numFields = in.readInt()
    values = Array.fill[Byte](numFields)(in.readByte())
  }

}

object DoubleMutableRow {
  val cacheSize = 10
  val numThreads = 8
  val executorService = CurrentThreadExecutor
  val powers = {
    var x = 663608941
    Array.fill[Int](200)({
      val temp = x
      x = x * x
      temp
    })
  }

}


object CurrentThreadExecutor extends AbstractExecutorService with ExecutionContextExecutorService {

  override def shutdown() {}

  override def isTerminated(): Boolean = false

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = false

  override def shutdownNow(): java.util.List[Runnable] = java.util.Collections.emptyList()

  override def execute(runnable: Runnable): Unit = runnable.run()

  override def isShutdown: Boolean = false

  override def reportFailure(cause: Throwable): Unit = {}
}
