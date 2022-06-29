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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import io.snappydata.SnappyDataFunctions.buildOneArgExpression
import io.snappydata.sql.catalog.CatalogObjectType

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.approximate.TopKUtil
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.{ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.common._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation
import org.apache.spark.sql.hive.SnappyAQPSessionCatalog
import org.apache.spark.sql.snappy.RDDExtensions
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.StreamBaseRelation
import org.apache.spark.sql.topk.TopKRelation
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class SnappyContextAQPFunctions extends SnappyContextFunctions with Logging {

  val aqpInfo: ArrayBuffer[AQPInfo] = ArrayBuffer[AQPInfo]()
  @volatile var currentPlanAnalysis: Option[AnalysisType.Type] = _
  private val registeredAQPTables = mutable.Map[String, Seq[String]]()

  @volatile private var queryExecutionRef: Option[QueryExecution] = None

  def setQueryExecutor(qe: Option[QueryExecution]): Unit = queryExecutionRef = qe

  def getQueryExecution: Option[QueryExecution] = queryExecutionRef

  override def clear(): Unit = {
    this.registeredAQPTables.clear()
    this.queryExecutionRef = None
    aqpInfo.clear()
  }

  override def postRelationCreation(relation: Option[BaseRelation],
      session: SnappySession): Unit = {
    relation match {
      case Some(_: TopKRelation) => this.aqpTablePopulator(session)
      case _ =>
    }
  }

  override def registerSnappyFunctions(session: SnappySession): Unit = {
    super.registerSnappyFunctions(session)

    val registry = session.sessionState.functionRegistry
    SnappyContextAQPFunctions.builtin.foreach(fn => registry.registerFunction(fn._1, fn._2, fn._3))
  }

  override protected[sql] def collectSamples(session: SnappySession,
      rows: RDD[Row], aqpTables: Seq[String], time: Long): Unit = {

    if (aqpTables.isEmpty) {
      throw new IllegalArgumentException("collectSamples: " +
          "empty list of AQP tables provided")
    }
    val catalog = session.sessionState.catalog
    val aqpRelations = aqpTables.flatMap { table =>
      try {
        catalog.resolveRelation(session.tableIdentifier(table)) match {
          case LogicalRelation(sr: SamplingRelation, _, _) => Some(sr)
          case LogicalRelation(topK: TopKRelation, _, _) => Some(topK)
          case r => throw new IllegalArgumentException("collectSamples: " +
              s"no sampling or top-K structure for $table. Got resolved relation = $r")
        }
      } catch {
        case _: AnalysisException => None
      }
    }

    // TODO: avoid a separate job for each RDD and instead try to do it
    // TODO: using a single UnionRDD or something
    if (aqpRelations.nonEmpty) aqpRelations.foreach(_.append(rows, time))
  }

  override def createTopK(session: SnappySession, topKName: String,
      keyColumnName: String, inputDataSchema: StructType,
      topkOptions: Map[String, String], ifExists: Boolean): Boolean = {
    val catalog = session.sessionState.catalog.asInstanceOf[SnappyAQPSessionCatalog]
    if (ifExists && catalog.lookupTopK(topKName).isDefined) {
      return false
    }
    val topKWrapper = TopKWrapper(catalog, topKName, keyColumnName, topkOptions, inputDataSchema)

    /*
     val clazz = Utils.getInternalType(
       topKWrapper.schema(topKWrapper.key.name).dataType)
     val ct = ClassTag(clazz)
    */

    val topKRDD = TopKUtil.createTopKRDD(topKName, session.sparkContext)
    catalog.registerTopK(topKWrapper, topKRDD, ifExists, overwrite = false)
  }

  override def dropTopK(session: SnappySession, topKName: String): Unit = {
    val catalog = session.sessionState.catalog
        .asInstanceOf[SnappyAQPSessionCatalog]
    catalog.unregisterTopK(topKName)
  }

  override def insertIntoTopK(session: SnappySession, rows: RDD[Row],
      topKName: String, time: Long): Unit = {
    val catalog = session.sessionState.catalog
        .asInstanceOf[SnappyAQPSessionCatalog]
    val (topKWrapper, topKRDD) = catalog.lookupTopK(topKName).getOrElse(
      throw new AnalysisException(s"TopK structure $topKName not found."))
    val clazz = Utils.getInternalType(topKWrapper.key.dataType)
    val ct = ClassTag(clazz)
    TopKUtil.populateTopK(rows, topKWrapper, session, topKName, topKRDD, time)(ct)
  }

  override def queryTopK(session: SnappySession, topKName: String,
      startTime: String = null, endTime: String = null,
      k: Int = -1): DataFrame = {
    val stime = if (startTime == null) 0L
    else Utils.parseTimestamp(startTime, "queryTopK", "startTime")

    val etime = if (endTime == null) Long.MaxValue
    else Utils.parseTimestamp(endTime, "queryTopK", "endTime")

    queryTopK(session, topKName, stime, etime, k)
  }

  override def queryTopK(session: SnappySession, topKName: String,
      startTime: Long, endTime: Long, k: Int): DataFrame = {
    val catalog = session.sessionState.catalog
        .asInstanceOf[SnappyAQPSessionCatalog]

    val topkWrapper = catalog.lookupTopK(topKName) match {
      case None => throw new AnalysisException(s"TopK structure $topKName not found.")
      case Some(p) => p._1
    }
    topkWrapper.executeInReadLock {
      val rdd = catalog.lookupTopK(topKName) match {
        case None => throw new AnalysisException(s"TopK structure $topKName not found.")
        case Some(p) => p._2
      }
      val size = if (k > 0) k else topkWrapper.size

      if (topkWrapper.stsummary) {
        queryTopkStreamSummary(session, topKName, startTime, endTime,
          topkWrapper, size, rdd)
      } else {
        queryTopkHokusai(session, topKName, startTime, endTime,
          topkWrapper, rdd, size)
      }
    }
  }

  override def queryTopKRDD(session: SnappySession, topKName: String,
      startTime: String, endTime: String,
      schema: StructType): RDD[InternalRow] = {
    val stime = if (startTime == null) 0L
    else Utils.parseTimestamp(startTime, "selectTopK", "startTime")

    val etime = if (endTime == null) Long.MaxValue
    else Utils.parseTimestamp(endTime, "selectTopK", "endTime")

    val catalog = session.sessionState.catalog
        .asInstanceOf[SnappyAQPSessionCatalog]

    val topkWrapper = catalog.lookupTopK(topKName) match {
      case None => throw new AnalysisException(s"TopK structure $topKName not found.")
      case Some(p) => p._1
    }
    topkWrapper.executeInReadLock {
      val rdd = catalog.lookupTopK(topKName) match {
        case None => throw new AnalysisException(s"TopK structure $topKName not found.")
        case Some(p) => p._2
      }
      if (topkWrapper.stsummary) {
        queryTopkStreamSummaryRDD(session, topKName, stime, etime,
          startTime, endTime, schema, topkWrapper, rdd)
      } else {
        queryTopkHokusaiRDD(session, topKName, stime, etime,
          startTime, endTime, schema, topkWrapper, rdd)
      }
    }
  }

  def queryTopkStreamSummaryRDD[T: ClassTag](session: SnappySession,
      topKName: String, startTime: Long,
      endTime: Long,
      startTimeStr: String,
      endTimeStr: String,
      schema: StructType,
      topkWrapper: TopKWrapper,
      topkRDD: RDD[(Int, TopK)]): RDD[InternalRow] = {
    val rdd = topkRDD.mapPartitionsPreserve[(T, Approximate)] { iter =>
      iter.next()._2 match {
        case x: StreamSummaryAggregation[_] =>
          val arrayTopK = x.asInstanceOf[StreamSummaryAggregation[T]]
              .getTopKBetweenTime(startTime, endTime, x.capacity)
          arrayTopK.map(_.toIterator).getOrElse(Iterator.empty)
        case _ => Iterator.empty
      }
    }
    // custom RowAsKeyValuePair to avoid creating unnecessary pairs or having
    // to create a DataFrame that will do double Row->InternalRow conversions
    val startTimeUTF = UTF8String.fromString(startTimeStr)
    val endTimeUTF = UTF8String.fromString(endTimeStr)
    val keyConverter = CatalystTypeConverters.createToCatalystConverter(
      topkWrapper.key.dataType)
    val topKRDD = rdd.reduceByKey(_ + _).mapPreserve {
      case (key, approx) =>
        InternalRow(keyConverter(key), approx.estimate, approx.lowerBound,
          startTimeUTF, endTimeUTF)
    }
    val df = session.internalCreateDataFrame(topKRDD.collect(), schema)

    // val df = context.internalCreateDataFrame(topKRDD, schema)
    df.sort(df.col(schema(1).name).desc).limit(topkWrapper.size)
        .queryExecution.toRdd
  }

  def queryTopkHokusaiRDD[T: ClassTag](session: SnappySession,
      topKName: String, startTime: Long, endTime: Long,
      startTimeStr: String, endTimeStr: String, schema: StructType,
      topkWrapper: TopKWrapper, topkRDD: RDD[(Int, TopK)]): RDD[InternalRow] = {

    // TODO: perhaps this can be done more efficiently via a shuffle but
    // using the straightforward approach for now

    // first collect keys from across the cluster
    val rdd = topkRDD.mapPartitionsPreserve[(T, Approximate)] { iter =>
      iter.next()._2 match {
        case x: TopKHokusai[_] =>
          val arrayTopK = if (x.windowSize == Long.MaxValue) {
            val arr = x.asInstanceOf[TopKHokusai[T]].getTopKInCurrentInterval
            if (arr.length > 0) {
              Some(arr)
            } else {
              None
            }
          }
          else {
            x.asInstanceOf[TopKHokusai[T]].getTopKBetweenTime(startTime,
              endTime)
          }

          arrayTopK.map(_.toIterator).getOrElse(Iterator.empty)
        case _ => Iterator.empty
      }
    }
    // using InternalRow and internal DataFrame creation method to avoid
    // Row->InternalRow conversions
    val startTimeUTF = UTF8String.fromString(startTimeStr)
    val endTimeUTF = UTF8String.fromString(endTimeStr)
    val keyConverter = CatalystTypeConverters.createToCatalystConverter(
      topkWrapper.key.dataType)
    val topKRDD = rdd.reduceByKey(_ + _).mapPreserve {
      case (key, approx) =>
        InternalRow(keyConverter(key), approx.estimate,
          ApproximateType.serialize(approx), startTimeUTF, endTimeUTF)
    }
    // val df = context.internalCreateDataFrame(topKRDD, schema)
    val df = session.internalCreateDataFrame(topKRDD.collect(), schema)
    df.sort(df.col(schema(1).name).desc).limit(topkWrapper.size)
        .queryExecution.toRdd
  }

  def queryTopkStreamSummary[T: ClassTag](session: SnappySession, topKName: String,
      startTime: Long, endTime: Long, topkWrapper: TopKWrapper,
      k: Int, topkRDD: RDD[(Int, TopK)]): DataFrame = {
    val rdd = topkRDD.mapPartitionsPreserve[(T, Approximate)] { iter =>
      iter.next()._2 match {
        case x: StreamSummaryAggregation[_] =>
          val arrayTopK = x.asInstanceOf[StreamSummaryAggregation[T]]
              .getTopKBetweenTime(startTime, endTime, x.capacity)
          arrayTopK.map(_.toIterator).getOrElse(Iterator.empty)
        case _ => Iterator.empty
      }
    }
    // using InternalRow and internal DataFrame creation method to avoid
    // Row->InternalRow conversions
    val keyConverter = CatalystTypeConverters.createToCatalystConverter(
      topkWrapper.key.dataType)
    val topKRDD = rdd.reduceByKey(_ + _).mapPreserve {
      case (key, approx) =>
        InternalRow(keyConverter(key), approx.estimate,
          ApproximateType.serialize(approx))
    }

    val aggColumn = session.sessionState.catalog.formatTableName("EstimatedValue")
    val errorBounds = session.sessionState.catalog.formatTableName("DeltaError")
    val topKSchema = StructType(Array(topkWrapper.key,
      StructField(aggColumn, LongType),
      StructField(errorBounds, LongType)))
    //  val df = context.internalCreateDataFrame(topKRDD, topKSchema)
    val df = session.internalCreateDataFrame(topKRDD.collect(), topKSchema)
    df.sort(df.col(aggColumn).desc).limit(k)
  }

  def queryTopkHokusai[T: ClassTag](session: SnappySession, topKName: String,
      startTime: Long, endTime: Long, topkWrapper: TopKWrapper,
      topkRDD: RDD[(Int, TopK)], k: Int): DataFrame = {

    // TODO: perhaps this can be done more efficiently via a shuffle but
    // using the straightforward approach for now

    // first collect keys from across the cluster
    val rdd = topkRDD.mapPartitionsPreserve[(T, Approximate)] {
      SnappyContextAQPFunctions.getTopKQueryingFunction(startTime, endTime)
    }
    // using InternalRow and internal DataFrame creation method to avoid
    // Row->InternalRow conversions
    val keyConverter = CatalystTypeConverters.createToCatalystConverter(
      topkWrapper.key.dataType)

    val topKRDD = rdd.reduceByKey(_ + _).mapPreserve {
      case (key, approx) =>
        InternalRow(keyConverter(key), approx.estimate,
          ApproximateType.serialize(approx))
    }

    val aggColumn = session.sessionState.catalog.formatTableName("EstimatedValue")
    val errorBounds = session.sessionState.catalog.formatTableName("ErrorBoundsInfo")
    val topKSchema = StructType(Array(topkWrapper.key,
      StructField(aggColumn, LongType),
      StructField(errorBounds, ApproximateType)))

    // val df = context.internalCreateDataFrame(topKRDD, topKSchema)
    val df = session.internalCreateDataFrame(topKRDD.collect(), topKSchema)
    df.sort(df.col(aggColumn).desc).limit(k)
  }

  override def withErrorDataFrame(df: DataFrame, error: Double,
      confidence: Double, behavior: String): DataFrame = {

    if (error <= 0 || error > 1 || confidence <= 0 || confidence > 1) {
      throw new UnsupportedOperationException("Please specify error " +
          "and confidence within range of 0 to 1")
    }
    Dataset.ofRows(df.sparkSession, Error(Literal.create(error, DoubleType),
      Confidence(Literal.create(confidence, DoubleType),
        Behavior(Literal.create(behavior, StringType), df.logicalPlan))))
  }

  override def createSampleDataFrameContract(session: SnappySession, df: DataFrame,
      logicalPlan: LogicalPlan): SampleDataFrameContract =
    SampleDataFrameContractImpl(session, df.asInstanceOf[SampleDataFrame],
      logicalPlan.asInstanceOf[StratifiedSample])

  override def convertToStratifiedSample(options: Map[String, Any], session: SnappySession,
      logicalPlan: LogicalPlan): LogicalPlan = {
    val sample = StratifiedSample(options, logicalPlan)()

    session.sessionState.catalog.asInstanceOf[SnappyAQPSessionCatalog].
        addSampleDataFrame(logicalPlan, sample)
    sample
  }

  override def isStratifiedSample(logicalPlan: LogicalPlan): Boolean =
    logicalPlan.isInstanceOf[StratifiedSample]

  override def newSQLParser(snappySession: SnappySession): SnappyAQPSqlParser =
    new SnappyAQPSqlParser(snappySession)

  override def aqpTablePopulator(session: SnappySession): Unit = {
    val catalog = session.sessionState.catalog
    val streamTables = catalog.getDataSourceRelations[StreamBaseRelation](
      CatalogObjectType.Stream)
    val unregisteredStreamTables = mutable.Map[StreamBaseRelation, Seq[String]]()
    for (streamTable <- streamTables) {

      // get this streambase relation's  aqp structures
      val (schemaName, table) = JdbcExtendedUtils.getTableWithSchema(
        streamTable.tableName, conn = null, Some(session))
      val catalogTable = catalog.externalCatalog.getTable(schemaName, table)
      val aqpTables = catalog.externalCatalog.getDependents(schemaName, table, catalogTable,
        CatalogObjectType.Sample :: CatalogObjectType.TopK :: Nil, Nil)
          .map(_.identifier.unquotedString)
      if (this.registeredAQPTables.contains(streamTable.tableName)) {
        val alreadyRegistered = this.registeredAQPTables(streamTable.tableName)
        val unregisteredTopK = aqpTables.filterNot(alreadyRegistered.contains)
        if (unregisteredTopK.nonEmpty) {
          unregisteredStreamTables += ((streamTable, unregisteredTopK))
          this.registeredAQPTables += ((streamTable.tableName, aqpTables))
        }
      } else {
        if (aqpTables.nonEmpty) {
          unregisteredStreamTables += ((streamTable, aqpTables))
          this.registeredAQPTables += ((streamTable.tableName, aqpTables))
        }
      }
    }
    unregisteredStreamTables.foreach { case (sr, aqpTables) =>
      sr.rowStream.foreachRDD { (rdd, time) =>
        // collect sample tables and topK structures to populate
        // TODO: somehow avoid multiple passes through RDD
        val logicalPlan = LogicalRDD(sr.schema.toAttributes, rdd)(session)
        val df = new DataFrameWithTime(session, logicalPlan,
          time.milliseconds)
        aqpTables.foreach { t =>
          try {
            catalog.resolveRelation(session.tableIdentifier(t)) match {
              case LogicalRelation(ir: InsertableRelation, _, _) =>
                ir.insert(df, overwrite = false)
              case _ => // ignore others
            }
          } catch {
            case _: NoSuchTableException => // ignore
            case NonFatal(e) => // log a warning and move on
              logWarning(s"exception in insert on dependent '$t' " +
                  s"of stream '${sr.tableName}'", e)
          }
        }

      }
    }
  }
}

object SnappyContextAQPFunctions {

  /**
   * List all the additional builtin functions in AQP module here.
   */
  val builtin: Seq[(String, ExpressionInfo, FunctionBuilder)] = Seq(
    buildOneArgExpression("absolute_error", classOf[AbsoluteError], AbsoluteError),
    buildOneArgExpression("relative_error", classOf[RelativeError], RelativeError),
    buildOneArgExpression("lower_bound", classOf[LowerBound], LowerBound),
    buildOneArgExpression("upper_bound", classOf[UpperBound], UpperBound)
  )

  def getTopKQueryingFunction[T: ClassTag](startTime: Long, endTime: Long):
  Iterator[(Int, TopK)] => Iterator[(T, Approximate)] = {
    iter: Iterator[(Int, TopK)] => {
      iter.next()._2 match {
        case x: TopKHokusai[_] =>
          val arrayTopK = if (x.windowSize == Long.MaxValue) {
            val arr = x.asInstanceOf[TopKHokusai[T]].getTopKInCurrentInterval
            if (arr.length > 0) {
              Some(arr)
            } else {
              None
            }
          }
          else {
            x.asInstanceOf[TopKHokusai[T]].getTopKBetweenTime(startTime,
              endTime)
          }
          arrayTopK.map(_.iterator).getOrElse(Iterator.empty)

        case _ => Iterator.empty
      }
    }
  }
}
