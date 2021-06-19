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
package org.apache.spark.sql.sampling

import scala.collection.mutable

import com.gemstone.gemfire.internal.cache.{AbstractRegion, ExternalTableMetaData, LocalRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.Misc._
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}
import io.snappydata.{Constant, Property}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Project}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, ColumnFormatRelation}
import org.apache.spark.sql.execution.columnar.{ColumnBatchCreator, ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.execution.common.AnalysisType
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.execution.row.RowFormatRelation
import org.apache.spark.sql.execution.{BucketsBasedIterator, LeafExecNode, PartitionedDataSourceScan, SampleOptions, SparkPlan, StratifiedSampler}
import org.apache.spark.sql.internal.SnappyAQPSessionState
import org.apache.spark.sql.row.JDBCMutableRelation
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types._
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

case class ColumnFormatSamplingRelation(
    override val schema: StructType,
    override val baseRelation: BaseColumnFormatRelation,
    @transient override val sqlContext: SQLContext,
    override val samplingOptions: Map[String, String],
    origOptions: Map[String, String],
    sampleTable: String,
    baseSchema: StructType,
    override val baseTable: Option[String],
    reservoirRegionName: String,
    basePartitioningColumns: Seq[String],
    baseNumPartition: Int)
    extends SamplingRelation
    with PartitionedDataSourceScan
    with SchemaInsertableRelation
    with DestroyRelation
    with Logging
    with Serializable {

  private val (qcsP, qcsSparkPlan) = resolveQCS(samplingOptions,
    schema.fieldNames, "ColumnFormatSamplingRelation", schema, baseSchema)

  override def table: String = baseRelation.table

  def getExternalStoreMetaData: ExternalTableMetaData = {
    ExternalStoreUtils.getExternalTableMetaData(table)
  }

  lazy val options: SampleOptions = StratifiedSampler.parseOptions(
    samplingOptions, qcsP.map(_._1).orNull, sampleTable,
    Property.FlushReservoirThreshold.get(sqlContext.conf),
    schema, qcsSparkPlan.map(_._1))

  override def canBeOnBuildSide: Boolean = {
    val currentPlanAnaysis = this.sqlContext.sessionState.
      asInstanceOf[SnappyAQPSessionState].contextFunctions.currentPlanAnalysis
    if (currentPlanAnaysis != null) {
      currentPlanAnaysis.map(x => x == AnalysisType.Bootstrap ||
        x == AnalysisType.ByPassErrorCalc).getOrElse(true)
    } else {
      // ??
      false
    }
  }
  def getColocatedTable: Option[String] = if (isCopartitionedWithBaseTable) {
    baseRelation.getColocatedTable
  } else None

  override def sizeInBytes: Long = baseRelation.sizeInBytes

  override def toString: String = s"${getClass.getSimpleName}[${Utils.toLowerCase(sampleTable)}]"

  private[sql] def tableCreated: Boolean = baseRelation.tableCreated

  override def qcs: Array[String] = qcsP.map(_._2).getOrElse(qcsSparkPlan.get._2)

  override val needConversion: Boolean = false

  // TODO: somehow use partitioning of baseRelation to avoid exchange
  // on that RDD while only reservoirRDD should be exchanged
  override def numBuckets: Int = 0

  override def partitionColumns: Seq[String] = Nil

  override def connectionType: ConnectionType.Value =
    baseRelation.connectionType

  override val isReservoirAsRegion: Boolean = sqlContext.conf.getConfString(Constant
      .RESERVOIR_AS_REGION, "true").toBoolean

  override val isPartitioned: Boolean = true

  private val isCopartitionedWithBaseTable = if (isReservoirAsRegion &&
    (basePartitioningColumns.nonEmpty || baseNumPartition > 0)) {
    baseRelation match {
      case c: ColumnFormatRelation => baseNumPartition == c.numBuckets
      case _ => false
    }
  } else false


  @transient override lazy val region: LocalRegion =
    Misc.getRegionForTable(sampleTable, true).asInstanceOf[LocalRegion]

  final val PARTITIONBUFSIZE = 10

  override def unhandledFilters(filters: Seq[Expression]): Seq[Expression] = filters

  override def buildUnsafeScan(requiredColumns: Array[String],
      filters: Array[Expression]): (RDD[Any], Seq[RDD[InternalRow]]) = {
    val (colRDD, rowRDD, others) = baseRelation.buildUnsafeScanForSampledRelation(
      requiredColumns, filters)

    if (isReservoirAsRegion) {
      val reservoirRDD = new SampledCachedRDD(sqlContext.sparkContext,
        Utils.getAllExecutorsMemoryStatus(_).keys, sampleTable,
        options.concurrency, getTotalCores, colRDD.partitions)
      val zippedRDD = reservoirRDD. zipPartitions(rowRDD, colRDD) { (first, second, third) =>
        Iterator[Any](first, second, third)
      }
      (zippedRDD, others)
    } else {
      val reservoirRDD = new SampledCachedRDD(sqlContext.sparkContext,
        Utils.getAllExecutorsMemoryStatus(_).keys, sampleTable,
        options.concurrency, getTotalCores, Array.empty[Partition])
      val zipped = rowRDD. zipPartitions(colRDD) { (first, second) =>
        BucketsBasedIterator(first, second)}
      (zipped, others.+:(reservoirRDD))
    }

    // projection is now done by generated code
    // (colRDD.asInstanceOf[RDD[Any]],
    //    Seq(doScan(reservoirRDD, requiredColumns, filters)))

  }

  private def resolveQCS(samplingOptions: Map[String, String],
      fieldNames: Array[String], module: String, schema: StructType,
      baseSchema: StructType): (Option[(Array[Int], Array[String])],
      Option[(SparkPlan, Array[String])]) = {
    try {
      (Some(Utils.resolveQCS(samplingOptions, schema.fieldNames, module)), None)
    } catch {
      case ex: AnalysisException =>

        Utils.matchOption("qcs", samplingOptions).map(_._2).map {
          case qs: String =>
            if (qs.isEmpty) throw ex
            else (None, Some(createQCSPlan(qs, baseSchema)))
          case qa: Array[String] =>
            val sb = new StringBuilder
            qa.addString(sb, ",")
            (None, Some(createQCSPlan(sb.toString(), baseSchema)))
          case _ => throw ex
        }.getOrElse(throw ex)
      case th: Throwable => throw th
    }

  }

  private def createQCSPlan(projections: String,
      baseSchema: StructType): (SparkPlan, Array[String]) = {
    val sql = s"select $projections from RowToSample"
    val baseAttribs = baseSchema.toAttributes
    val logicalPlan = this.sqlContext.sessionState.sqlParser.parsePlan(sql).transformUp {
      case _: UnresolvedRelation => QcsLogicalPlan(baseAttribs)
    }
    val qe = this.sqlContext.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()

    val projOnTable = qe.analyzed.find {
      case Project(_, child) => child match {
        case _: QcsLogicalPlan => true
        case _ => false
      }
      case _ => false
    }.get.asInstanceOf[Project]


    // val baseExprIds = baseAttribs.map(_.exprId)
    /*
    val rootAttribs = projOnTable.projectList.flatMap { ne =>
      ne.collect {
        case ar: AttributeReference => ar
      }

    }
    val qcsNames = rootAttribs.map(ne => {
      baseAttribs.find(x => x.exprId == ne.exprId).get.name
    }).toArray
    */
    val qcsNames = new mutable.ArrayBuffer[String]
    projOnTable.projectList.foreach(e => qcsNames += e.name)
    // (qe.executedPlan, rowToInRowConverter, realQCS, realDataTypes, realQCS.length)
    (qe.executedPlan, qcsNames.toArray)
  }

  private def getTotalCores: Int = sqlContext.sparkContext.schedulerBackend
      .defaultParallelism()

  /*
  protected def doScan(reservoirRDD: RDD[InternalRow],
      requiredColumns: Array[String],
      filters: Array[Filter]): RDD[InternalRow] = {

    val schemaFields = Utils.schemaFields(schema)
    val requestedSchema = ExternalStoreUtils.pruneSchema(schemaFields,
      requiredColumns)
    val columnIndicesAndFields = ExternalStoreUtils.columnIndicesAndDataTypes(
      requestedSchema, schema)

    reservoirRDD.mapPartitionsPreserve { rows =>
      val projection = UnsafeProjection.create(columnIndicesAndFields.map(
        f => BoundReference(f._1, f._2.dataType, f._2.nullable)))
      rows.map(projection)
    }
  }
  */

  override def insertableRelation(
      sourceSchema: Seq[Attribute]): Option[InsertableRelation] = {
    val isByPass = samplingOptions.getOrElse(Constant.keyBypassSampleOperator, "false").toBoolean
    if ((isByPass && (sourceSchema.size - baseSchema.size) == 1)
        || (!isByPass && sourceSchema.size == baseSchema.size)) {
      if (isByPass) {
        Some(this)
      } else if (schema(schema.length - 1).name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME)) {
        // remove WEIGHTAGE column since data to be inserted will not have it
        Some(copy(schema = StructType(schema.fields.dropRight(1))))
      } else {
        Some(this)
      }
    } else None
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val bypassSampling = samplingOptions.get(Constant.keyBypassSampleOperator) match {
      case Some(x) => x.toBoolean
      case None => false
    }
    if (overwrite) {
      truncate()
    }
    if (bypassSampling) {
      this.baseRelation.insert(data, overwrite)
    } else {
      append(data.rdd)
    }
  }

  override def append(rows: RDD[Row], time: Long): Unit = {
    // for inserts, the copy of relation returned by insertableRelation
    // will not have the weightage column in schema, so add it
    var options = this.options
    val schema = options.schema
    if (!schema(schema.length - 1).name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME)) {
      options = options.copy(schema = schema.add(Utils.WEIGHTAGE_COLUMN_NAME,
        LongType, nullable = false))
    }

    def getDefaultIterator[U](rowIterator: Iterator[Row]): Iterator[U] = {
      getIterator[U](sampleIsPartitioned = false)(-1, rowIterator: Iterator[Row])
    }

    def getIterator[U](sampleIsPartitioned: Boolean)(partIndex: Int,
        rowIterator: Iterator[Row]): Iterator[U] = {
      val sampler = StratifiedSampler(options, cached = true,
        reservoirRegionName, sampleIsPartitioned)
      val externalTableMetaData = getExternalStoreMetaData
      val batchCreator = new ColumnBatchCreator(
        baseRelation.region.asInstanceOf[PartitionedRegion],
        baseRelation.table, baseRelation.externalColumnTableName,
        sampler.schema, baseRelation.externalStore, externalTableMetaData.compressionCodec)
      val buffer = batchCreator.createColumnBatchBuffer(
        // externalTableMetaData.columnBatchSize,
        // Fix for AQP-273, for sample table, the strata needs to remain as a single entry
        -1,
        externalTableMetaData.columnMaxDeltaRows)

      val rowEncoder = RowEncoder(sampler.schema)
      val rowCount = sampler.append(rowIterator, (),
        (_: Unit, row: InternalRow) => buffer.appendRow(row),
        (_: Unit, bucketId: Int) => buffer.startRows(bucketId),
        (_: Unit) => buffer.endRows(), rowEncoder, partIndex)
      EmptyIteratorWithRowCount(rowCount)
    }

    def getPreferredLocationsForBucketId(bucketId: Int): Seq[String] = {
      val reservoirRegion = Misc.getReservoirRegionForSampleTable(reservoirRegionName)
      val prefNodes = StoreUtils.getBucketPreferredLocations(reservoirRegion,
        bucketId, forWrite = true)
      if (prefNodes.nonEmpty) prefNodes
      else {
        StoreUtils.getBucketPreferredLocations(reservoirRegion,
          bucketId, forWrite = false)
      }
    }

    val sampled = if (isCopartitionedWithBaseTable) {

      val ds = sqlContext.createDataFrame(rows, baseSchema)
      val cols = basePartitioningColumns.map(s => ds.col(s))
      val partitionedrows = ds.repartition(baseNumPartition, cols: _*).rdd
      partitionedrows.mapPartitionsWithIndexPreserveLocations(getIterator(
        sampleIsPartitioned = true), getPreferredLocationsForBucketId)
    } else {
     /* val ds = sqlContext.createDataFrame(rows, baseSchema)
      val cols = this.qcs.map(ds.col(_))
      val partitionedrows = ds.repartition(
        this.baseRelation.asInstanceOf[ColumnFormatRelation].numBuckets, cols: _*).rdd

      partitionedrows.mapPartitionsWithIndexPreserveLocations(getIterator(
        sampleIsPartitioned = false), getPreferredLocationsForBucketId)
        */
      rows.mapPartitionsPreserve(getDefaultIterator)
    }

    // materialize it
    val insertedRowCount = sampled.count()

    // flush the reservoir
    if (insertedRowCount > Property.FlushReservoirThreshold.get(sqlContext.conf)) {
      flushReservoir()
    }

    // flush the row buffer
    baseRelation.flushRowBuffer()
  }

  /**
   * Destroy and cleanup this relation. It may include, but not limited to,
   * dropping the external table that this relation represents.
   */
  override def destroy(ifExists: Boolean): Unit = {
    try {
      try {
        if (isReservoirAsRegion) {
          SampleTableQuery.createOrDropReservoirRegion(
            sampleTable, baseRelation, reservoirRegionName, isDrop = true)
        }
      } finally {
        baseRelation.destroy(ifExists)
      }
    } finally {
      // In case of reservoir saved as a region, sampler is removed as part of
      // createOrDropReservoirRegion call.
      if (!isReservoirAsRegion) {
        try {
          Utils.mapExecutors[Unit](sqlContext.sparkContext, () => {
            StratifiedSampler.removeSampler(sampleTable, markFlushed = false)
            Iterator.empty
          })
        } finally {
          StratifiedSampler.removeSampler(sampleTable, markFlushed = false)
        }
      }
      GemFireContainerInvalidator.invalidateExtraTableInfo(this.baseTable, sqlContext)
    }
  }

  /**
   * Truncate the table represented by this relation.
   */
  override def truncate(): Unit = {
    try {
      baseRelation.truncate()
    } finally {
      try {
        Utils.mapExecutors[Unit](sqlContext.sparkContext, () => {
          StratifiedSampler.removeSampler(sampleTable, markFlushed = false)
          Iterator.empty
        })
      } finally {
        StratifiedSampler.removeSampler(sampleTable, markFlushed = false)
      }
    }
  }

  def flushReservoir(): Unit = {
    // materialize options before referencing in mapExecutors on executors
    val options = this.options
    val isPartitioned = this.isCopartitionedWithBaseTable
    SnappyContext.getClusterMode(sqlContext.sparkContext) match {
      case SnappyEmbeddedMode(_, _) | LocalMode(_, _) =>
        // force flush all the buckets into the column store
        Utils.mapExecutors[Unit](sqlContext.sparkContext, () => {
          val externalTableMetaData = getExternalStoreMetaData
          val sampler = StratifiedSampler(options, cached = true,
            reservoirRegionName, isPartitioned)
          val batchCreator = new ColumnBatchCreator(
            baseRelation.region.asInstanceOf[PartitionedRegion],
            baseRelation.table, baseRelation.externalColumnTableName,
            sampler.schema, baseRelation.externalStore, externalTableMetaData.compressionCodec)
          val buffer = batchCreator.createColumnBatchBuffer(
            // externalTableMetaData.columnBatchSize,
            // Fix for AQP-273, for sample table, the strata needs to remain as a single entry
            -1,
            externalTableMetaData.columnMaxDeltaRows)
          sampler.flushReservoir((),
            (_: Unit, row: InternalRow) => buffer.appendRow(row),
            (_: Unit, bucketId: Int) => buffer.startRows(bucketId),
            (_: Unit) => buffer.endRows())
          Iterator.empty
        })
      case _ =>
    }
  }
}

final class DefaultSource
    extends RelationProvider
        with SchemaRelationProvider
        with CreatableRelationProvider with DataSourceRegister {

  override def shortName(): String = SnappyContext.SAMPLE_SOURCE

  private[sql] def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schemaOpt: Option[StructType],
      asSelect: Boolean): ColumnFormatSamplingRelation = {
    val parameters = new CaseInsensitiveMutableHashMap(options)
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val catalog = session.sessionCatalog
    val baseTable = parameters.remove(SnappyExternalCatalog.BASETABLE_PROPERTY)
        .map(catalog.resolveExistingTable(_).unquotedString)
    val samplingOptions = ExternalStoreUtils.removeSamplingOptions(parameters)
    val colTableParams = new CaseInsensitiveMutableHashMap(parameters)
    val table = ExternalStoreUtils.removeInternalPropsAndGetTable(parameters)
    // at least one of schema or baseTable is required
    val trimmedSchema = schemaOpt.map { s =>
      if (s(s.length - 1).name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME)) {
        StructType(s.fields.dropRight(1))
      } else s
    }
    val (schema, baseTablePlanOpt) = Utils.getSchemaAndPlanFromBase(
      trimmedSchema, baseTable, catalog, asSelect, table, CatalogObjectType.Sample.toString)
    val newSchema = schema.add(Utils.WEIGHTAGE_COLUMN_NAME, LongType,
      nullable = false)

    val (basePartitioningColumns: Seq[String],
    baseNumPartition: Int) = baseTablePlanOpt match {
      case Some(baseTablePlan) => baseTablePlan match {
        case LogicalRelation(cr: ColumnFormatRelation, _, _) =>
          val region = Misc.getRegionForTable(cr.resolvedName, true)
          region.asInstanceOf[AbstractRegion] match {
            case _: PartitionedRegion => (cr.partitionColumns, cr.numBuckets)
            case _ => (Nil, 0)
          }
        case LogicalRelation(rr: RowFormatRelation, _, _) =>
          val region = Misc.getRegionForTable(rr.resolvedName, true)
          region.asInstanceOf[AbstractRegion] match {
            case _: PartitionedRegion => (rr.partitionColumns, rr.numBuckets)
            case _ => (Nil, 0)
          }
        case _ => (Nil, 0)
      }
      case None => (Nil, 0)
    }

    val basePropsOption = baseTablePlanOpt match {
      case Some(baseTablePlan) =>
        snappy.unwrapSubquery(baseTablePlan) match {
          case LogicalRelation(r: JDBCMutableRelation, _, _) => Some(new
              CaseInsensitiveMutableHashMap(r.origOptions))
          case LogicalRelation(r: BaseColumnFormatRelation, _, _) => Some(new
              CaseInsensitiveMutableHashMap(r.origOptions))
          case _ => None
        }
      case _ => None
    }

    basePropsOption match {
      case Some(props) if props.contains(ExternalStoreUtils.PARTITION_BY) &&
        !props.contains(ExternalStoreUtils.REPLICATE) =>
        Array(ExternalStoreUtils.BUCKETS, StoreUtils.PERSISTENCE,
          StoreUtils.PERSISTENT, StoreUtils.REDUNDANCY).foreach(
          prop => {
          props.get(prop) match {
            case Some(x) if !colTableParams.contains(prop) => colTableParams += prop -> x
            case None if !colTableParams.contains(prop) => if (prop.equalsIgnoreCase(
              ExternalStoreUtils.BUCKETS) && baseNumPartition > 0) {
              // noinspection RedundantDefaultArgument
              val buckets = ExternalStoreUtils.getAndSetTotalPartitions(session, props,
                forManagedTable = false, forSampleTable = false)
              StoreUtils.getAndSetPartitioningAndKeyColumns(session, schema, props)
              colTableParams += prop -> buckets.toString

            }
            case _ =>
          }
        })
      case _ =>
    }

    // By default, eviction was always disabled for sample tables.
    // Now using EVICTION_BY option instead of OVERFLOW to indicate the same, to be more intuitive.
    if (!colTableParams.contains(StoreUtils.EVICTION_BY)) {
      colTableParams += StoreUtils.EVICTION_BY -> StoreUtils.NONE
    }

    val tableOptions = colTableParams.toMap

    val resolved = DataSource(
      sqlContext.sparkSession,
      className = SnappyParserConsts.COLUMN_SOURCE,
      userSpecifiedSchema = Some(newSchema),
      options = tableOptions).resolveRelation().asInstanceOf[ColumnFormatRelation]

    var success = false
    var reservoirRegionName: String = null
    try {
      reservoirRegionName =
        if (sqlContext.conf.getConfString(Constant.RESERVOIR_AS_REGION,
          "true").toBoolean) {
          val (schemaName, tableName) = JdbcExtendedUtils.getTableWithSchema(table,
            conn = null, session = Some(session), forSpark = false)
          val reservoirName = Misc.getReservoirRegionNameForSampleTable(
            schemaName, schemaName + '.' + tableName)
          // Create the reservoir region using system procedure that will be persisted
          // and get created on all members including any new ones in future.
          // Always attempt to create for old catalog data (indicated by RELATION_FOR_SAMPLE).
          if (resolved.tableCreated ||
              colTableParams.contains(ExternalStoreUtils.RELATION_FOR_SAMPLE)) {
            SampleTableQuery.createOrDropReservoirRegion(
              table, resolved, reservoirName, isDrop = false)
          }
          reservoirName
        } else null
      val relation = ColumnFormatSamplingRelation(newSchema, resolved,
        sqlContext, samplingOptions, options, table, schema, baseTable,
        reservoirRegionName, basePartitioningColumns, baseNumPartition)
      success = true
      relation
    } finally {
      // This clean up is done if column table creation is successful
      // and other operations subsequent to it failed.
      if (!success && resolved.tableCreated) {
        // destroy reservoir region first
        if (reservoirRegionName ne null) {
          SampleTableQuery.createOrDropReservoirRegion(
            table, resolved, reservoirRegionName, isDrop = true)
        }
        // destroy the base column table relation
        resolved.destroy(ifExists = true)
      }

      if (success) {
        // invalidate the ExtraTableInfo of the base table GemFireContainer on all executors
        GemFireContainerInvalidator.invalidateExtraTableInfo(baseTable, sqlContext)
      }
    }
  }



  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String]): BaseRelation = {
    // table has already been checked for existence by callers if required so here mode is Ignore
    createRelation(sqlContext, SaveMode.Ignore, options, None, asSelect = false)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame): BaseRelation = {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val relation = createRelation(sqlContext, mode, options, Some(data.schema), asSelect = true)
    var success = false
    try {
      // Need to create a catalog entry before insert since it will read the catalog
      // on the servers to determine table properties like compression etc.
      // SnappyExternalCatalog will alter the definition for final entry if required.
      session.sessionCatalog.createTableForBuiltin(relation.table,
        getClass.getCanonicalName, relation.schema, relation.origOptions,
        mode != SaveMode.ErrorIfExists)
      relation.insert(data, mode == SaveMode.Overwrite)
      success = true
      relation
    } finally {
      if (!success && relation.tableCreated) {
        // remove the catalog entry
        session.sessionCatalog.externalCatalog.dropTable(relation.baseRelation.schemaName,
          relation.baseRelation.tableName, ignoreIfNotExists = true, purge = false)
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType): BaseRelation = {
    // table has already been checked for existence by callers if required so here mode is Ignore
    createRelation(sqlContext, SaveMode.Ignore, options, Some(schema), asSelect = false)
  }
}



case class QcsLogicalPlan(output: Seq[Attribute]) extends LeafNode

case class QcsSparkPlan(override val output: Seq[Attribute]) extends LeafExecNode {
  override def doExecute(): RDD[InternalRow] = new DummyRDD(this.sparkContext)
}

class DummyRDD(@transient val sc: SparkContext) extends RDD[InternalRow](sc, Nil) {

  override def getPartitions: Array[Partition] = DummyRDD.constantPartition

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = null

}

object DummyRDD {
  val constantPartition: Array[Partition] = Array[Partition](new Partition() {
    override val index: Int = 1
  })
}

object GemFireContainerInvalidator {
  def invalidateExtraTableInfo(baseTable: Option[String], sqlContext: SQLContext): Unit = {
    baseTable.foreach(fqn => {
      try {
        Utils.mapExecutors[Unit](sqlContext.sparkContext, () => {
          val rgn = Misc.getRegionForTable(fqn, false)
          if (rgn != null) {
            val gfc = rgn.getUserAttribute.asInstanceOf[GemFireContainer]
            if (gfc != null) {
              gfc.invalidateHiveMetaData()
            }
          }
          Iterator.empty
        })
      } finally {
        val rgn = Misc.getRegionForTable(fqn, false)
        if (rgn != null) {
          val gfc = rgn.getUserAttribute.asInstanceOf[GemFireContainer]
          if (gfc != null) {
            gfc.invalidateHiveMetaData()
          }
        }
      }
    })
  }
}
