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
package org.apache.spark.sql.topk

import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.approximate.TopKUtil
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

case class TopKRelation(
    @transient override val sqlContext: SnappyContext,
    topKName: String,
    topKOptions: Map[String, String],
    baseTable: Option[String],
    baseTableSchema: StructType,
    keyColumn: String,
    isStreamSummary: Boolean)(override val schema: StructType =
      TopKUtil.getSchema(sqlContext, baseTableSchema(keyColumn),
        isStreamSummary))
    extends BaseRelation
        with PrunedFilteredScan
        with SchemaInsertableRelation
        with DestroyRelation
        with Logging
        with Serializable {

  override val needConversion: Boolean = false

  private[sql] var tableCreated: Boolean = _


  // create the TopK structure if not present
  create(ifExists = true)

  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    val (startTime, endTime) = filters.foldLeft((null: String, null: String)) {
      case ((start, end), EqualTo(attr, value)) =>
        if (attr.equalsIgnoreCase(TopKUtil.START_TIME_COLUMN)) {
          (value.toString, end)
        } else if (attr.equalsIgnoreCase(TopKUtil.END_TIME_COLUMN)) {
          (start, value.toString)
        } else {
          (start, end)
        }
      case (v, _) => v
    }
    val projection = requiredColumns.map(schema.fieldIndex)
    val rdd = sqlContext.snappySession.sessionState.
        contextFunctions.queryTopKRDD(sqlContext.snappySession, topKName,
      startTime, endTime, schema)
    // check if this selects all columns, then nothing required to be done
    if (projection.length == schema.length && projection.indices == projection.toSeq) {
      rdd.asInstanceOf[RDD[Row]]
    } else {
      val projectionTypes = projection.map(i => (i, schema.fields(i).dataType))
      rdd.mapPreserve { row =>
        new GenericInternalRow(projectionTypes.map(p =>
          row.get(p._1, p._2).asInstanceOf[Any]))
      }.asInstanceOf[RDD[Row]]
    }
  }

  // TODO: topK create/drop/truncate are not thread-safe among each other.
  // Not expending effort on that since this whole will change to
  // use GemXD storage in next release

  def create(ifExists: Boolean): Unit = {
    tableCreated = sqlContext.snappySession.sessionState.
        contextFunctions.createTopK(sqlContext.snappySession, topKName,
      keyColumn, baseTableSchema, topKOptions, ifExists)
  }

  override def insertableRelation(
      sourceSchema: Seq[Attribute]): Option[InsertableRelation] = {
    Some(copy()(schema = sourceSchema.toStructType))
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      truncate()
    }
    val time = data match {
      case dt: DataFrameWithTime => dt.time
      case _ => System.currentTimeMillis()
    }
    append(data.rdd, time)
  }

  override def append(rows: RDD[Row], time: Long): Unit = {
    val session = sqlContext.snappySession
    session.sessionState.contextFunctions.insertIntoTopK(session, rows, topKName, time)
  }

  /**
    * Destroy and cleanup this relation. It may include, but not limited to,
    * dropping the external table that this relation represents.
    */
  override def destroy(ifExists: Boolean): Unit =
    sqlContext.sessionState.contextFunctions.dropTopK(sqlContext.snappySession, topKName)

  /**
    * Truncate the table represented by this relation.
    */
  override def truncate(): Unit = {
    sqlContext.sessionState.contextFunctions.dropTopK(sqlContext.snappySession, topKName)
    create(ifExists = false)
  }
}

final class DefaultSource
    extends RelationProvider
        with SchemaRelationProvider
        with CreatableRelationProvider with DataSourceRegister {

  override def shortName(): String = SnappyContext.TOPK_SOURCE

  def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schemaOpt: Option[StructType],
      asSelect: Boolean): TopKRelation = {

    val params = new CaseInsensitiveMutableHashMap(options)

    val snc = sqlContext.asInstanceOf[SnappyContext]
    val catalog = snc.sessionState.catalog
    val baseTable = params.remove(SnappyExternalCatalog.BASETABLE_PROPERTY)
        .map(catalog.resolveExistingTable(_).unquotedString)
    val topKName = ExternalStoreUtils.removeInternalPropsAndGetTable(params, tableAsUpper = false)

    // at least one of schema or baseTable is required
    val schema = Utils.getSchemaAndPlanFromBase(schemaOpt, baseTable, catalog,
      asSelect, topKName, CatalogObjectType.TopK.toString)._1

    // get the key and streamSummary options
    val keyColumn = params.remove("key").map(catalog.formatTableName)
        .getOrElse(throw Utils.analysisException(
          "CREATE TOPK: 'key' option missing"))
    val streamSummary = params.getOrElse("streamSummary", "false").toBoolean

    val (topKSchema, topKTable) = SnappyExternalCatalog.getTableWithSchema(topKName, "")
    // schema can only be the current schema
    if (!topKSchema.isEmpty && !topKSchema.equalsIgnoreCase(catalog.getCurrentSchema)) {
      throw new AnalysisException(s"TopK name '$topKName' cannot have schema " +
          s"other than current = '${catalog.getCurrentSchema}'.")
    }
    TopKRelation(snc, topKTable, params.toMap, baseTable, schema, keyColumn, streamSummary)()
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String]): BaseRelation = {
    // table has already been checked for existence by callers if required so here mode is Ignore
    createRelation(sqlContext, SaveMode.Ignore, options, None, asSelect = false)
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType): BaseRelation = {
    // table has already been checked for existence by callers if required so here mode is Ignore
    createRelation(sqlContext, SaveMode.Ignore, options, Some(schema), asSelect = false)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame): BaseRelation = {
    val relation = createRelation(sqlContext, mode, options, Some(data.schema), asSelect = true)
    var success = false
    try {
      relation.insert(data, mode == SaveMode.Overwrite)
      success = true
      relation
    } finally {
      if (!success && relation.tableCreated) {
        relation.destroy(ifExists = true)
      }
    }
  }
}
