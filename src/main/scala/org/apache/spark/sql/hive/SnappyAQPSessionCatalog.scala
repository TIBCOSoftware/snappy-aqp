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
package org.apache.spark.sql.hive

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, FunctionResourceLoader, GlobalTempViewManager, SimpleCatalogRelation}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.datasources.FindDataSourceTable
import org.apache.spark.sql.execution.{TopK, TopKWrapper}
import org.apache.spark.sql.internal.{SQLConf, SnappySessionCatalog}
import org.apache.spark.sql.{AnalysisException, SnappySession}

class SnappyAQPSessionCatalog(_externalCatalog: SnappyExternalCatalog,
    _session: SnappySession, globalViewManager: GlobalTempViewManager,
    _functionResourceLoader: FunctionResourceLoader,
    _functionRegistry: FunctionRegistry, _sqlConf: SQLConf, _hadoopConf: Configuration)
    extends SnappySessionCatalog(_externalCatalog,
      _session, globalViewManager, _functionResourceLoader,
      _functionRegistry, _sqlConf, _hadoopConf) {

  /**
   * Temporary sample dataFrames registered using stratifiedSample API that do not go
   * in external catalog.
   */
  private val mainDFToSampleDFs =
    new ConcurrentHashMap[LogicalPlan, mutable.ArrayBuffer[(LogicalPlan, String)]]()

  def addSampleDataFrame(base: LogicalPlan, sample: LogicalPlan, name: String = ""): Unit = {
    val sampleName = formatTableName(name)
    mainDFToSampleDFs.compute(base, new java.util.function.BiFunction[LogicalPlan,
        mutable.ArrayBuffer[(LogicalPlan, String)], mutable.ArrayBuffer[(LogicalPlan, String)]] {
      override def apply(plan: LogicalPlan, samples: mutable.ArrayBuffer[
          (LogicalPlan, String)]): mutable.ArrayBuffer[(LogicalPlan, String)] = {
        if (samples eq null) {
          new mutable.ArrayBuffer[(LogicalPlan, String)](4) += (sample -> sampleName)
        } else {
          if (samples.exists(_._2 == sampleName)) samples else (sample, sampleName) +=: samples
        }
      }
    })
  }

  /**
   * Return the set of temporary samples for a given table that are not tracked in catalog.
   */
  override def getSamples(base: LogicalPlan): Seq[LogicalPlan] = mainDFToSampleDFs.get(base) match {
    case null => Nil
    case samples => if (samples.isEmpty) Nil else samples.collect {
      // skip named samples that are stored in catalog and should be retrieved by getSampleRelations
      case (plan, name) if name.isEmpty => plan
    }
  }

  /**
   * Return the set of samples for a given table that are tracked in catalog and are not temporary.
   */
  override def getSampleRelations(baseTable: TableIdentifier): Seq[(LogicalPlan, String)] = {
    val schema = getSchemaName(baseTable)
    val table = formatTableName(baseTable.table)
    // get the samples from dependents
    externalCatalog.getTableOption(schema, table) match {
      case Some(baseCatalogTable) =>
        val samples = externalCatalog.getDependents(schema, table, baseCatalogTable,
          CatalogObjectType.Sample :: Nil, Nil)
        if (samples.nonEmpty) {
          samples.map(t => new FindDataSourceTable(snappySession)(
            SimpleCatalogRelation(t.database, t)) -> t.identifier.unquotedString)
        } else Nil
      case None =>
        // check for temporary table
        if (isTemporaryTable(baseTable)) mainDFToSampleDFs.get(resolveRelation(baseTable)) match {
          case null => Nil
          case samples =>
            // in addition to skipping un-named samples created by API, this ensures that
            // a copy of internal ArrayBuffer is returned
            samples.filterNot(_._2.isEmpty)
        } else Nil
    }
  }

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = {
    super.createTable(table, ignoreIfExists)
    // add entry for temporary base table if present
    table.storage.properties.find(_._1.equalsIgnoreCase(
      SnappyExternalCatalog.BASETABLE_PROPERTY)) match {
      case None =>
      case Some((_, baseTable)) =>
        val baseTableIdent = snappySession.sessionCatalog.resolveExistingTable(baseTable)
        if (isTemporaryTable(baseTableIdent)) {
          addSampleDataFrame(resolveRelation(baseTableIdent), resolveRelation(table.identifier),
            table.identifier.unquotedString)
        }
    }
  }

  override protected def dropTemporaryTable(tableIdent: TableIdentifier): Unit = {
    // remove entry for temporary table in mainDFToSampleDFs if present
    mainDFToSampleDFs.remove(resolveRelation(tableIdent))
  }

  override protected def dropFromTemporaryBaseTable(table: CatalogTable): Unit = {
    // check for base temporary table and remove from mainDFToSampleDFs if present
    val params = new CaseInsensitiveMutableHashMap[String](table.storage.properties)
    params.get(SnappyExternalCatalog.BASETABLE_PROPERTY) match {
      case None =>
      case Some(baseTable) =>
        val baseTableIdent = snappySession.sessionCatalog.resolveExistingTable(baseTable)
        if (isTemporaryTable(baseTableIdent)) {
          val baseTableRelation = resolveRelation(baseTableIdent)
          val sampleTableName = table.identifier.unquotedString
          // try to remove from all sessions that can be found
          val catalogs = SnappySession.getPlanCache.asMap().keySet().asScala.map(
            _.session.sessionCatalog.asInstanceOf[SnappyAQPSessionCatalog]).toSet + this
          catalogs.foreach(_.mainDFToSampleDFs.compute(baseTableRelation,
            new java.util.function.BiFunction[LogicalPlan,
                mutable.ArrayBuffer[(LogicalPlan, String)],
                mutable.ArrayBuffer[(LogicalPlan, String)]] {
              override def apply(plan: LogicalPlan, samples: mutable.ArrayBuffer[
                  (LogicalPlan, String)]): mutable.ArrayBuffer[(LogicalPlan, String)] = {
                if (samples ne null) {
                  val index = samples.indexWhere(p => p._2 == sampleTableName)
                  if (index != -1) samples.remove(index)
                  if (samples.isEmpty) null else samples
                } else null
              }
            }))
        }
    }
  }

  def lookupTopK(topKName: String): Option[(TopKWrapper, RDD[(Int, TopK)])] = {
    val topKPlanName = TopKPlan.getPlanName(topKName)
    getGlobalTempView(topKPlanName) match {
      case None => None
      case Some(p) => Some(p.asInstanceOf[TopKPlan].topK)
    }
  }

  def registerTopK(topKWrapper: TopKWrapper, rdd: RDD[(Int, TopK)],
      ifExists: Boolean, overwrite: Boolean): Boolean = globalViewManager.synchronized {
    val topKPlanName = TopKPlan.getPlanName(topKWrapper.name)
    val exists = getGlobalTempView(topKPlanName).isDefined
    if (exists) {
      if (ifExists) return false
      if (!overwrite) {
        throw new AnalysisException(s"Top-K structure '${topKWrapper.name}' already exists.")
      }
    }
    createGlobalTempView(topKPlanName, TopKPlan(topKWrapper -> rdd), overwrite)
    !exists
  }

  def unregisterTopK(topKName: String): Unit = globalViewManager.synchronized {
    val topKPlanName = TopKPlan.getPlanName(topKName)
    getGlobalTempView(topKPlanName) match {
      case None =>
      case Some(p) =>
        dropGlobalTempView(topKPlanName)
        p.asInstanceOf[TopKPlan].topK._2.unpersist()
    }
  }
}

case class TopKPlan(topK: (TopKWrapper, RDD[(Int, TopK)])) extends LeafNode {
  override def output: Seq[Attribute] = Nil
}

object TopKPlan {
  /** suffix used for TopKPlan name when caching its plan like a global temporary view */
  private[this] val PLAN_SUFFIX = "_TOPKPLAN"

  private[sql] def getPlanName(name: String): String = name + PLAN_SUFFIX
}
