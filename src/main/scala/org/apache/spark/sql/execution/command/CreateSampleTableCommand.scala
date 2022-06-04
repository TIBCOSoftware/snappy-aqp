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
package org.apache.spark.sql.execution.command

import io.snappydata.sql.catalog.SnappyExternalCatalog

import org.apache.spark.sql.{DataFrame, Row, SnappySession, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.SamplingRelation

class CreateSampleTableCommand (_table: CatalogTable, _ignoreIfExists: Boolean)
  extends CreateDataSourceTableCommand (_table, _ignoreIfExists) {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val retVal = super.run(sparkSession)
    val parameters = new CaseInsensitiveMutableHashMap(_table.storage.properties)
    val baseTable = parameters.get(SnappyExternalCatalog.BASETABLE_PROPERTY)
    val sampleDf = sparkSession.sqlContext.table(_table.qualifiedName)
    this.populateSampleTableOnCreation(sampleDf, baseTable, sparkSession)
    retVal
  }

  private def populateSampleTableOnCreation(sampleDf: DataFrame,
    baseTableOpt: Option[String], session: SparkSession): Unit = {
    val srOpt = sampleDf.queryExecution.analyzed.collectFirst {
      case l: LogicalRelation => l.relation.asInstanceOf[SamplingRelation]
    }
    baseTableOpt.foreach(baseTable => srOpt.foreach(_.insert(session.table(baseTable), false)))
  }
}


object CreateSampleTableCommand {
  def apply(table: CatalogTable, ignoreIfExists: Boolean): CreateSampleTableCommand =
    new CreateSampleTableCommand(table, ignoreIfExists)

  def unapply(arg: CreateSampleTableCommand): Option[(CatalogTable, Boolean)] =
    Some((arg.table, arg.ignoreIfExists))
}
