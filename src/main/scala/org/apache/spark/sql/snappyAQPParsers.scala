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
package org.apache.spark.sql

import scala.language.implicitConversions

import io.snappydata.sql.catalog.CatalogObjectType
import org.parboiled2.{Rule0, Rule1}

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.CreateTableUsingCommand
import org.apache.spark.sql.execution.common.{Behavior, Confidence, Error, ErrorDefaults}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Snappy SQL extensions. Includes:
 *
 * Stratified sample tables:
 * 1) ERROR ESTIMATE AVG: error estimate for mean of a column/expression
 * 2) ERROR ESTIMATE SUM: error estimate for sum of a column/expression
 */
class SnappyAQPParser(session: SnappySession) extends SnappyParser(session) {

  protected final def ERROR: Rule0 = rule { keyword(SnappyParserConsts.ERROR) }
  protected final def ESTIMATE: Rule0 = rule { keyword(SnappyParserConsts.ESTIMATE) }
  protected final def CONFIDENCE: Rule0 = rule { keyword(SnappyParserConsts.CONFIDENCE) }
  protected final def BEHAVIOR: Rule0 = rule { keyword(SnappyParserConsts.BEHAVIOR) }
  protected final def SAMPLE: Rule0 = rule { keyword(SnappyParserConsts.SAMPLE) }
  protected final def TOPK: Rule0 = rule { keyword(SnappyParserConsts.TOPK) }

  override protected def select: Rule1[LogicalPlan] = rule {
    super.select ~
    (WITH ~ ERROR ~ expression).? ~
    (CONFIDENCE ~ expression).? ~
    (BEHAVIOR ~ expression).? ~
    (WITH ~ ERROR ~ push(true)).? ~> { (q: LogicalPlan, e: Any, c: Any, b: Any, a: Any) =>
      val withError = e match {
        case None => q
        case Some(expr) => Error(expr.asInstanceOf[Expression], q)
      }
      val withConfidence = c match {
        case None => withError
        case Some(expr) => Confidence(expr.asInstanceOf[Expression], withError)
      }
      val withBehavior = b match {
        case None => withConfidence
        case Some(expr) => Behavior(expr.asInstanceOf[Expression], withConfidence)
      }
      a match {
        case None => withBehavior
        case Some(_) => ErrorDefaults(withBehavior)
      }
    }
  }

  protected override def ddl: Rule1[LogicalPlan] = rule {
    super.ddl | createSampleOrTopK
  }

  protected def createSampleOrTopK: Rule1[LogicalPlan] = rule {
    CREATE ~ (SAMPLE ~ push(true) | TOPK ~ push(false)) ~ TABLE ~ ifNotExists ~ tableIdentifier ~
    (ON ~ tableIdentifier | tableSchema).? ~ OPTIONS ~ options ~
    (AS ~ query.named("select")).? ~> { (tableType: Boolean,
    ifExists: Boolean, tableIdent: TableIdentifier, baseTableOrSchema: Any,
    opts: Map[String, String], query: Any) =>
      val (provider, name) = if (tableType) {
        (SnappyContext.SAMPLE_SOURCE, CatalogObjectType.Sample.toString)
      } else {
        (SnappyContext.TOPK_SOURCE, CatalogObjectType.TopK.toString)
      }
      val (options, baseTable, userSpecifiedSchema) = baseTableOrSchema match {
        case None => (opts, None, None)
        case Some(table: TableIdentifier) =>
          (opts, Some(table.unquotedString), None)
        case Some(columns: Seq[_]) =>
          (opts, None, Some(StructType(columns.asInstanceOf[Seq[StructField]])))
        case _ => throw Utils.analysisException(
          s"CREATE $name: unexpected schema or baseTable $baseTableOrSchema")
      }
      val mode = if (ifExists) SaveMode.Ignore else SaveMode.ErrorIfExists
      val queryOpt = query.asInstanceOf[Option[LogicalPlan]]
      CreateTableUsingCommand(tableIdent, baseTable, userSpecifiedSchema, schemaDDL = None,
        provider, mode, options, Utils.EMPTY_STRING_ARRAY, bucketSpec = None,
        queryOpt, isBuiltIn = true)
    }
  }

  override def newInstance(): SnappyAQPParser = new SnappyAQPParser(session)
}

class SnappyAQPSqlParser(session: SnappySession)
    extends SnappySqlParser(session) {

  @transient protected[sql] override val sqlParser: SnappyAQPParser =
    new SnappyAQPParser(session)
}
