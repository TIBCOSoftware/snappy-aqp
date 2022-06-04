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
package org.apache.spark

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.execution.SnappyContextAQPFunctions
import org.apache.spark.sql.execution.common.{AQPInfo, AQPInfoStoreTestHook, AQPRules, AnalysisType}

object AssertAQPAnalysis extends AQPInfoStoreTestHook {

  var aqpInfoArray: Array[AQPInfo] = _

  def closedFormAnalysis(snc: SnappyContext, position: Int = 0): Unit = {
    assert(getAnalsyis(aqpInfoArray, position) match {
      case Some(x) => x.analysisType.get == AnalysisType.Closedform
      case None => false
    }, s"Found analysis type to be :" +
      s" ${getAnalsyis(aqpInfoArray, position).getOrElse("No Analysis found")}")
  }

  def bootStrapAnalysis(snc: SnappyContext, position: Int = 0): Unit = {
    val aqpFuncCtx = snc.sessionState.contextFunctions
        .asInstanceOf[SnappyContextAQPFunctions]
    assert(getAnalsyis(aqpInfoArray, position) match {
      case Some(x) => x.analysisType.get == AnalysisType.Bootstrap
      case None => false
    })
  }

  def bypassErrorCalc(snc: SnappyContext, position: Int = 0): Unit = {
    val aqpFuncCtx = snc.sessionState.contextFunctions
        .asInstanceOf[SnappyContextAQPFunctions]
    assert(getAnalsyis(aqpInfoArray, position) match {
      case Some(x) => x.analysisType.get == AnalysisType.ByPassErrorCalc
      case None => false
    })
  }


  private def getAnalsyis(arr: Array[AQPInfo], position: Int = 0): Option[AQPInfo] = {
    if (arr.length <= position) {
      None
    } else {
      Some(arr(position))
    }
  }

  def getAnalysisType(snc: SnappyContext): Option[AnalysisType.Type] =
    getAnalsyis(this.aqpInfoArray).get.analysisType

  override def callbackBeforeClearing(aqpInfos: Array[AQPInfo]): Unit = {
    this.aqpInfoArray = aqpInfos
  }
}
