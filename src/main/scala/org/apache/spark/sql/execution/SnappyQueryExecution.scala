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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.common.{ErrorAndConfidence, MakeSubqueryNodesInvisible, PlaceHolderPlan, QueryRoutingRules}
import org.apache.spark.sql.internal.SnappyAQPSessionState
import org.apache.spark.sql.sources.SampleTableQuery

object SnappyQueryExecution {
  def processPlan(analyzed: LogicalPlan,
      sessionState: SnappyAQPSessionState, unanalyzedPlan: LogicalPlan): LogicalPlan = {
    val modAnalyzed = MakeSubqueryNodesInvisible(analyzed)
    val sampleQueryStack = scala.collection.mutable.Stack[Option[SampleTableQuery]]()
    val criteria: PartialFunction[LogicalPlan, LogicalPlan] = {
      case ErrorAndConfidence(err, confidenceX, child, sampleRatioAttribute,
      bootstrapMultiplicities, isAQPDebug, isFixedSeed, analysisType, bsAliasID,
      numBootstrapTrials, behavior, tableNameX, _) =>

        val queryRoutingPlan = if (QueryRoutingRules.shouldRouteToBaseTable(unanalyzedPlan,
          sessionState.snappySession.sessionCatalog, sessionState.conf)) {
          Some(unanalyzedPlan)
        } else None
        val stq = SampleTableQuery(sessionState.contextFunctions
            .getQueryExecution, null, err,
          confidenceX,
          SampleTableQuery.masquerade(sampleRatioAttribute),
          bootstrapMultiplicities, isAQPDebug, isFixedSeed,
          analysisType, bsAliasID, numBootstrapTrials, behavior, tableNameX, -1, true,
          true, queryRoutingPlan)
        sampleQueryStack.pop()
        sampleQueryStack.push(Some(stq))
        child
    }
    sampleQueryStack.push(None)
    val modifiedPlan = modAnalyzed.transformDown {
       criteria orElse {
         case PlaceHolderPlan(hidden, _, _, _) => {
           sampleQueryStack.push(None)
           val newHidden = hidden.transformDown {
             criteria
           }
           sampleQueryStack.pop() match {
             case Some(stq) => stq.copy(child =
               newHidden)
               // newHidden.asInstanceOf[SubqueryAlias].copy(child = newChild)
             case None => newHidden
           }
         }
         case stq: SampleTableQuery => stq.copy(makeVisible = true)
       }
    }
    sampleQueryStack.pop() match {
      case Some(stq) => stq.copy(child = modifiedPlan)
      case None => // No sample query found
                   modifiedPlan transformDown {
                     case PlaceHolderPlan(hiddenChild, _, _, _) => hiddenChild
                   }
    }
  }
}
