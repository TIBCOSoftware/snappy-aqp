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

package org.apache.spark.sql.catalysts.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Alias, EqualNullSafe, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}

abstract class SnappyUnaryNode  extends LogicalPlan{

  def child: LogicalPlan


  override def children: Seq[LogicalPlan] = if (child ne null) child :: Nil else Nil
  /**
    * Generates an additional set of aliased constraints by replacing the original constraint
    * expressions with the corresponding alias
    */
  protected def getAliasedConstraints(projectList: Seq[NamedExpression]): Set[Expression] = {
    var allConstraints = child.constraints.asInstanceOf[Set[Expression]]
    projectList.foreach {
      case a @ Alias(e, _) =>
        // For every alias in `projectList`, replace the reference in constraints by its attribute.
        allConstraints ++= allConstraints.map(_ transform {
          case expr: Expression if expr.semanticEquals(e) =>
            a.toAttribute
        })
        allConstraints += EqualNullSafe(e, a.toAttribute)
      case _ => // Don't change.
    }

    allConstraints -- child.constraints
  }

  override protected def validConstraints: Set[Expression] =
    if (child ne null) child.constraints else Set.empty[Expression]

  override def statistics: Statistics = {
    // There should be some overhead in Row object, the size should not be zero when there is
    // no columns, this help to prevent divide-by-zero error.
    val childRowSize = child.output.map(_.dataType.defaultSize).sum + 8
    val outputRowSize = output.map(_.dataType.defaultSize).sum + 8
    // Assume there will be the same number of rows as child has.
    var sizeInBytes = (child.statistics.sizeInBytes * outputRowSize) / childRowSize
    if (sizeInBytes == 0) {
      // sizeInBytes can't be zero, or sizeInBytes of BinaryNode will also be zero
      // (product of children).
      sizeInBytes = 1
    }

    child.statistics.copy(sizeInBytes = sizeInBytes)
  }
}
