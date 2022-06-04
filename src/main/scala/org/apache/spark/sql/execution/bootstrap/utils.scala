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

import java.net.URL

import com.gemstone.gemfire.internal.shared.NativeCalls
import com.gemstone.gemfire.internal.shared.jna.OSType
import com.pivotal.gemfirexd.internal.snappy.CallbackFactoryProvider

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.{SnappyContext, SnappySession}

object BootStrapUtils {

  def getSeeds(attributes: Seq[Attribute]): Seq[Attribute] =
    attributes.collect { case seed@TaggedAttribute(Seed, _, _, _, _) => seed }

  /**
   * Get the first BootstrapCounts if any of this plan.
   */
  def getBtCnt(attributes: Seq[Attribute]): Option[TaggedAttribute] =
    attributes.collectFirst { case btCnt@TaggedAttribute(Bootstrap, _, _, _, _) => btCnt }


  private[this] def cartesian(exprs: Seq[Seq[Expression]]): Seq[Seq[Expression]] = exprs match {
    case Seq() => Seq(Nil)
    case _ => for (x <- exprs.head; y <- cartesian(exprs.tail)) yield x +: y
  }
}

object ClusterUtils {

  def checkClusterRestrictions(sc: SparkContext): Unit = {
    if (SnappySession.isEnterpriseEdition && !Utils.isLoner(sc) && isAWS) {
      CallbackFactoryProvider.getClusterCallbacks.getClusterType match {
        case "AWS_RESTRICTED" =>
          val servers = SnappyContext.getAllBlockIds.filterKeys(k => !k.equalsIgnoreCase("driver"))
          if (servers.size > 2) {
            // Only allow max 2 data-servers
            throw new IllegalStateException("Number of dataservers found to be exceeding the " +
                "limit.")
          } else if (servers.size > 1) {
            val serverHost = servers.values.head.blockId.host
            servers.foreach(k => {
              if (!k._2.blockId.host.equalsIgnoreCase(serverHost)) {
                throw new IllegalStateException("Dataservers found to be running on " +
                    "different hosts.")
              }
            })
          }
        case _ =>
      }
    }
  }

  private lazy val isAWS: Boolean = {
    try {
      var checkAMI = true
      // Check for system UUID on Linux as given here:
      // http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/identify_ec2_instances.html
      // As noted there the checks are not fool-proof but can be relied on for negation.
      // The DMI UUID needs root privileges so this checks HVM type instance that is readable.
      if (NativeCalls.getInstance().getOSType == OSType.LINUX) {
        val uuidFile = "/sys/hypervisor/uuid"
        val dmiFile = "/sys/devices/virtual/dmi/id/product_name"
        try {
          if (new java.io.File(uuidFile).exists()) {
            val uuidSource = scala.io.Source.fromFile(uuidFile)
            if (!uuidSource.mkString.startsWith("ec2")) {
              // definitely not AWS
              checkAMI = false
            }
            uuidSource.close()
          } else if (new java.io.File(dmiFile).exists()) {
            val dmiSource = scala.io.Source.fromFile(dmiFile)
            if (!dmiSource.mkString.toLowerCase.contains("hvm")) {
              // definitely not AWS
              checkAMI = false
            }
            dmiSource.close()
          } else {
            // definitely not AWS if neither of above two files are present
            checkAMI = false
          }
        } catch {
          case _: Throwable => // fallback to AMI check
        }
      }
      // Safer to use ami-id instead of product-codes which may not be available
      // for a community AMI
      if (checkAMI) {
        val conn = new URL("http://169.254.169.254/latest/meta-data/ami-id").openConnection
        conn.setConnectTimeout(2000)
        conn.setReadTimeout(2000)
        conn.getInputStream match {
          case null => false
          case stream => try {
            stream.read() != -1
          } finally {
            stream.close()
          }
        }
      } else false
    } catch {
      case _: Throwable => false
    }
  }
}
