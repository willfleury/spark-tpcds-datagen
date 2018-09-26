/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import java.util.Locale


class TPCDSQueryValidatorArguments(val args: Array[String]) {

  var queryFilter: Set[String] = Set.empty
  var targetSystem = "HDFS"
  var dataLocation: String = null
  var database: String = null
  var queryGridLink: String = null

  parseArgs(args.toList)
  validateArguments()

  private def optionMatch(optionName: String, s: String): Boolean = {
    optionName == s.toLowerCase(Locale.ROOT)
  }

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case ("--target-system") :: value :: tail =>
          targetSystem = value
          args = tail

        case ("--database") :: value :: tail =>
          database = value
          args = tail

        case ("--query-grid-link") :: value :: tail =>
          queryGridLink = value
          args = tail

        case ("--data-location") :: value :: tail =>
          dataLocation = value
          args = tail

        case ("--query-filter") :: value :: tail =>
          queryFilter = value.toLowerCase(Locale.ROOT).split(",").map(_.trim).toSet
          args = tail

        case _ =>
          // scalastyle:off println
          System.err.println("Unknown/unsupported param " + args)
          // scalastyle:on println
          printUsageAndExit(1)
      }
    }
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off
    System.err.println("""
      |Usage: spark-submit --class <this class> <spark sql test jar> [Options]
      |Options:
      |  --target-system      Which source system to use [HDFS | TD] (default: HDFS)
      |  --data-location      Path to TPCDS data
      |  --query-filter       Queries to filter, e.g., q3,q5,q13
      |  --query-grid-link    QueryGrid link to use if TD target
      |  --database           TD Database if target is TD
    """.stripMargin)
    // scalastyle:on
    System.exit(exitCode)
  }

  private def validateArguments(): Unit = {
    if (targetSystem.equals("HDFS") && dataLocation == null) {
      // scalastyle:off println
      System.err.println("Must specify a data location for HDFS target")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    if (!targetSystem.equals("HDFS") && !targetSystem.equals("TD")) {
      // scalastyle:off println
      System.err.println("Target system must be either HDFS or TD")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    if (targetSystem.equals("TD") && database == null) {
      // scalastyle:off println
      System.err.println("Must specify a database for TD target")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    if (targetSystem.equals("TD") && queryGridLink == null) {
      // scalastyle:off println
      System.err.println("Must specify a query grid link for TD target")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
  }
}
