/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License,
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 *
 */
package codecdb.dataset.column.listener

import org.slf4j.LoggerFactory

class FailureMonitor extends ColumnReaderListener {
  val FACTOR = 100
  var failedCount = 0
  var totalCount = 0
  val logger = LoggerFactory.getLogger(getClass)

  def start(event: ColumnReaderEvent): Unit = {
    failedCount = 0
    totalCount = 0
  }

  def readRecord(event: ColumnReaderEvent): Unit = {
    totalCount += 1
  }

  def failRecord(event: ColumnReaderEvent): Unit = {
    failedCount += 1
  }

  def done(event: ColumnReaderEvent): Unit = {
    if (failedCount * FACTOR > totalCount) {
      logger.warn("File contains too many malformatted records:" + event.getSource.toString)
    }
  }

}