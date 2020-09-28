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
package codecdb.dataset.column

import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import codecdb.Config
import codecdb.dataset.column.listener.{ColumnReaderEvent, ColumnReaderListener, FailureMonitor, FailureStopper}
import codecdb.dataset.schema.Schema
import org.apache.commons.lang3.event.EventListenerSupport
import org.slf4j.LoggerFactory

trait ColumnReader {

  protected val logger = LoggerFactory.getLogger(getClass)
  protected val eventSupport = new EventListenerSupport(classOf[ColumnReaderListener])

  eventSupport.addListener(new FailureMonitor)
  eventSupport.addListener(new FailureStopper)

  def readColumn(source: URI, schema: Schema): Iterable[Column]

  protected def allocTempFolder(source: URI): Path = {
    val tempRoot = Paths.get(Config.columnFolder)
    val tempFolder = Files.createTempDirectory(tempRoot, "columner")
    tempFolder
  }

  protected def allocFileForCol(folder: Path, colName: String, colIdx: Int): URI = {
    val path = Files.createTempFile(folder, "%s_%d".format(colName, colIdx), null)
    path.toUri
  }

  protected def fireStart(source: URI) = eventSupport.fire().start(new ColumnReaderEvent(source))
  protected def fireReadRecord(source: URI) = eventSupport.fire().readRecord(new ColumnReaderEvent(source))
  protected def fireFailRecord(source: URI) = eventSupport.fire().failRecord(new ColumnReaderEvent(source))
  protected def fireDone(source: URI) = eventSupport.fire().done(new ColumnReaderEvent(source))
}