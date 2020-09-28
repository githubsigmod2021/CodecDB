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
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */

package codecdb.dataset

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.concurrent.{Callable, Executors, TimeUnit}

import codecdb.Config
import codecdb.dataset.column.{Column, ColumnReader, ColumnReaderFactory}
import codecdb.dataset.feature.Features
import codecdb.dataset.persist.Persistence
import codecdb.dataset.schema.Schema
import codecdb.util.FileUtils
import edu.uchicago.cs.encsel.dataset.column.Column
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
/**
  * Created by harper on 4/23/17.
  */
object CollectData extends App {
  val f = new File(args(0))
  new DataCollector().scan(f.toURI)
}


class DataCollector {

  var persistence: Persistence = Persistence.get
  val logger = LoggerFactory.getLogger(this.getClass)
  val threadPool = Executors.newFixedThreadPool(Config.collectorThreadCount)

  def scan(source: URI): Unit = {
    val target = Paths.get(source)
    val tasks = scala.collection.immutable.List(target).flatMap(FileUtils.scanFunction(_)).map { p => {
      new Callable[Unit] {
        def call: Unit = {
          collect(p.toUri)
        }
      }
    }
    }
    threadPool.invokeAll(tasks)
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
  }

  def collect(source: URI): Unit = {
    try {
      val path = Paths.get(source)
      if (Files.isDirectory(path)) {
        logger.warn("Running on Directory is undefined")
        return
      }
      if (logger.isDebugEnabled())
        logger.debug("Scanning " + source.toString)

      if (isDone(source)) {
        if (logger.isDebugEnabled())
          logger.debug("Scanned mark found, skip")
        return
      }

      val columner: ColumnReader = ColumnReaderFactory.getColumnReader(source)
      if (columner == null) {
        if (logger.isDebugEnabled())
          logger.debug("No available reader found, skip")
        return
      }
      val defaultSchema = Schema.getSchema(source)
      if (null == defaultSchema) {
        if (logger.isDebugEnabled())
          logger.debug("Schema not found, skip")
        return
      }
      val columns = columner.readColumn(source, defaultSchema)

      columns.foreach(extractFeature)

      persistence.save(columns)

      markDone(source)
      if (logger.isDebugEnabled())
        logger.debug("Scanned " + source.toString)

    } catch {
      case e: Exception =>  logger.error("Exception while scanning " + source.toString, e)
    }
  }

  protected def isDone(file: URI): Boolean = {
    FileUtils.isDone(file, "done")
  }

  protected def markDone(file: URI) = {
    FileUtils.markDone(file, "done")
  }

  private def extractFeature(col: Column): Unit = {
    try {
      col.features ++= Features.extract(col).toSet
    } catch {
      case e: Exception => logger.warn("Exception while processing column:%s@%s".format(col.colName, col.origin), e)
    }
  }

}