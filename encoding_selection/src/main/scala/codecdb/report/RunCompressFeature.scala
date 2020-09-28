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

package codecdb.report

import codecdb.dataset.feature.Features
import codecdb.dataset.feature.compress.ParquetCompressFileSizeAndTime
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.dataset.feature._
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Random

/**
  * Created by harper on 4/23/17.
  */
object RunCompressFeature extends App {

  val logger = LoggerFactory.getLogger(getClass)

  val persist = new JPAPersistence

  // val missed = Seq(new MiscEncFileSize(new BitVectorEncoding))
  val missed = Seq(ParquetCompressFileSizeAndTime)

  Features.extractors.clear()
  Features.extractors ++= missed

  val sql = "SELECT c FROM Column c WHERE c.parentWrapper IS NULL"

  val rate = 0.2

  val columns = persist.em.createQuery(sql, classOf[ColumnWrapper]).getResultList
  var counter = 0
  columns.foreach(column => {
    counter += 1
    System.out.println("Processing %d : %s".format(counter, column.colFile))
    if (Random.nextDouble() <= rate) {
      try {
        column.replaceFeatures(Features.extract(column))
        persist.save(Seq(column))
      } catch {
        case e: Exception => {
          logger.warn("Failed during processing", e)
        }
      }
    }
  })
}
