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

import codecdb.dataset.feature.Features
import codecdb.dataset.feature.classify.SimilarWords
import codecdb.dataset.feature.compress.ParquetCompressFileSize
import codecdb.dataset.feature.resource.ParquetEncFileSize
import codecdb.dataset.persist.Persistence
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.dataset.feature._
import edu.uchicago.cs.encsel.dataset.feature.classify._
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Created by harper on 4/23/17.
  */
object RunFeatureOnSingleColumn extends App {

  val logger = LoggerFactory.getLogger(getClass)

  val persist = new JPAPersistence

  // val missed = Seq(new MiscEncFileSize(new BitVectorEncoding))
  val missed = Seq(new SimilarWords)

  val from = args(0).toInt
  val to = args(1).toInt

  Features.extractors.clear()
  Features.extractors ++= missed

  val persistence = Persistence.get
  val columns = persist.em.createQuery("SELECT c FROM Column c WHERE c.parentWrapper IS NULL and c.id BETWEEN :a and :b ORDER BY c.id",
    classOf[ColumnWrapper])
    .setParameter("a", from)
    .setParameter("b", to).getResultList.asScala.toList
  val size = columns.size
  var counter = 0
  columns.foreach(column => {
    counter += 1
    System.out.println("Processing %d, %d / %d : %s".format(column.id, counter, size, column.colFile))
    try {
      column.replaceFeatures(Features.extract(column))
      persistence.save(Seq(column))
    } catch {
      case e: Exception => {
        logger.warn("Failed during processing", e)
      }
    }
  })
}
