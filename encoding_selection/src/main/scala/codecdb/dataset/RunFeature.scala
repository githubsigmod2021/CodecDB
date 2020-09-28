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

import codecdb.dataset.feature.{Features, Filter}
import codecdb.dataset.feature.resource.ScanTimeUsage
import codecdb.dataset.persist.Persistence
import codecdb.dataset.persist.jpa.JPAPersistence
import codecdb.encoding.BitVectorEncoding
import edu.uchicago.cs.encsel.dataset.feature._
import edu.uchicago.cs.encsel.dataset.feature.resource.ScanTimeUsage
import jdk.nashorn.internal.runtime.BitVector
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Created by harper on 4/23/17.
  */
object RunFeature extends App {

  val logger = LoggerFactory.getLogger(getClass)

  val persist = new JPAPersistence

  // val missed = Seq(new MiscEncFileSize(new BitVectorEncoding))
  val missed = Seq(ScanTimeUsage)

  val prefix = args.length match {
    case gt if gt > 0 => args(0)
    case _ => ""
  }

  val filter = args.length match {
    case gt if gt > 0 =>
      args(1) match {
        case "none" => null
        case "firstn" => Filter.firstNFilter(args(2).toInt)
        case "iid" => Filter.iidSamplingFilter(args(2).toDouble)
        case "size" => Filter.sizeFilter(args(2).toInt)
        case "minsize" => Filter.minSizeFilter(args(2).toInt, args(3).toDouble)
        case _ => throw new IllegalArgumentException(args(1))
      }
    case _ => null
  }

  Features.extractors.clear()
  Features.extractors ++= missed

  val persistence = Persistence.get
  val columns = persistence.load().toList
  val size = columns.size
  var counter = 0
  columns.foreach(column => {
    counter += 1
    System.out.println("Processing %d / %d : %s".format(counter, size, column.colFile))
    try {
      if (filter == null) {
        column.replaceFeatures(Features.extract(column))
      } else {
        column.replaceFeatures(Features.extract(column, filter, prefix))
      }
      persistence.save(Seq(column))
    } catch {
      case e: Exception => {
        logger.warn("Failed during processing", e)
      }
    }
  })
}
