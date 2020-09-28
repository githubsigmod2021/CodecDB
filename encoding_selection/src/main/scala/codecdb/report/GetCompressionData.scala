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

import java.io.{FileOutputStream, PrintWriter}

import codecdb.dataset.feature.compress.ParquetCompressFileSize
import codecdb.dataset.feature.resource.ParquetEncFileSize
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}

import scala.collection.JavaConverters._

object GetCompressionData extends App {

  val persist = new JPAPersistence

  val types = Array(DataType.INTEGER, DataType.DOUBLE, DataType.STRING)
    .map(dt => (dt, new PrintWriter(new FileOutputStream("cae_%s".format(dt.name()))))).toList.toMap

  val cols = persist.em.createQuery("SELECT c FROM Column c WHERE c.parentWrapper IS NULL", classOf[ColumnWrapper])
    .getResultList.asScala

  val comp = Array("GZIP", "LZO", "SNAPPY")

  cols.foreach(col => {
    if (types.contains(col.dataType) && col.hasFeature(ParquetCompressFileSize.featureType)) {
      val encs = col.findFeatures(ParquetEncFileSize.featureType).filter(_.value > 0)

      if (encs.nonEmpty) {
        val bestEnc = encs.minBy(_.value)
        val bestEncSize = bestEnc.value.toString
        val name = bestEnc.name.replace("_file_size", "")

        val compressed = comp.map(c => "%s_%s_file_size".format(name, c)).map(name =>
          col.findFeature(ParquetCompressFileSize.featureType, name) match {
            case Some(f) => f.value.toString
            case None => ""
          }
        )
        val data = (bestEncSize +: compressed).mkString(",")
        types.getOrElse(col.dataType, null).println(data)
      }
    }
  })

  types.foreach(_._2.close)
}
