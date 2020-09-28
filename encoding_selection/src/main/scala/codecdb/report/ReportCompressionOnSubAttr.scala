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

import codecdb.dataset.column.Column
import codecdb.dataset.feature.compress.ParquetCompressFileSize
import codecdb.dataset.feature.resource.ParquetEncFileSize
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}

import scala.collection.JavaConverters._

object ReportCompressionOnSubAttr extends App {

  val sql = "SELECT c FROM Column c WHERE c.parentWrapper IS NULL AND c.dataType = :dt"
  val childSql = "SELECT c FROM Column c WHERE c.parentWrapper = :parent"

  val em = new JPAPersistence().em

  val fTypes = Array(
    ParquetCompressFileSize.featureType,
    ParquetCompressFileSize.featureType,
  )

  val names = Array(
    "_GZIP_file_size",
    "_LZO_file_size"
  )

  val output = new PrintWriter(new FileOutputStream("subattr_compress.csv"))

  em.createQuery(sql, classOf[ColumnWrapper]).setParameter("dt", DataType.STRING)
    .getResultList.asScala.filter(c => {
    val sab = c.getInfo("subattr_benefit")
    sab > 0 && sab < 0.95
  }).foreach(col => {

    val colEncSizes = col.findFeatures(ParquetEncFileSize.featureType).filter(f => f.name != "BITVECTOR_file_size" && f.value > 0)

    if (colEncSizes.nonEmpty) {

      val colBestEnc = colEncSizes.minBy(_.value)

      val colValues = fTypes.zip(names).flatMap(pair => {
        val ft = pair._1
        val n = pair._2
        val rn = colBestEnc.name.replace("_file_size", n)
        col.findFeature(ft, rn)
      }).map(_.value)
      if (colValues.length == 2) {

        val children = getChildren(col)
        val sumsize = children.map(child => {
          val encs = child.findFeatures(ParquetEncFileSize.featureType).filter(f => f.name != "BITVECTOR_file_size" && f.value > 0)
          if (encs.nonEmpty) {
            val bestEnc = encs.minBy(_.value)

            val values = fTypes.zip(names).flatMap(pair => {
              val ft = pair._1
              val n = pair._2
              val rn = bestEnc.name.replace("_file_size", n)
              child.findFeature(ft, rn)
            }).map(_.value)

            (bestEnc.value, values(0), values(1))
          } else {
            (0.0, 0.0, 0.0)
          }
        }).reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
        output.println(((colBestEnc.value +: colValues) ++ sumsize.productIterator).mkString(","))
      }
    }
  })
  output.close

  def getChildren(col: Column): Seq[Column] = {
    em.createQuery(childSql, classOf[ColumnWrapper]).setParameter("parent", col).getResultList.asScala
  }
}
