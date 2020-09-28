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

package codecdb.dataset.feature.subattr

import java.io.InputStream

import codecdb.dataset.column.Column
import codecdb.dataset.feature.{Feature, FeatureExtractor}
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import codecdb.util.FileUtils
import edu.uchicago.cs.encsel.dataset.feature.FeatureExtractor
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence

object SubattrUnmatchRate extends FeatureExtractor {

  override def featureType: String = "SubattrStat"

  override def supportFilter: Boolean = false

  override def extract(col: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    val originalSize = FileUtils.numLine(col.colFile)
    val unmatchCol = getUnmatchChild(col)
    if (null != unmatchCol && originalSize != 0) {
      val unmatchSize = FileUtils.numLine(unmatchCol.colFile)
      val ratio = unmatchSize.toDouble / originalSize
      Iterable(new Feature(featureType, "unmatch_ratio", ratio))
    }
    else {
      Iterable()
    }
  }

  val persist = new JPAPersistence

  def getUnmatchChild(col: Column): Column = {
    val sql = "SELECT c FROM Column c WHERE c.parentWrapper =:parent AND c.colIndex = -1"
    try {
      persist.em.createQuery(sql, classOf[ColumnWrapper]).setParameter("parent", col).getSingleResult
    } catch {
      case e: Exception => {
        null
      }
    }
  }
}
