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

package codecdb.dataset.feature.subattr

import java.io.{File, InputStream}
import java.nio.file.{Files, Paths}

import codecdb.dataset.column.Column
import codecdb.dataset.feature.{Feature, FeatureExtractor}
import codecdb.dataset.feature.resource.ParquetEncFileSize
import codecdb.dataset.persist.jpa.JPAPersistence
import codecdb.util.FileUtils
import edu.uchicago.cs.encsel.dataset.feature.FeatureExtractor
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence

object SubattrEncodeBenefit extends FeatureExtractor {

  override def featureType: String = "SubattrStat"

  override def supportFilter: Boolean = false

  override def extract(col: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    val originalSize = columnSize(col)
    val pSize = plainSize(col)
    val subtableFile = FileUtils.addExtension(col.colFile,"subtable")
    if(Files.exists(Paths.get(subtableFile))) {
      val childrenSize = new File(subtableFile).length()
      Iterable(
        new Feature("SubattrStat", "size_ratio", childrenSize / originalSize),
        new Feature("SubattrStat", "plain_size_ratio", childrenSize / pSize)
      )
    } else {
      Iterable()
    }
  }

  val persist = new JPAPersistence()

  def columnSize(col: Column): Double = {
    val features = col.findFeatures(ParquetEncFileSize.featureType).map(_.value).filter(_ > 0)
    features.size match {
      case 0 => 0
      case _ => features.min
    }
  }

  def plainSize(col: Column): Double = {
    val feature = col.findFeature(ParquetEncFileSize.featureType, "PLAIN_file_size")
    feature match {
      case None => 0
      case Some(s) => s.value
    }
  }
}
