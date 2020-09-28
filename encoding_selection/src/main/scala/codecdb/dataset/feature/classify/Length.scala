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

package codecdb.dataset.feature.classify

import java.io.InputStream

import codecdb.dataset.column.Column
import codecdb.dataset.feature.{Feature, FeatureExtractor}
import codecdb.util.DataUtils
import edu.uchicago.cs.encsel.dataset.feature.FeatureExtractor
import org.apache.commons.lang.StringUtils

import scala.io.Source

object Length extends FeatureExtractor {

  def featureType = "Length"

  def supportFilter: Boolean = true

  def extract(column: Column, input: InputStream, prefix: String): Iterable[Feature] = {

    val fType = "%s%s".format(prefix, featureType)
    val source = Source.fromInputStream(input)
    try {
      val length = source.getLines()
        .filter(StringUtils.isNotEmpty).map(_.length().toDouble).toSeq
      if (0 == length.size) {
        Iterable(new Feature(fType, "max", 0),
          new Feature(fType, "min", 0),
          new Feature(fType, "mean", 0),
          new Feature(fType, "variance", 0))
      } else {
        val statforlen = DataUtils.stat(length)
        Iterable(
          new Feature(fType, "max", length.max),
          new Feature(fType, "min", length.min),
          new Feature(fType, "mean", statforlen._1),
          new Feature(fType, "variance", statforlen._2))
      }
    } finally {
      source.close()
    }
  }
}