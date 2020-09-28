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

package codecdb.dataset.feature.classify

import java.io.InputStream

import codecdb.dataset.column.Column
import codecdb.dataset.feature.{Feature, FeatureExtractor}
import edu.uchicago.cs.encsel.dataset.feature.FeatureExtractor

import scala.io.Source

/**
  * This captures features similar to what is captured in <code>Sortness</code>.
  * But instead of considering all pairs in a window, it only look at adjacent pairs.
  */
object AdjInvertPair extends FeatureExtractor {

  def featureType: String = "AdjInvertPair"

  def supportFilter: Boolean = true

  def extract(column: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    val source = Source.fromInputStream(input)
    val comparator = column.dataType.comparator()
    var numPair = 0
    var numInverted = 0
    val fType = featureType(prefix)
    try {
      var prev: String = null
      source.getLines().foreach(line => {
        if (prev != null) {
          numPair += 1
          numInverted += (comparator.compare(prev, line) match {
            case gt if gt > 0 => 1
            case _ => 0
          })
        }
        prev = line
      })
      if (numPair != 0) {
        val ratio = (numPair - numInverted).toDouble / numPair
        val ivpair = 1 - Math.abs(1 - 2 * ratio)
        val kendallsTau = 1 - 2 * numInverted.toDouble / numPair
        Iterable(
          new Feature(fType, "totalpair", numPair),
          new Feature(fType, "ivpair", ivpair),
          new Feature(fType, "kendallstau", kendallsTau)
        )
      } else {
        Iterable(
          new Feature(fType, "totalpair", numPair),
          new Feature(fType, "ivpair", 0),
          new Feature(fType, "kendallstau", 0)
        )
      }
    } finally {
      source.close()
    }
  }
}

