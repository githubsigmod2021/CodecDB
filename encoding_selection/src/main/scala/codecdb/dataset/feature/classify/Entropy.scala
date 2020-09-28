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
import org.slf4j.LoggerFactory

import scala.io.Source

object Entropy extends FeatureExtractor {

  var logger = LoggerFactory.getLogger(getClass)

  def featureType = "Entropy"

  def supportFilter: Boolean = true

  def extract(column: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    val allcalc = new EntropyCalc()
    val linecalc = new EntropyCalc()

    val source = Source.fromInputStream(input)
    try {
      val lineEntropy = source.getLines().filter(StringUtils.isNotEmpty)
        .map(line => {
          allcalc.add(line)
          entropy(line, linecalc)
        }).toTraversable

      val fType = "%s%s".format(prefix, featureType)

      if (0 == lineEntropy.size) {
        Iterable(new Feature(fType, "line_max", 0),
          new Feature(fType, "line_min", 0),
          new Feature(fType, "line_mean", 0),
          new Feature(fType, "line_var", 0),
          new Feature(fType, "total", 0))
      } else {
        val stat = DataUtils.stat(lineEntropy)

        Iterable(new Feature(fType, "line_max", lineEntropy.max),
          new Feature(fType, "line_min", lineEntropy.min),
          new Feature(fType, "line_mean", stat._1),
          new Feature(fType, "line_var", stat._2),
          new Feature(fType, "total", allcalc.done()))
      }
    } finally {
      source.close()
    }
  }

  def entropy(data: String, linecalc: EntropyCalc): Double = {
    linecalc.reset()
    linecalc.add(data)
    linecalc.done()
  }
}

class EntropyCalc {

  var counter = scala.collection.mutable.HashMap[Char, Double]()

  def add(data: String): Unit = {
    data.toCharArray.foreach(c => {
      counter += ((c, counter.getOrElse(c, 0d) + 1))
    })
  }

  def reset(): Unit = {
    counter.clear()
  }

  def done(): Double = {
    val sum = counter.values.sum
    counter.map(entry => {
      val p = (entry._2 / sum)
      -p * Math.log(p)
    }).sum
  }
}