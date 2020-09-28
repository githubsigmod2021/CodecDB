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
import java.util.Comparator

import codecdb.dataset.column.Column
import codecdb.common.Conversions._
import codecdb.dataset.feature.{Feature, FeatureExtractor}
import codecdb.util.FileUtils
import edu.uchicago.cs.encsel.dataset.feature.{Feature, FeatureExtractor}

import scala.io.Source
import scala.util.Random

/**
  * This feature computes how much the dataset is sorted by compute the number
  * of inverted pairs
  */
class Sortness(val windowSize: Int) extends FeatureExtractor {

  def featureType = Sortness.featureType

  def supportFilter: Boolean = true

  def extract(col: Column, input: InputStream, prefix: String): Iterable[Feature] = {

    val source = Source.fromInputStream(input)

    val stream = source.getLines().toStream
//    val lineCount = stream.size
    // For performance consideration
    val lineCount = FileUtils.numLine(col.colFile)

    val selection = lineCount > windowSize * windowSize match {
      case true => 2.0 / windowSize
      case false => 2.0 * windowSize / lineCount
    }

    // The ratio of selection makes sure the operation is n
    try {
      var total_pair = 0
      var inverted_pair = 0
      var counter = 0
      var rankdiff = 0

      if (windowSize != -1) {
        stream.sliding(windowSize, windowSize)
          .filter(p => Random.nextDouble() <= selection)
          .foreach(group => {
            val (invert, total) = Sortness.computeInvertPair(group, col.dataType.comparator())
            total_pair += total
            inverted_pair += invert

            val diffrank = Sortness.computeDiffRank(group, col.dataType.comparator())
            counter += group.length
            rankdiff += diffrank
          })
      } else {
        val (invert, total) = Sortness.computeInvertPair(stream, col.dataType.comparator())
        total_pair += total
        inverted_pair += invert

        rankdiff += Sortness.computeDiffRank(stream, col.dataType.comparator())
        counter += stream.size
      }
      val fType = featureType(prefix)
      if (0 != total_pair) {
        // 1 - abs(2x-1)
        val ratio = (total_pair - inverted_pair).toDouble / total_pair
        val ivpair = 1 - Math.abs(2 * ratio - 1)
        // Kendall's Tau
        val ktau = (total_pair - 2 * inverted_pair).toDouble / total_pair
        // Spearman's Rho
        val srho = 1 - 6.0 * rankdiff / (counter * (counter * counter - 1))

        Iterable(
          new Feature(fType, "totalpair_%d".format(windowSize), total_pair),
          new Feature(fType, "ivpair_%d".format(windowSize), ivpair),
          new Feature(fType, "kendalltau_%d".format(windowSize), ktau),
          new Feature(fType, "numitem_%d".format(windowSize), counter),
          new Feature(fType, "spearmanrho_%d".format(windowSize), srho)
        )
      } else {
        Iterable(
          new Feature(fType, "totalpair_%d".format(windowSize), 0),
          new Feature(fType, "ivpair_%d".format(windowSize), 0),
          new Feature(fType, "kendalltau_%d".format(windowSize), 0),
          new Feature(fType, "numitem_%d".format(windowSize), 0),
          new Feature(fType, "spearmanrho_%d".format(windowSize), 0)
        )
      }
    } finally {
      source.close()
    }
  }
}

object Sortness {
  val featureType: String = "Sortness"

  def computeInvertPair(input: Seq[String],
                        comparator: Comparator[String]): (Int, Int) = {
    if (input.isEmpty)
      return (0, 0)
    var invert = 0
    input.indices.foreach(i => {
      input.indices.drop(i + 1).foreach(j => {
        if (comparator.compare(input(i), input(j)) > 0) {
          invert += 1
        }
      })
    })
    return (invert, input.length * (input.length - 1) / 2)
  }

  def computeDiffRank(input: Seq[String], comparator: Comparator[String]): Int = {
    if (input.isEmpty)
      return 0

    var counter =
      input.zipWithIndex.sortBy(_._1)(comparator).zipWithIndex.map(item => {
        val d = item._1._2 - item._2
        d * d
      }).sum
    counter
  }
}
