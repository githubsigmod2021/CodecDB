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

package codecdb.ptnmining.eval

import codecdb.ptnmining.{PAny, Pattern}
import codecdb.ptnmining.matching.{PatternMatcher, RegexMatcher}
import edu.uchicago.cs.encsel.ptnmining.matching.RegexMatcher
import edu.uchicago.cs.encsel.ptnmining.PAny
import org.apache.commons.lang3.StringUtils

/**
  * <code>PatternEvaluator</code> evaluates a given pattern on a dataset to
  * determine its efficiency based on the pattern size + encoded data size
  *
  */
object PatternEvaluator {
  val matcher: PatternMatcher = RegexMatcher

  def evaluate(ptn: Pattern, dataset: Traversable[String]): Double = {

    if (StringUtils.isEmpty(ptn.getName))
      ptn.naming()

    // Pattern Size
    val sizeVisitor = new SizeVisitor
    ptn.visit(sizeVisitor)
    val ptnSize = sizeVisitor.size

    val anyNames = ptn.flatten.filter(_.isInstanceOf[PAny]).map(_.getName).distinct

    // Encoded Data Size
    val matched = dataset.map(di => (matcher.matchon(ptn, di), di))

    val encodedSize = matched.map(item => {
      val record = item._1
      val origin = item._2
      record.isDefined match {
        case true => {
          val content = record.get
          val unionSel = content.choices.values
            .map(x => intSize(x._2)).sum
          val anys = anyNames.map(name => {
            content.get(name) match {
              case "" => 0
              case a => a.length
            }
          })
          val intRange = content.rangeDeltas.values.map(intSize).sum
          unionSel + (anys.isEmpty match {
            case true => 0
            case false => anys.sum + anys.size
          }) + intRange
        }
        case false => origin.length
      }
    })

    ptnSize + encodedSize.sum
  }

  def intSize(input: Int): Int = Math.ceil(Math.log(input) / (8 * Math.log(2))).toInt

  def intSize(input: BigInt): Int = Math.ceil(Math.log(input.doubleValue()) / (8 * Math.log(2))).toInt
}
