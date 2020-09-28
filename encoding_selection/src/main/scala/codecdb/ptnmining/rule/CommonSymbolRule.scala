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

package codecdb.ptnmining.rule

import codecdb.ptnmining
import codecdb.ptnmining.{PEmpty, PSeq, PToken, PUnion, Pattern}
import codecdb.ptnmining.parser.TSymbol
import edu.uchicago.cs.encsel.ptnmining._

import scala.collection.mutable.ArrayBuffer

/**
  * Look for common separators (non-alphabetic, non-numerical characters) from Union and use them to
  * split data. Rows that does not contain any symbols will be treated as a whole to put to the first group.
  */
object CommonSymbolRule {
  val threshold = 0.4
}

class CommonSymbolRule extends RewriteRule {

  /**
    * Only apply to non-empty, non-single record union of
    * 1. seq of token
    * 2. single token
    * 3. empty
    *
    * @param ptn
    * @return
    */
  protected def condition(ptn: Pattern): Boolean =
    ptn.isInstanceOf[PUnion] && {
      val cnt = ptn.asInstanceOf[PUnion].content.view
      cnt.size > 1 && {
        val res = cnt.map(_ match {
          case seq: PSeq => (seq.content.forall(t => t.isInstanceOf[PToken] || t == PEmpty), seq.content.size)
          case token: PToken => (true, 1)
          case PEmpty => (true, 1)
          case _ => (false, 0)
        })
        res.exists(_._2 > 1) && res.forall(_._1)
      }
    }

  protected def update(union: Pattern): Pattern = {
    // flatten the union content
    var hasEmpty = false
    val unionData = union.asInstanceOf[PUnion].content
      .flatMap(_ match {
        case seq: PSeq => Some(seq.content)
        case PEmpty => {
          hasEmpty = true
          None
        }
        case p => Some(Seq(p))
      })
    if (unionData.length == 1)
      return union
    // Scan union data for symbols
    val symbolsWithPos = unionData.map(_.zipWithIndex.filter(_._1 match {
      case t: PToken => t.token.isInstanceOf[TSymbol]
      case _ => false
    }))

    val groups = symbolsWithPos.zipWithIndex.groupBy(_._1.isEmpty)

    val noSymbolLines = groups.getOrElse(true, Seq()).map(p => p._2).toSet
    // Valid lines are lines with at least one symbol
    val validLinesWithIndex = groups.getOrElse(false, Seq())
    if (validLinesWithIndex.isEmpty) {
      // No valid lines
      return union
    }
    // If there is at least one line with no symbol, the common symbol found is optional
    val optionalSymbol = noSymbolLines.nonEmpty
    val validLines = validLinesWithIndex.map(_._1)
    val validIndexMapping = validLinesWithIndex.map(_._2).zipWithIndex.map(p => (p._1, p._2)).toMap
    // Determine common symbols and match symbol in each line to the common
    val commonSeq = new CommonSeq
    val commonSymbols = commonSeq.find(validLines, (a: (Pattern, Int), b: (Pattern, Int)) => {
      a._1.equals(b._1)
    }).flatten
    val n = commonSymbols.length
    commonSymbols.isEmpty match {
      case true => {
        // No common symbol, go for the first major single symbol
        val majorThreshold = CommonSymbolRule.threshold * validLines.size
        val majorSymbols = validLines.map(_.groupBy(_._1).map(p => (p._1, p._2.minBy(_._2)._2)))
          .reduce((mapa, mapb) => {
            mapa ++ mapb.map(kv => (kv._1, kv._2 + mapa.getOrElse(kv._1, 0)))
          }).filter(_._2 >= majorThreshold)
        if (majorSymbols.nonEmpty) {
          happen()
          // Use the most common symbol for a partition
          val majorSymbol = majorSymbols.maxBy(_._2)._1

          val firstGroup = new ArrayBuffer[Pattern]
          val secondGroup = new ArrayBuffer[Pattern]
          unionData.indices.foreach(j => {
            if (noSymbolLines.contains(j)) {
              firstGroup += PSeq(unionData(j))
            } else {
              val symbols = validLines(validIndexMapping.getOrElse(j, -1))
              val data = unionData(j)
              // Result will contain only two groups
              // The lines containing no symbol will be put to the first group
              // The lines containing some symbol but not the majority will be put to the second group
              symbols.find(p => p._1 == majorSymbol) match {
                case Some(found) => {
                  firstGroup += PSeq(data.view(0, found._2))
                  secondGroup += PSeq(data.view(found._2 + 1, data.size))
                }
                case None => {
                  secondGroup += PSeq(data)
                }
              }
            }
          })
          firstGroup += PEmpty
          if (noSymbolLines.nonEmpty)
            secondGroup += PEmpty
          new PSeq(Seq(
            PUnion(firstGroup),
            PUnion.collect(majorSymbol, PEmpty),
            PUnion(secondGroup)))
        }
        else {
          union
        }
      }
      case false => {
        // use common symbol to partition input
        happen()
        // Use the positions to split data and generate new unions
        // n common symbols split the data to at most n+1 pieces
        val pieces = Array.fill(n + 1)(new ArrayBuffer[Pattern])
        val symbols = optionalSymbol match {
          case true => commonSymbols.map(cs => PUnion(Seq(cs._1, PEmpty)))
          case false => commonSymbols.map(_._1)
        }
        unionData.indices.foreach(j => {
          if (noSymbolLines.contains(j)) {
            // This line has no symbol, for first column return all, for others return empty
            pieces(0) += PSeq(unionData(j))
            // DO This just once below out of the loop
            // for (i <- 1 to n)
            //   pieces(i) += PEmpty
          } else {
            // This line has symbol
            val data = unionData(j)
            val symbols = symbolsWithPos(j)
            val pos = commonSeq.positions(validIndexMapping.getOrElse(j, -1))
            val index = pos.map(p => symbols.view(p._1, p._1 + p._2)).flatten.map(_._2)

            var pointer = 0
            index.indices.foreach(i => {
              pieces(i) += PSeq(data.slice(pointer, index(i)))
              pointer = index(i) + 1
            })
            pieces.last += (pointer match {
              case last if last == data.length => PEmpty
              case _ => PSeq(data.view(pointer, data.length))
            })
          }
        })
        if (noSymbolLines.nonEmpty) {
          for (i <- 1 to n)
            pieces(i) += PEmpty
        }
        // Make a sequence formed of union pieces and common sequences
        val result = ptnmining.PSeq((0 until 2 * n + 1).view.map(i => {
          i % 2 match {
            case 0 => PUnion(pieces(i / 2))
            case 1 => symbols((i - 1) / 2)
          }
        }))
        hasEmpty match {
          case true => PUnion(Seq(result, PEmpty))
          case false => result
        }
      }
    }
  }
}
