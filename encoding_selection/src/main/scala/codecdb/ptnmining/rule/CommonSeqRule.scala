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
import codecdb.ptnmining.parser.TWord
import codecdb.wordvec.SimilarWord
import edu.uchicago.cs.encsel.ptnmining._

import scala.collection.mutable.ArrayBuffer

object CommonSeqEqualFunc {

  def exactEquals(a: Pattern, b: Pattern): Boolean = a == b

  def patternFuzzyEquals(a: Pattern, b: Pattern): Boolean = {
    (a, b) match {
      case (atk: PToken, btk: PToken) => {
        (atk.token, btk.token) match {
          case (aw: TWord, bw: TWord) => {
            aw.value.length == bw.value.length
          }
          case (at, bt) => {
            at.getClass == bt.getClass
          }
        }
      }
      case _ => a.equals(b)
    }
  }

  def similarWordEquals(similarWord: SimilarWord): (Pattern, Pattern) => Boolean = {
    val func = (a: Pattern, b: Pattern) =>
      (a, b) match {
        case (atk: PToken, btk: PToken) => {
          (atk.token, btk.token) match {
            case (aw: TWord, bw: TWord) => similarWord.similar(aw.value, bw.value)
            case (at, bt) => at.getClass == bt.getClass
          }
        }
        case _ => a.equals(b)
      }
    func
  }
}

/**
  * Look for common sequence from a union and split it into smaller pieces
  *
  */
class CommonSeqRule(val eqfunc: (Pattern, Pattern) => Boolean = CommonSeqEqualFunc.exactEquals)
  extends RewriteRule {

  val cseq = new CommonSeq()
  val exactMatch = (eqfunc == CommonSeqEqualFunc.exactEquals _)

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

  protected def update(ptn: Pattern): Pattern = {
    // flatten the union content
    val union = ptn.asInstanceOf[PUnion]
    var hasEmpty = false
    // For non-exact match, each component of the symbol should be break-down into separate groups
    // E.g., P01, W21 -> (P,W) (01,21)

    val unionData = union.content.flatMap(
      _ match {
        case seq: PSeq => Some(seq.content)
        case PEmpty => {
          hasEmpty = true
          None
        }
        case p => Some(Seq(p))
      })
    // Single sequence, no common sequence to find
    if (unionData.size == 1) {
      return union
    }
    // Look for common sequence
    val seq = cseq.find(unionData, eqfunc)

    seq.isEmpty match {
      case true => union
      case false => {
        happen()
        val sectionBuffers = Array.fill(seq.length + 1)(new ArrayBuffer[Pattern])
        // Symbols, for non-exact match, group each piece of symbol as a union
        val fuzzySymbols = exactMatch match {
          case true => Seq()
          case false => seq.map(s => Array.fill(s.length)(new ArrayBuffer[Pattern]()))
        }
        val commonPos = cseq.positions
        val n = seq.length

        commonPos.indices.foreach(j => {
          val pos = commonPos(j)
          val data = unionData(j)

          var pointer = 0

          pos.indices.foreach(i => {
            val sec = pos(i)
            sectionBuffers(i) += PSeq(data.view(pointer, sec._1))
            pointer = sec._1 + sec._2

            if (!exactMatch)
              fuzzySymbols(i).zip(data.view(sec._1, pointer)).foreach(p => p._1 += p._2)
          })
          sectionBuffers.last += (pointer match {
            case last if last == data.length => PEmpty
            case _ => PSeq(data.view(pointer, data.length))
          })
        })
        val symbols = exactMatch match {
          case true => seq.map(PSeq(_))
          case false => fuzzySymbols.map(t => ptnmining.PSeq(t.map(PUnion(_))))
        }
        // Create new pattern
        val result = ptnmining.PSeq((0 until 2 * n + 1).map(i => i % 2 match {
          case 0 => PUnion(sectionBuffers(i / 2))
          case 1 => symbols(i / 2)
        }))

        hasEmpty match {
          case true => PUnion(Seq(result, PEmpty))
          case false => result
        }
      }
    }
  }
}

