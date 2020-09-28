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

package codecdb.ptnmining.rule

import codecdb.ptnmining.{PAny, PEmpty, PSeq, PToken, PUnion, Pattern}
import codecdb.ptnmining.parser.TSymbol
import edu.uchicago.cs.encsel.ptnmining._

class ExpandBoundRule extends RewriteRule {
  override protected def condition(ptn: Pattern): Boolean = {
    ptn.isInstanceOf[PSeq] && ptn.asInstanceOf[PSeq].content.exists(_.isInstanceOf[PAny])
  }

  override protected def update(ptn: Pattern): Pattern = {
    val seq = ptn.asInstanceOf[PSeq]
    seq.content.zipWithIndex.filter(_._1.isInstanceOf[PAny]).foreach(pair => {
      val any = pair._1.asInstanceOf[PAny]
      if (any.maxLength != -1 || any.minLength > 1) {
        // Check neighbors
        val prevOpen = pair._2 match {
          case 0 => false
          case i => openBoundary(seq.content(i - 1))
        }
        val nextOpen = pair._2 match {
          case last if last == seq.content.size - 1 => false
          case i => openBoundary(seq.content(i + 1))
        }

        if (prevOpen || nextOpen) {
          happen()
          // Expand boundary
          any.maxLength = -1
          any.minLength = Math.min(any.minLength, 1)
        }
      }
    })

    seq
  }

  private def openBoundary(ptn: Pattern): Boolean = {
    ptn match {
      case token: PToken => token.token.isInstanceOf[TSymbol]
      case union: PUnion => {
        union.content.size == 2 && union.content.map(_ match {
          case PEmpty => 1
          case t: PToken => if (t.token.isInstanceOf[TSymbol]) 2 else 0
          case _ => 0
        }).sum == 3
      }
      case _ => false
    }
  }
}
