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

import codecdb.ptnmining.{PAny, PEmpty, PIntAny, PSeq, PToken, PUnion, PWordDigitAny, Pattern}
import codecdb.ptnmining.parser.{TInt, TWord}
import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser.TInt

/**
  * This rule looks at top-level tokens and decide whether to upgrade them into <code>PAny</code>
  * we need to check tokens at top level (tokens incldued in the final sequence).
  * The following cases will be processed:
  * 1. TInt will be rewritten as PIntAny as we don't think numbers are representative.
  * 2. Union that contains only int/word will be rewritten as PWordAny.
  *
  * For the second case, we can do this as Unions of single tokens will be processed
  * by rule UseAny. The only thing left is unions of mixture, which contains only int
  * or word tokens as symbols are extracted in advance.  In addition, these unions are
  * not aligned as aligned tokens will be extracted by SameLenMergeRule. So the only
  * thing left would be combinations of unaligned text/numbers combination, which does
  * not hurt if we use <code>PAny</code> to represent them.
  *
  */
class GeneralizeTokenRule extends RewriteRule {

  override protected def condition(ptn: Pattern): Boolean = {
    path.size == 2 && (ptn match {
      case union: PUnion => GeneralizeTokenRule.check(union)
      case token: PToken => token.token.isInstanceOf[TInt]
      case _ => false
    })
  }

  override protected def update(ptn: Pattern): Pattern = {
    ptn match {
      case token: PToken => {
        happen()
        new PIntAny(token.numChar._1)
      }
      case union: PUnion => {
        happen()
        val childRange = union.numChar
        if (union.content.contains(PEmpty))
          new PWordDigitAny(0, childRange._2)
        else if (childRange._1 == childRange._2)
          new PWordDigitAny(childRange._1)
        else
          new PWordDigitAny(1, -1)
      }
      case _ => ptn
    }
  }
}

object GeneralizeTokenRule {
  def check(target: Pattern): Boolean = {

    target match {
      case u: PUnion => u.content.map(check).forall(_ == true)
      case s: PSeq => s.content.map(check).forall(_ == true)
      case t: PToken => t.token.isInstanceOf[TWord] || t.token.isInstanceOf[TInt]
      case PEmpty => true
      case any: PAny => true
      case _ => false
    }
  }
}
