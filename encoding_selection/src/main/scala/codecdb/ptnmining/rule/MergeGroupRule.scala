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

import codecdb.ptnmining.{PSeq, PUnion, Pattern}
import edu.uchicago.cs.encsel.ptnmining.PUnion

/**
  * This rule merges unnecessary PSeq/PUnion.
  * E.g.,
  * Seq(Seq(a,b),Seq(x,y)) => Seq(a,b,x,y)
  * Seq(Seq(a,b),x,y) => Seq(a,b,x,y)
  * Union(Union(a,b)) => Union(a,b)
  * Union(Union(a,b),x,y) => Union(a,b,x,y)
  *
  */
class MergeGroupRule extends RewriteRule {

  protected def condition(ptn: Pattern): Boolean = {
    (ptn.isInstanceOf[PSeq] &&
      ptn.asInstanceOf[PSeq].content.exists(_.isInstanceOf[PSeq])) ||
      (ptn.isInstanceOf[PUnion] &&
        ptn.asInstanceOf[PUnion].content.exists(_.isInstanceOf[PUnion]))
  }

  protected def update(ptn: Pattern): Pattern = {
    ptn match {
      case union: PUnion => {
        happen()
        PUnion(union.content.flatMap(_ match {
          case subu: PUnion => subu.content
          case x => Seq(x)
        }))
      }
      case seq: PSeq => {
        happen()
        PSeq(seq.content.flatMap(_ match {
          case subs: PSeq => subs.content
          case x => Seq(x)
        }))
      }
      case _ => ptn
    }
  }
}
