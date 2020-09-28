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

import codecdb.ptnmining.{PAny, PDoubleAny, PEmpty, PIntAny, PToken, PUnion, PWordAny, Pattern}
import codecdb.ptnmining.parser.{TDouble, TInt, TWord}
import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser.TDouble

/**
  * Use <code>PAny</code> to replace big Union of tokens
  */
object UseAnyRule {
  // Execute the rule if union size is greater than threshold * data size
  val threshold = 0.001
}

class UseAnyRule extends DataRewriteRule {

  override def condition(ptn: Pattern): Boolean = {
    val qualifiedUnion = ptn.isInstanceOf[PUnion] && {
      val union = ptn.asInstanceOf[PUnion]
      union.content.length >= UseAnyRule.threshold * originData.length &&
        union.content.view.forall(p => p.isInstanceOf[PToken] || p == PEmpty)
    }
    qualifiedUnion
  }


  override protected def update(ptn: Pattern): Pattern = {
    val union = ptn.asInstanceOf[PUnion]
    val hasEmpty = union.content.contains(PEmpty)
    val anyed = union.content.view.filter(_ != PEmpty).map(
      _ match {
        case token: PToken => {
          token.token match {
            case word: TWord => new PWordAny(word.numChar)
            case int: TInt => new PIntAny(int.numChar, int.isHex)
            case double: TDouble => new PDoubleAny(double.numChar)
            case other => token
          }
        }
        case other => other
      }
    ).groupBy(_.getClass)

    if ((anyed.size == 1 && classOf[PAny].isAssignableFrom(anyed.head._1)) ||
      (anyed.size == 2 && anyed.contains(classOf[PIntAny]) && anyed.contains(classOf[PDoubleAny]))) {
      val shrinked = anyed.map(kv => {
        kv._1 match {
          case wa if wa == classOf[PWordAny] => {
            kv._2.reduce((a, b) => {
              val aw = a.asInstanceOf[PWordAny]
              val bw = b.asInstanceOf[PWordAny]
              new PWordAny(Math.min(aw.minLength, bw.minLength),
                Math.max(aw.maxLength, bw.maxLength))
            })
          }
          case ia if ia == classOf[PIntAny] => {
            kv._2.reduce((a, b) => {
              val ai = a.asInstanceOf[PIntAny]
              val bi = b.asInstanceOf[PIntAny]
              new PIntAny(Math.min(ai.minLength, bi.minLength),
                Math.max(ai.maxLength, bi.maxLength),
                ai.hasHex || bi.hasHex)
            })
          }
          case da if da == classOf[PDoubleAny] => {
            kv._2.reduce((a, b) => {
              val ad = a.asInstanceOf[PDoubleAny]
              val bd = b.asInstanceOf[PDoubleAny]
              new PDoubleAny(Math.min(ad.minLength, bd.minLength),
                Math.max(ad.maxLength, bd.maxLength))
            })
          }
          case _ => throw new IllegalArgumentException
        }
      })
      val any = shrinked.size match {
        case 1 => shrinked.head
        case 2 => {
          // Double merge with int
          shrinked.reduce((a, b) => {
            val (iany, dany) = a match {
              case ia: PIntAny => (ia, b.asInstanceOf[PDoubleAny])
              case da: PDoubleAny => (b.asInstanceOf[PIntAny], da)
            }
            if (!iany.hasHex) {
              new PDoubleAny(Math.min(iany.minLength, dany.minLength), Math.max(iany.maxLength, dany.maxLength))
            } else
              null
          })
        }
      }
      if (any != null) {
        happen()
        hasEmpty match {
          case true => new PUnion(Seq(any, PEmpty))
          case false => any
        }
      } else
        union
    } else {
      union
    }
  }
}
