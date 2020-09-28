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
import edu.uchicago.cs.encsel.ptnmining._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by harper on 3/27/17.
  */
trait RewriteRule {

  private var modified: Boolean = false

  protected val path = new ArrayBuffer[Pattern]

  protected def happen() = modified = true

  def happened = modified

  def reset = modified = false

  /**
    * Apply the rule on pattern
    *
    * @param root The target Pattern
    * @return pattern after rewritten
    */
  def rewrite(root: Pattern): Pattern = {
    path += root
    try {
      root match {
        case union: PUnion => condition(union) match {
          case true => update(union)
          case false => {
            // Look for match in the union, replace it with rewritten if any
            val modified_content = union.content.map(p => rewrite(p))
            val modified_union = modified match {
              case true => new PUnion(modified_content)
              case false => union
            }
            modified_union
          }
        }
        case seq: PSeq => {
          condition(seq) match {
            case true => update(seq)
            case false => {
              val modified_content = seq.content.map(p => rewrite(p))
              val modified_seq = modified match {
                case true => new PSeq(modified_content)
                case false => seq
              }
              modified_seq
            }
          }
        }
        case pattern => {
          condition(pattern) match {
            case true => update(pattern)
            case false => pattern
          }
        }
      }
    }
    finally {
      path.remove(path.size - 1)
    }
  }

  protected def condition(ptn: Pattern): Boolean

  protected def update(ptn: Pattern): Pattern
}
