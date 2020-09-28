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

import codecdb.ptnmining.{PAny, PEmpty, PIntRange, PSeq, PToken, PUnion, Pattern, PatternVisitor}
import edu.uchicago.cs.encsel.ptnmining._

/**
  * Compute the size of a pattern. This includes the followings:
  * 1. The size of pattern itself
  * 2. Additional bytes for each records encoded using this pattern
  */
class SizeVisitor extends PatternVisitor {

  var size = 0

  def on(ptn: Pattern): Unit = {
    ptn match {
      case token: PToken => size += token.token.length
      case union: PUnion => size += union.content.size + 2
      case seq: PSeq => size += seq.content.length + 2
      case any: PAny => size += 1
      case range: PIntRange => size += 5 // One indicator and 2 32-bit integer
      case PEmpty => size += 1
      case _ => Unit
    }
  }
}
