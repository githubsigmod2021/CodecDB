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

import java.math.BigInteger

import codecdb.ptnmining.{PIntRange, PToken, PUnion, Pattern}
import codecdb.ptnmining.parser.TInt
import edu.uchicago.cs.encsel.ptnmining.PIntRange

/**
  *
  * Convert a union of integer to a Integer range
  *
  * @deprecated
  * @see edu.uchicago.cs.encsel.ptnmining.PIntRange
  */

class IntegerRangeRule extends RewriteRule {

  override protected def update(ptn: Pattern): Pattern = {
    var max: BigInt = null
    var min: BigInt = null
    val union = ptn.asInstanceOf[PUnion]
    union.content.foreach(item => {
      val intToken = item.flatten(0).asInstanceOf[PToken].token.asInstanceOf[TInt]
      if (min == null || intToken.intValue < min)
        min = intToken.intValue
      if (max == null || intToken.intValue > max)
        max = intToken.intValue
    })
    happen()
    new PIntRange(min, max)
  }

  override def condition(ptn: Pattern): Boolean = {
    if (ptn.isInstanceOf[PUnion]) {
      val union = ptn.asInstanceOf[PUnion]
      union.content.map(item => {
        val list = item.flatten
        list.length == 1 &&
          list(0).isInstanceOf[PToken] &&
          list(0).asInstanceOf[PToken].token.isInstanceOf[TInt]
      }).reduce(_ && _)
    } else false
  }
}
