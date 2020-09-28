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

package codecdb.ptnmining.matching

import codecdb.ptnmining.Pattern

import scala.util.matching.Regex

object RegexMatcher extends PatternMatcher {

  val regexgen = new GenRegexVisitor

  private var cachedPattern: Pattern = null

  private var regex: Regex = null

  override def matchon(pattern: Pattern, input: String): Option[Record] = {
    if (cachedPattern == null || !cachedPattern.eq(pattern)) {
      regex = genRegex(pattern).r
      cachedPattern = pattern
    }
    val matched = regex.findFirstMatchIn(input)

    val groupPatterns = regexgen.list
    matched match {
      case Some(mc) => {
        // matched value by index, skip the entire group
        val patternValues = (1 to mc.groupCount).map(i => mc.group(i) match {
          case null => ""
          case a => a
        }).zip(groupPatterns).map(t => (t._2, t._1)).toMap

        val record = new Record(patternValues)
        Some(record)
      }
      case None => None
    }
  }

  def genRegex(pattern: Pattern): String = {
    if (pattern.name.isEmpty)
      pattern.naming()
    regexgen.reset
    pattern.visit(regexgen)
    regexgen.get
  }
}
