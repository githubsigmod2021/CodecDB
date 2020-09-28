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

package codecdb.ptnmining.parser

import java.io.StringReader

import scala.collection.mutable.ArrayBuffer

/**
  * Parse and convert a string into sequence of tokens
  *
  * @see lexer.jflex
  */
object Tokenizer {
  def tokenize(line: String): Iterator[Token] = {
    val lexer = new Lexer(new StringReader(line))

    val tokens = new ArrayBuffer[Token]

    var nextsym = lexer.scan()
    while (nextsym != null) {
      tokens += nextsym
      nextsym = lexer.scan()
    }
    tokens.iterator
  }
}
