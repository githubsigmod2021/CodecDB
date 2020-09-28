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
 */

package codecdb.ptnmining.parser

import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 3/14/17.
  */
class TokenizerTest {

  @Test
  def testTokenize:Unit = {
    val line = "778-9383 Suspendisse Av. Weirton IN 93479 (326) 677-3419"

    val tokens = Tokenizer.tokenize(line).toArray

    assertEquals(22, tokens.length)
  }

  def testTokenize2:Unit = {

  }
}
