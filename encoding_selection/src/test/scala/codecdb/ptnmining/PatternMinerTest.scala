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

package codecdb.ptnmining

import codecdb.ptnmining.parser.{TSpace, TSymbol, Tokenizer}
import edu.uchicago.cs.encsel.ptnmining.parser.TSpace
import org.junit.Assert._
import org.junit.Test

import scala.io.Source

/**
  * Created by harper on 4/4/17.
  */
class PatternMinerTest {

  @Test
  def testMine: Unit = {
    val input = Source.fromFile("src/test/resource/pattern/pattern_miner_sample")
      .getLines().map(Tokenizer.tokenize(_).toSeq).toSeq
    val pm = new PatternMiner
    val pattern = pm.mine(input)

    assertEquals(PSeq.collect(
      new PIntAny(1, -1),
      new PToken(new TSymbol("-")),
      new PIntAny(1, -1),
      new PToken(new TSymbol("-")),
      new PWordAny(1, -1)
    ), pattern)
  }


  @Test
  def testMineFromRealData1: Unit = {
    val input = Source.fromFile("src/test/resource/colsplit/realdata1")
      .getLines().map(Tokenizer.tokenize(_).toSeq).toSeq
    val pm = new PatternMiner
    val pattern = pm.mine(input)

    val expected = PSeq.collect(
      new PWordDigitAny(1, -1),
      new PToken(new TSymbol("-")),
      new PWordDigitAny(1, -1),
      PUnion.collect(
        new PToken(new TSymbol("-")),
        PEmpty
      ),
      new PIntAny(0, -1, true),
      PUnion.collect(
        new PToken(new TSymbol("-")),
        PEmpty
      ),
      new PIntAny(0, -1),
      PUnion.collect(
        new PToken(new TSymbol("-")),
        PEmpty
      ),
      new PIntAny(0, -1)
    )
    assertEquals(expected, pattern)
  }

  @Test
  def testMineFromRealData2: Unit = {
    val input = Source.fromFile("src/test/resource/colsplit/realdata2")
      .getLines().map(Tokenizer.tokenize(_).toSeq).toSeq
    val pm = new PatternMiner
    val pattern = pm.mine(input)

    val expected = PSeq.collect(
      new PIntAny(1, -1),
      new PToken(new TSymbol("-")),
      new PIntAny(1, -1),
      new PToken(new TSymbol("-")),
      new PIntAny(1, -1),
      new PToken(new TSpace),
      new PIntAny(1, -1),
      new PToken(new TSymbol(":")),
      new PIntAny(1, -1),
      new PToken(new TSymbol(":")),
      new PDoubleAny(1, -1)
    )

    assertEquals(expected, pattern)
  }
}
