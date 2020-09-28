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
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */

package codecdb.ptnmining.matching

import codecdb.ptnmining.{PIntAny, PSeq, PToken, PUnion, PWordAny, PWordDigitAny}
import codecdb.ptnmining.parser.{TSpace, TSymbol}
import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser.TSpace
import org.junit.Assert._
import org.junit.Test

class RegexMatcherTest {

  @Test
  def testMatch: Unit = {

    val ptn = PSeq(Seq(
      new PIntAny(4, 4, true),
      new PWordAny(3, 3),
      PUnion(Seq(
        new PIntAny(5),
        new PWordAny(3)
      )),
      new PWordAny(3)
    ))
    ptn.naming()
    val test1 = "1D34qmP55237oWD"
    val test2 = "12E3KKdMPPwoq"
    val test3 = "12341423owdfwr"

    val match1 = RegexMatcher.matchon(ptn, test1)

    assertTrue(match1.isDefined)
    val m1 = match1.get
    assertEquals("1D34", m1.get("_0_0"))
    assertEquals("qmP", m1.get("_0_1"))
    assertEquals("55237", m1.get("_0_2"))
    assertEquals("oWD", m1.get("_0_3"))

    val match2 = RegexMatcher.matchon(ptn, test2)

    assertTrue(match2.isDefined)
    val m2 = match2.get
    assertEquals("12E3", m2.get("_0_0"))
    assertEquals("KKd", m2.get("_0_1"))
    assertEquals("MPP", m2.get("_0_2"))
    assertEquals("woq", m2.get("_0_3"))

    val match3 = RegexMatcher.matchon(ptn, test3)

    assertTrue(match3.isEmpty)
  }

  @Test
  def testVarLength: Unit = {
    val pattern = PSeq.collect(
      new PWordAny(0, 3),
      new PToken(new TSymbol("*")),
      new PIntAny(4, 7),
      new PToken(new TSymbol("-")),
      new PWordDigitAny(1, -1)
    )

    val testFailureData = Array("123*3132-DAFS", "WDPA*3141-KKMP", "WWD*323-AAAFA",
      "WMP*43234253-KKDSFW", "WMP*WFWE-32423", "WWK*1234-_#@$").map(RegexMatcher.matchon(pattern, _))
    assertTrue(testFailureData.forall(_.isEmpty))

    val testSuccessData = Array("*3132-DAFS", "WP*3141-1234", "WWD*323000-AAA2FA", "*3323123-1")
      .map(RegexMatcher.matchon(pattern, _))
    assertTrue(testSuccessData.forall(_.nonEmpty))

    val g0 = testSuccessData.map(m => m.get.get("_0_0")).toArray[AnyRef]
    val g1 = testSuccessData.map(m => m.get.get("_0_2")).toArray[AnyRef]
    val g2 = testSuccessData.map(m => m.get.get("_0_4")).toArray[AnyRef]

    assertArrayEquals(Array[AnyRef]("", "WP", "WWD", ""), g0)
    assertArrayEquals(Array[AnyRef]("3132", "3141", "323000", "3323123"), g1)
    assertArrayEquals(Array[AnyRef]("DAFS", "1234", "AAA2FA", "1"), g2)
  }

  @Test
  def testSpaceMatch: Unit = {
    val pattern = PSeq.collect(
      new PWordAny(0, 3),
      new PToken(new TSpace),
      new PIntAny(4, 7)
    )

    val testSuccessData = Array("MPQ 3132", "WD    3423312", " 01412").map(RegexMatcher.matchon(pattern, _))
    assertTrue(testSuccessData.forall(_.nonEmpty))

    assertArrayEquals(Array[AnyRef]("MPQ", "WD", ""), testSuccessData.map(_.get.get("_0_0")).toArray[AnyRef])
    assertArrayEquals(Array[AnyRef]("3132", "3423312", "01412"), testSuccessData.map(_.get.get("_0_2")).toArray[AnyRef])
  }
}
