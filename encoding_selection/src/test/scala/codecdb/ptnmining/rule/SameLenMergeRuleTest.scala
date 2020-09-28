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

package codecdb.ptnmining.rule

import codecdb.ptnmining.{PEmpty, PSeq, PToken, PUnion}
import codecdb.ptnmining.parser.{TInt, TWord, Tokenizer}
import edu.uchicago.cs.encsel.ptnmining.parser.TInt
import edu.uchicago.cs.encsel.ptnmining.PEmpty
import org.junit.Assert._
import org.junit.Test

class SameLenMergeRuleTest {

  @Test
  def testRewrite: Unit = {
    val data = PUnion(Array("3A5FB", "45527", "667FD", "73982", "A3A2D")
      .map(rs => PSeq(Tokenizer.tokenize(rs).map(new PToken(_)).toSeq)))
    val rule = new SameLenMergeRule
    val output = rule.rewrite(data)
    assertTrue(rule.happened)

    assertTrue(output.asInstanceOf[PUnion].content.forall(p => p.asInstanceOf[PToken].token.isInstanceOf[TInt]))
  }

  @Test
  def testRewriteComplex: Unit = {
    val data = PUnion(Array("74Y9656YL1CK1355009", "00E5867YL1AK20930EF",
      "74Y9656YL1CK134621E", "74Y9656YL1CK1355009", "74Y9656YL1CK134621E",
      "74Y9656YL1CK1352349", "00E5867YL1DK21210D1", "74Y9656YL1CK134621E")
      .map(rs => PSeq(Tokenizer.tokenize(rs).map(new PToken(_)).toSeq)))
    val rule = new SameLenMergeRule
    val output = rule.rewrite(data)
    assertTrue(rule.happened)

    val seq = output.asInstanceOf[PSeq]
    assertEquals(7, seq.content.size)
    val u0 = seq.content(0).asInstanceOf[PUnion].content
    assertTrue(u0.contains(new PToken(new TInt("00"))))
    assertTrue(u0.contains(new PToken(new TInt("74"))))

    val u1 = seq.content(1).asInstanceOf[PUnion].content
    assertTrue(u1.contains(new PToken(new TWord("E"))))
    assertTrue(u1.contains(new PToken(new TWord("Y"))))

    val u2 = seq.content(2).asInstanceOf[PUnion].content
    assertTrue(u2.contains(new PToken(new TInt("5867"))))
    assertTrue(u2.contains(new PToken(new TInt("9656"))))

    assertEquals(seq.content(3), new PToken(new TWord("YL")))

    assertEquals(seq.content(4), new PToken(new TInt("1")))
    val u4 = seq.content(5).asInstanceOf[PUnion].content
    assertTrue(u4.contains(new PToken(new TWord("DK"))))
    assertTrue(u4.contains(new PToken(new TWord("CK"))))
    assertTrue(u4.contains(new PToken(new TWord("AK"))))


    val u6 = seq.content(6).asInstanceOf[PUnion].content
    assertTrue(u6.contains(new PToken(new TInt("1352349"))))
    assertTrue(u6.contains(new PToken(new TInt("134621E"))))
    assertTrue(u6.contains(new PToken(new TInt("20930EF"))))
    assertTrue(u6.contains(new PToken(new TInt("21210D1"))))
    assertTrue(u6.contains(new PToken(new TInt("1355009"))))
  }

  @Test
  def testRewriteMultipleGroup: Unit = {
    val data = PUnion(Array("40000", "00080", "04C80", "I4", "00080", "I2", "7B371")
      .map(rs => PSeq(Tokenizer.tokenize(rs).map(new PToken(_)).toSeq)))
    val rule = new SameLenMergeRule
    val output = rule.rewrite(data)
    assertTrue(rule.happened)

    assertTrue(output.isInstanceOf[PUnion])
    val outputUnion = output.asInstanceOf[PUnion]
    assertEquals(2, outputUnion.content.size)

    val group = outputUnion.content.groupBy(_.getClass)
    val seq0 = group.get(classOf[PSeq]).get.head
    val cand = PSeq.collect(new PToken(new TWord("I")),
      PUnion.collect(new PToken(new TInt("4")), new PToken(new TInt("2"))))
    assertEquals(seq0, cand)

    val union1 = group.get(classOf[PUnion]).get.head.asInstanceOf[PUnion]
    assertTrue(union1.content.contains(new PToken(new TInt("40000"))))
    assertTrue(union1.content.contains(new PToken(new TInt("00080"))))
    assertTrue(union1.content.contains(new PToken(new TInt("04C80"))))
    assertTrue(union1.content.contains(new PToken(new TInt("7B371"))))
  }

  @Test
  def testRewriteWithEmpty: Unit = {
    val data = PUnion(Array("40000", "00080", "04C80", "I4", "00080", "I2", "7B371")
      .map(rs => PSeq(Tokenizer.tokenize(rs).map(new PToken(_)).toSeq)) :+ PEmpty)
    val rule = new SameLenMergeRule
    val output = rule.rewrite(data)
    assertTrue(rule.happened)

    assertTrue(output.isInstanceOf[PUnion])
    val outputUnion = output.asInstanceOf[PUnion]
    assertEquals(3, outputUnion.content.size)

    assertTrue(outputUnion.content.contains(PEmpty))

    val group = outputUnion.content.groupBy(_.getClass)
    val seq0 = group.get(classOf[PSeq]).get.head
    val cand = PSeq.collect(new PToken(new TWord("I")),
      PUnion.collect(new PToken(new TInt("4")), new PToken(new TInt("2"))))
    assertEquals(seq0, cand)

    val union1 = group.get(classOf[PUnion]).get.head.asInstanceOf[PUnion]
    assertTrue(union1.content.contains(new PToken(new TInt("40000"))))
    assertTrue(union1.content.contains(new PToken(new TInt("00080"))))
    assertTrue(union1.content.contains(new PToken(new TInt("04C80"))))
    assertTrue(union1.content.contains(new PToken(new TInt("7B371"))))
  }

  @Test
  def testRewriteInvalid: Unit = {

    val data = PUnion(Array("MPWRK", "33650", "WEFSK", "12812", "KKMPD")
      .map(rs => PSeq(Tokenizer.tokenize(rs).map(new PToken(_)).toSeq)))
    val rule = new SameLenMergeRule
    rule.rewrite(data)
    assertFalse(rule.happened)

    val data2 = PUnion(Array("1PWRK", "33650", "2EFSK", "12812", "3KMPD")
      .map(rs => PSeq(Tokenizer.tokenize(rs).map(new PToken(_)).toSeq)))
    rule.rewrite(data2)
    assertFalse(rule.happened)

    val data3 = PUnion(Array("123423423", "33650", "2221", "12812", "3")
      .map(rs => PSeq(Tokenizer.tokenize(rs).map(new PToken(_)).toSeq)))
    rule.rewrite(data3)
    assertFalse(rule.happened)

    val data4 = PUnion(Array("1234DMPT23423")
      .map(rs => PSeq(Tokenizer.tokenize(rs).map(new PToken(_)).toSeq)))
    rule.rewrite(data4)
    assertFalse(rule.happened)

    val data5 = PUnion(Array(PEmpty))
    rule.rewrite(data5)
    assertFalse(rule.happened)

  }

  @Test
  def testRewrite2: Unit = {
    val ptn = PSeq.collect(
      new PToken(new TInt("324")),
      new PToken(new TWord("dasf")),
      new PToken(new TInt("323")))
    val rule = new SameLenMergeRule
    val result = rule.rewrite(ptn)

    assertFalse(rule.happened)

    val ptn2 = PSeq.collect(new PToken(new TInt("3")),
      new PToken(new TWord("A")),
      new PToken(new TInt("5")),
      new PToken(new TWord("FB")))
    rule.reset
    val result2 = rule.rewrite(ptn2)

    assertFalse(rule.happened)
  }

  @Test
  def testWithSymbol: Unit = {
    val data = PUnion(Array("degree(MA", "degree(BA")
      .map(rs => PSeq(Tokenizer.tokenize(rs).map(new PToken(_)).toSeq)))
    val rule = new SameLenMergeRule
    val result = rule.rewrite(data)
    assertFalse(rule.happened)
  }

  @Test
  def testPrioritizeLetter: Unit = {
    val data = PUnion(Array("degree1", "degree2")
      .map(rs => PSeq(Tokenizer.tokenize(rs).map(new PToken(_)).toSeq)))
    val rule = new SameLenMergeRule
    val result = rule.rewrite(data)
    assertTrue(rule.happened)

    val expected = PSeq(
      Seq(
        new PToken(new TWord("degree")),
        PUnion.collect(
          new PToken(new TInt("1")),
          new PToken(new TInt("2"))
        )
      )
    )

    assertEquals(expected, result)
  }
}
