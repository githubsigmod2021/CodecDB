package codecdb.ptnmining.rule

import codecdb.ptnmining.parser.{TInt, TSymbol, TWord, Tokenizer}
import codecdb.ptnmining.{PEmpty, PSeq, PToken, PUnion, Pattern}
import edu.uchicago.cs.encsel.ptnmining.parser.TSymbol
import edu.uchicago.cs.encsel.ptnmining._
import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 3/29/17.
  */
class CommonSeqRuleTest {

  @Test
  def testRewrite: Unit = {
    val union = new PUnion(
      Seq(
        PSeq(Seq(new PToken(new TWord("abc")), new PToken(new TInt("312")),
          new PToken(new TSymbol("-")), new PToken(new TInt("212")),
          new PToken(new TWord("good")), new PToken(new TSymbol("-")))),
        PSeq(Seq(new PToken(new TInt("4021")), new PToken(new TSymbol("-")),
          new PToken(new TInt("2213")), new PToken(new TWord("akka")),
          new PToken(new TSymbol("-")), new PToken(new TInt("420")))),
        PSeq(Seq(new PToken(new TWord("kwmt")), new PToken(new TWord("ddmpt")),
          new PToken(new TInt("2323")), new PToken(new TSymbol("-")),
          new PToken(new TInt("33130")), new PToken(new TSymbol("-")))),
        PSeq(Seq(new PToken(new TWord("ttpt")), new PToken(new TInt("3232")),
          new PToken(new TSymbol("-")), new PToken(new TInt("42429")),
          new PToken(new TWord("dddd")), new PToken(new TSymbol("-")))))
    )
    val csq = new CommonSeqRule((a: Pattern, b: Pattern) => {
      (a, b) match {
        case (atk: PToken, btk: PToken) => {
          atk.token.getClass == btk.token.getClass
        }
        case _ => a.equals(b)
      }
    })

    val newptn = csq.rewrite(union)

    assertTrue(csq.happened)

    assertTrue(newptn.isInstanceOf[PSeq])
    val newseq = newptn.asInstanceOf[PSeq]

    assertEquals(5, newseq.content.length)

    assertTrue(newseq.content(0).isInstanceOf[PUnion])
    assertTrue(newseq.content(1).isInstanceOf[PSeq])
    assertTrue(newseq.content(2).isInstanceOf[PUnion])
    assertTrue(newseq.content(3).isInstanceOf[PToken])
    assertTrue(newseq.content(4).isInstanceOf[PUnion])

    val u0 = newseq.content.head.asInstanceOf[PUnion]
    val c1 = newseq.content(1).asInstanceOf[PSeq]
    val u2 = newseq.content(2).asInstanceOf[PUnion]
    val c3 = newseq.content(3).asInstanceOf[PToken]
    val u4 = newseq.content(4).asInstanceOf[PUnion]

    assertEquals(4, u0.content.size)
    assertEquals(new TSymbol("-"), c1.content(1).asInstanceOf[PToken].token)
    assertEquals(new TSymbol("-"), c3.token)
    assertEquals(4, u2.content.size)
    assertEquals(2, u4.content.size)

    assertTrue(u0.content.contains(new PToken(new TWord("abc"))))
    assertTrue(u0.content.contains(PEmpty))
    assertTrue(u0.content.contains(PSeq.collect(new PToken(new TWord("kwmt")), new PToken(new TWord("ddmpt")))))
    assertTrue(u0.content.contains(new PToken(new TWord("ttpt"))))
  }

  @Test
  def testSequencesWithPEmpty: Unit = {

    val data = PUnion(Array("J01", "P05", "L37", "D53", "M21")
      .map(s => PSeq(Tokenizer.tokenize(s).map(t => new PToken(t)).toSeq)).toSeq :+ PEmpty)

    val rule = new CommonSeqRule(CommonSeqEqualFunc.patternFuzzyEquals _)
    val result = rule.rewrite(data)
    assertTrue(rule.happened)
    assertTrue(result.isInstanceOf[PUnion])
    val u = result.asInstanceOf[PUnion]
    assertEquals(2, u.content.size)
    assertTrue(u.content(0).isInstanceOf[PSeq])
    assertEquals(PEmpty, u.content(1))

    val s0 = u.content(0).asInstanceOf[PSeq]
    val u00 = s0.content(0).asInstanceOf[PUnion]
    val u01 = s0.content(1).asInstanceOf[PUnion]

    assertEquals(5, u00.content.size)
    assertTrue(u00.content.contains(new PToken(new TWord("J"))))
    assertTrue(u00.content.contains(new PToken(new TWord("P"))))
    assertTrue(u00.content.contains(new PToken(new TWord("L"))))
    assertTrue(u00.content.contains(new PToken(new TWord("D"))))
    assertTrue(u00.content.contains(new PToken(new TWord("M"))))
    assertEquals(5, u01.content.size)
    assertTrue(u01.content.contains(new PToken(new TInt("01"))))
    assertTrue(u01.content.contains(new PToken(new TInt("05"))))
    assertTrue(u01.content.contains(new PToken(new TInt("37"))))
    assertTrue(u01.content.contains(new PToken(new TInt("53"))))
    assertTrue(u01.content.contains(new PToken(new TInt("21"))))
  }

  @Test
  def testWithSimilarWord: Unit = {
    val rule = new CommonSeqRule


  }
}
