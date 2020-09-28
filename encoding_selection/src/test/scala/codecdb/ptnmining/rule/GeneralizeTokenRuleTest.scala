package codecdb.ptnmining.rule

import codecdb.ptnmining.parser.{TInt, TSymbol, TWord}
import codecdb.ptnmining.{PEmpty, PIntAny, PSeq, PToken, PUnion, PWordDigitAny}
import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser.TSymbol
import org.junit.Assert._
import org.junit.Test

class GeneralizeTokenRuleTest {

  @Test
  def testRewrite: Unit = {

    val ptn = PSeq.collect(
      new PToken(new TWord("ABC")),
      PUnion.collect(
        new PToken(new TWord("POM")),
        PSeq.collect(
          new PToken(new TWord("I")),
          new PToken(new TInt("3"))
        )
      ),
      new PToken(new TInt("3305")),
      PUnion.collect(
        new PToken(new TWord("POM")),
        PSeq.collect(
          new PToken(new TWord("I")),
          new PToken(new TInt("3")),
          new PToken(new TWord("M"))
        ),
        PEmpty
      ),
      PUnion.collect(
        new PToken(new TWord("POM")),
        PSeq.collect(
          new PToken(new TWord("I")),
          new PToken(new TInt("3")),
          new PToken(new TWord("M"))
        )
      )
    )
    val rule = new GeneralizeTokenRule
    val output = rule.rewrite(ptn)

    assertTrue(rule.happened)
    assertTrue(output.isInstanceOf[PSeq])
    val seq = output.asInstanceOf[PSeq]

    assertEquals(5, seq.content.size)
    assertEquals(new PToken(new TWord("ABC")), seq.content(0))
    assertEquals(new PWordDigitAny(1, -1), seq.content(1))
    assertEquals(new PIntAny(4), seq.content(2))
    assertEquals(new PWordDigitAny(0, 3), seq.content(3))
    assertEquals(new PWordDigitAny(3), seq.content(4))
  }

  @Test
  def testNotHappen: Unit = {
    val ptn = PSeq.collect(
      new PToken(new TWord("ABC")),
      new PToken(new TWord("3305")),
      PUnion.collect(
        new PToken(new TWord("POM")),
        PSeq.collect(
          new PToken(new TWord("I")),
          new PToken(new TSymbol("-")),
          new PToken(new TWord("M"))
        )
      )
    )
    val rule = new GeneralizeTokenRule
    val output = rule.rewrite(ptn)

    assertFalse(rule.happened)
  }
}
