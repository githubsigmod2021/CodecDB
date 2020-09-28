package codecdb.ptnmining.rule

import codecdb.ptnmining.parser.{TInt, TWord}
import codecdb.ptnmining.{PEmpty, PIntAny, PSeq, PToken, PUnion}
import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser.TInt
import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 3/29/17.
  */
class SuccinctRuleTest {

  @Test
  def testRewrite: Unit = {
    val pattern = new PSeq(Seq(
      new PUnion(Array(PEmpty, new PToken(new TWord("ddd")))),
      new PSeq(Seq(new PToken(new TInt("32342")))),
      new PUnion(Seq(new PToken(new TWord("abc")), new PToken(new TWord("abc")))),
      new PUnion(Seq(new PIntAny(2, 3), PEmpty))
    ))

    val rule = new SuccinctRule

    val rewritten = rule.rewrite(pattern)

    assertTrue(rule.happened)

    assertTrue(rewritten.isInstanceOf[PSeq])
    val newseq = rewritten.asInstanceOf[PSeq]
    assertEquals(4, newseq.content.length)

    //noinspection ZeroIndexToHead
    assertTrue(newseq.content(0).isInstanceOf[PUnion])
    assertTrue(newseq.content(1).isInstanceOf[PToken])
    assertTrue(newseq.content(2).isInstanceOf[PToken])
    assertTrue(newseq.content(3).isInstanceOf[PIntAny])
    //noinspection ZeroIndexToHead
    assertEquals(2, newseq.content(0).asInstanceOf[PUnion].content.size)
    assertEquals("32342", newseq.content(1).asInstanceOf[PToken].token.value)
    assertEquals("abc", newseq.content(2).asInstanceOf[PToken].token.value)
    assertEquals(new PIntAny(0, 3), newseq.content(3))
  }
}
